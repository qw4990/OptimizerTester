package cetest

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

/*
	datasetZipFX's schemas are:
		CREATE TABLE tint ( a INT, b INT, KEY(a), KEY(a, b) )
		CREATE TABLE tdouble ( a DOUBLE, b DOUBLE, KEY(a), KEY(a, b) )
		CREATE TABLE tstring ( a VARCHAR(32), b VARCHAR(32), KEY(a), KEY(a, b) )
		CREATE TABLE tdatetime (a DATETIME, b DATATIME, KEY(a), KEY(a, b))
*/
type datasetZipFX struct {
	opt DatasetOpt
	ins tidb.Instance

	tbs         []string
	cols        []string
	orderedVals [][][]string // [tbIdx][colIdx][]string{ordered values}
	mcv         [][][]string
	lcv         [][][]string
	percent     int
}

func newDatasetZipFX(opt DatasetOpt, ins tidb.Instance) (Dataset, error) {
	return &datasetZipFX{
		opt:     opt,
		ins:     ins,
		tbs:     []string{"int", "double", "string", "datetime"},
		cols:    []string{"a", "b"},
		percent: 10, // most/least 10% common values
	}, nil
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}

func (ds *datasetZipFX) GenCases(n int, qt QueryType) ([]string, error) {
	if ds.orderedVals == nil {
		if err := ds.init(); err != nil {
			return nil, err
		}
	}

	sqls := make([]string, 0, n)
	switch qt {
	case QTSingleColPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randPointQuery(1))
		}
	case QTMultiColsPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randPointQuery(2))
		}
	case QTSingleColRangeQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randRangeQuery(1))
		}
	case QTMultiColsRangeQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randRangeQuery(2))
		}
	case QTMultiColsRangeQueryEQPrefix:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randRangeQueryEQPrefix())
		}
	case QTMCVPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randMCVLCVPointQuery(true))
		}
	case QTLCVPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randMCVLCVPointQuery(false))
		}
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt.String())
	}
	return sqls, nil
}

func (ds *datasetZipFX) randMCVLCVPointQuery(isMCV bool) string {
	tbIdx := rand.Intn(4)
	colIdx := rand.Intn(2)
	val := ""
	if isMCV {
		val = ds.mcv[tbIdx][colIdx][rand.Intn(len(ds.mcv[tbIdx][colIdx]))]
	} else {
		val = ds.lcv[tbIdx][colIdx][rand.Intn(len(ds.lcv[tbIdx][colIdx]))]
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v=%v", ds.tbs[tbIdx], ds.cols[colIdx], val)
}

func (ds *datasetZipFX) randRangeQueryEQPrefix() string {
	tbIdx := rand.Intn(4)
	cond := fmt.Sprintf("%v AND %v", ds.randPointColCond(tbIdx, 0), ds.randRangeColCond(tbIdx, 1))
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}

func (ds *datasetZipFX) randRangeQuery(cols int) string {
	tbIdx := rand.Intn(4)
	cond := ""
	if cols == 1 {
		cond = ds.randRangeColCond(tbIdx, rand.Intn(2))
	} else {
		cond = fmt.Sprintf("%v AND %v", ds.randRangeColCond(tbIdx, 0), ds.randRangeColCond(tbIdx, 1))
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}

func (ds *datasetZipFX) randRangeColCond(tbIdx, colIdx int) string {
	val1Idx := rand.Intn(len(ds.orderedVals[tbIdx][colIdx]))
	val2Idx := rand.Intn(len(ds.orderedVals[tbIdx][colIdx])-val1Idx) + val1Idx
	return fmt.Sprintf("%v>=%v AND %v<=%v", ds.cols[colIdx], ds.orderedVals[tbIdx][colIdx][val1Idx], ds.cols[colIdx], ds.orderedVals[tbIdx][colIdx][val2Idx])
}

func (ds *datasetZipFX) randPointQuery(cols int) string {
	tbIdx := rand.Intn(4)
	cond := ""
	if cols == 1 {
		cond = ds.randPointColCond(tbIdx, rand.Intn(2))
	} else {
		cond = fmt.Sprintf("%v AND %v", ds.randPointColCond(tbIdx, 0), ds.randPointColCond(tbIdx, 1))
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}

func (ds *datasetZipFX) randPointColCond(tbIdx, colIdx int) string {
	val := ds.orderedVals[tbIdx][colIdx][rand.Intn(len(ds.orderedVals[tbIdx][colIdx]))]
	return fmt.Sprintf("%v = %v", ds.cols[colIdx], val)
}

func (ds *datasetZipFX) init() error {
	ds.orderedVals = ds.valArray()
	for i, tb := range ds.tbs {
		for j, col := range ds.cols {
			sql := fmt.Sprintf("SELECT %v FROM t%v ORDER BY %v", col, tb, col)
			begin := time.Now()
			rows, err := ds.ins.Query(sql)
			if err != nil {
				return err
			}
			fmt.Printf("[%v-%v] %v cost %v\n", ds.opt.Label, ds.ins.Opt().Label, sql, time.Since(begin))
			for rows.Next() {
				var val interface{}
				if err := rows.Scan(&val); err != nil {
					return err
				}
				ds.orderedVals[i][j] = append(ds.orderedVals[i][j], val.(string))
			}
			rows.Close()
		}
	}

	// init mcv and lcv
	ds.mcv = ds.valArray()
	ds.lcv = ds.valArray()
	for i, tb := range ds.tbs {
		row, err := ds.ins.Query(fmt.Sprintf("SELECT COUNT(*) FROM %v", tb))
		if err != nil {
			return err
		}
		var total int
		if err := row.Scan(&total); err != nil {
			return err
		}
		row.Close()
		limit := total * ds.percent / 100

		for j, col := range ds.cols {
			for _, order := range []string{"DESC", "ASC"} {
				rows, err := ds.ins.Query(fmt.Sprintf("SELECT %v FROM %v GROUP BY %v ORDER BY COUNT(*) %v LIMIT %v", col, tb, col, order, limit))
				if err != nil {
					return err
				}
				for rows.Next() {
					var val interface{}
					if err := rows.Scan(&val); err != nil {
						return err
					}
					if order == "DESC" {
						ds.mcv[i][j] = append(ds.mcv[i][j], val.(string))
					} else {
						ds.lcv[i][j] = append(ds.lcv[i][j], val.(string))
					}
				}
				rows.Close()
			}
		}
	}
	return nil
}

func (ds *datasetZipFX) valArray() [][][]string {
	xs := make([][][]string, 4)
	for i := range xs {
		xs[i] = make([][]string, 2)
	}
	return xs
}
