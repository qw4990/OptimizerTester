package cetest

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

// Dataset ...
type Dataset interface {
	// Name returns the name of the dataset
	Name() string

	// init is used to initialize the Dataset
	init() error

	// valArray returns a string array to store data according to the schema of the table
	valArray() [][][]string

	// GenCases ...
	GenCases(n int, qt QueryType) (queries []string, err error)

	randPointQuery(cols int) string
	randPointColCond(tbIdx, colIdx int) string

	randRangeQuery(cols int) string
	randRangeColCond(tbIdx, colIdx int) string
	randRangeQueryEQPrefix() string

	randMCVLCVPointQuery(isMCV bool) string
}

type baseDataset struct {
	opt DatasetOpt
	ins tidb.Instance

	numTbs      int
	numCols     []int
	tbs         []string
	cols        [][]string
	orderedVals [][][]string // [tbIdx][colIdx][]string{ordered values}
	mcv         [][][]string // mcv[i][j] means the most common values in (table[i], column[j]) from the dataset
	lcv         [][][]string // least common values
	percent     int
}

func newBaseDataset(opt DatasetOpt, ins tidb.Instance, tbs []string, cols [][]string) (baseDataset, error) {
	numTbs := len(tbs)
	numCols := make([]int, numTbs)
	for idx, col := range cols {
		numCols[idx] = len(col)
	}
	return baseDataset{
		opt:     opt,
		ins:     ins,
		numTbs:  numTbs,
		numCols: numCols,
		tbs:     tbs,
		cols:    cols,
		percent: 10, // most/least 10% common values
	}, nil
}

func (ds *baseDataset) Name() string {
	return ""
}

func (ds *baseDataset) init() error {
	ds.orderedVals = ds.valArray()
	for i, tb := range ds.tbs {
		for j, col := range ds.cols[i] {
			sql := fmt.Sprintf("SELECT %v FROM %v ORDER BY %v", col, tb, col)
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

		for j, col := range ds.cols[i] {
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

func (ds *baseDataset) valArray() [][][]string {
	xs := make([][][]string, ds.numTbs)
	for i := range xs {
		xs[i] = make([][]string, ds.numCols[i])
	}
	return xs
}

func (ds *baseDataset) GenCases(n int, qt QueryType) ([]string, error) {
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

// todo: Support query conditions with more than two columns
func (ds *baseDataset) randPointQuery(cols int) string {
	tbIdx := rand.Intn(ds.numTbs)
	cond := ""
	if cols == 1 {
		cond = ds.randPointColCond(tbIdx, rand.Intn(ds.numCols[tbIdx]))
	} else {
		// TODO: check the ds.numTbs and generate random column index to construct the condition
		cond = fmt.Sprintf("%v AND %v", ds.randPointColCond(tbIdx, 0), ds.randPointColCond(tbIdx, 1))
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}

func (ds *baseDataset) randPointColCond(tbIdx, colIdx int) string {
	val := ds.orderedVals[tbIdx][colIdx][rand.Intn(len(ds.orderedVals[tbIdx][colIdx]))]
	return fmt.Sprintf("%v = %v", ds.cols[tbIdx][colIdx], val)
}

// todo: Support query conditions with more than two columns
func (ds *baseDataset) randRangeQuery(cols int) string {
	tbIdx := rand.Intn(ds.numTbs)
	cond := ""
	if cols == 1 {
		cond = ds.randRangeColCond(tbIdx, rand.Intn(ds.numCols[tbIdx]))
	} else {
		// TODO: check the ds.numTbs and generate random column index to construct the condition
		cond = fmt.Sprintf("%v AND %v", ds.randRangeColCond(tbIdx, 0), ds.randRangeColCond(tbIdx, 1))
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}

func (ds *baseDataset) randRangeColCond(tbIdx, colIdx int) string {
	val1Idx := rand.Intn(len(ds.orderedVals[tbIdx][colIdx]))
	val2Idx := rand.Intn(len(ds.orderedVals[tbIdx][colIdx])-val1Idx) + val1Idx
	return fmt.Sprintf("%v>=%v AND %v<=%v", ds.cols[tbIdx][colIdx], ds.orderedVals[tbIdx][colIdx][val1Idx], ds.cols[tbIdx][colIdx], ds.orderedVals[tbIdx][colIdx][val2Idx])
}

func (ds *baseDataset) randRangeQueryEQPrefix() string {
	tbIdx := rand.Intn(ds.numTbs)
	// TODO: check the ds.numTbs and generate random column index to construct the condition
	cond := fmt.Sprintf("%v AND %v", ds.randPointColCond(tbIdx, 0), ds.randRangeColCond(tbIdx, 1))
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}

func (ds *baseDataset) randMCVLCVPointQuery(isMCV bool) string {
	tbIdx := rand.Intn(ds.numTbs)
	colIdx := rand.Intn(ds.numCols[tbIdx])
	val := ""
	if isMCV {
		val = ds.mcv[tbIdx][colIdx][rand.Intn(len(ds.mcv[tbIdx][colIdx]))]
	} else {
		val = ds.lcv[tbIdx][colIdx][rand.Intn(len(ds.lcv[tbIdx][colIdx]))]
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v=%v", ds.tbs[tbIdx], ds.cols[tbIdx][colIdx], val)
}
