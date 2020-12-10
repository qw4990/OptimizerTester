package cetest

import (
	"database/sql"
	"fmt"
	"github.com/pingcap/errors"
	"math/rand"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

// Dataset ...
type Dataset interface {
	// Name returns the name of the dataset
	Name() string

	// GenCases ...
	GenCases(n int, qt QueryType) (queries []string, err error)
}

type baseDataset struct {
	opt DatasetOpt
	ins tidb.Instance

	numTbs      int
	numCols     []int
	tbs         []string
	cols        [][]string
	used        [][]bool     // `used[i][j] = false` means we will not use the value of (table[i], column[j])
	orderedVals [][][]string // [tbIdx][colIdx][]string{ordered values}
	mcv         [][][]string // mcv[i][j] means the most common values in (table[i], column[j]) from the dataset
	lcv         [][][]string // least common values
	percent     int
}

func newBaseDataset(opt DatasetOpt, ins tidb.Instance, tbs []string, cols [][]string, used [][]bool) (baseDataset, error) {
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
		used:    used,
		percent: 10, // most/least 10% common values
	}, nil
}

func (ds *baseDataset) init() error {
	// switch database
	if err := ds.ins.Exec(fmt.Sprintf("USE %v", ds.opt.DB)); err != nil {
		return err
	}

	// analyze tables
	for tbIdx, tb := range ds.tbs {
		used := false
		for _, flag := range ds.used[tbIdx] {
			if flag {
				used = true
				break
			}
		}
		if !used {
			continue
		}
		if err := ds.ins.Exec(fmt.Sprintf("ANALYZE TABLE %v", tb)); err != nil {
			return err
		}
	}

	// init ordered values
	ds.orderedVals = ds.valArray()
	for i, tb := range ds.tbs {
		for j, col := range ds.cols[i] {
			if !ds.used[i][j] {
				continue
			}
			sql := fmt.Sprintf("SELECT DISTINCT(%v) FROM %v ORDER BY %v", col, tb, col)
			begin := time.Now()
			rows, err := ds.ins.Query(sql)
			if err != nil {
				return err
			}
			fmt.Printf("[%v-%v] init values %v cost %v\n", ds.opt.Label, ds.ins.Opt().Label, sql, time.Since(begin))
			vals, err := drainOneColValsToStr(rows)
			if err != nil {
				return err
			}
			ds.orderedVals[i][j] = vals
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
		row.Next()
		if err := row.Scan(&total); err != nil {
			return err
		}
		row.Close()
		limit := total * ds.percent / 100

		for j, col := range ds.cols[i] {
			if !ds.used[i][j] {
				continue
			}
			for _, order := range []string{"DESC", "ASC"} {
				sql := fmt.Sprintf("SELECT %v FROM %v GROUP BY %v ORDER BY COUNT(*) %v LIMIT %v", col, tb, col, order, limit)
				begin := time.Now()
				rows, err := ds.ins.Query(sql)
				if err != nil {
					return err
				}
				fmt.Printf("[%v-%v] init MCV/LCV %v cost %v\n", ds.opt.Label, ds.ins.Opt().Label, sql, time.Since(begin))
				vals, err := drainOneColValsToStr(rows)
				if err != nil {
					return err
				}
				if order == "DESC" {
					ds.mcv[i][j] = vals
				} else {
					ds.lcv[i][j] = vals
				}
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

func (ds *baseDataset) randPointColCond(tbIdx, colIdx int) string {
	val := ds.orderedVals[tbIdx][colIdx][rand.Intn(len(ds.orderedVals[tbIdx][colIdx]))]
	return fmt.Sprintf("%v = %v", ds.cols[tbIdx][colIdx], val)
}

func (ds *baseDataset) randRangeColCond(tbIdx, colIdx int) string {
	val1Idx := rand.Intn(len(ds.orderedVals[tbIdx][colIdx]))
	val2Idx := rand.Intn(len(ds.orderedVals[tbIdx][colIdx])-val1Idx) + val1Idx
	return fmt.Sprintf("%v>=%v AND %v<=%v", ds.cols[tbIdx][colIdx], ds.orderedVals[tbIdx][colIdx][val1Idx], ds.cols[tbIdx][colIdx], ds.orderedVals[tbIdx][colIdx][val2Idx])
}

func drainOneColValsToStr(rows *sql.Rows) ([]string, error) {
	defer rows.Close()
	buf := make([]string, 0, 512)
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	switch colTypes[0].DatabaseTypeName() {
	case "INT":
		for rows.Next() {
			var i int
			if err := rows.Scan(&i); err != nil {
				return nil, err
			}
			buf = append(buf, fmt.Sprintf("%v", i))
		}
	case "DOUBLE":
		for rows.Next() {
			var i float64
			if err := rows.Scan(&i); err != nil {
				return nil, err
			}
			buf = append(buf, fmt.Sprintf("%.4f", i))
		}
	case "VARCHAR", "DATETIME":
		for rows.Next() {
			var i string
			if err := rows.Scan(&i); err != nil {
				return nil, err
			}
			buf = append(buf, fmt.Sprintf("'%v'", i))
		}
	default:
		return nil, errors.Errorf("unsupported database type=%v", colTypes[0].DatabaseTypeName())
	}
	return buf, nil
}
