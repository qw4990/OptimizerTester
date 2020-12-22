package cetest

import (
	"fmt"
	"strings"

	"github.com/qw4990/OptimizerTester/tidb"
)

type mulColIndexQuerier struct {
	db          string
	indexes     []string
	indexTables []string
	indexCols   [][]string   // idID, colNames
	colTypes    [][]DATATYPE // idxID, colID, type
	orderedVals [][][]string // idxID, rowID, colValues
	valRows     [][]int      // idxID, rowID, numOfRows
}

func newMulColIndexQuerier(ins tidb.Instance, db string, indexes, tbs []string, indexCols [][]string, colTypes [][]DATATYPE) (*mulColIndexQuerier, error) {
	distVals := make([][][]string, len(indexCols))
	actRows := make([][]int, len(indexCols))
	for i := range indexCols {
		distVals[i] = make([][]string, len(indexCols[i]))
	}

	tv := &mulColIndexQuerier{
		db:          db,
		indexes:     indexes,
		indexTables: tbs,
		indexCols:   indexCols,
		colTypes:    colTypes,
		orderedVals: distVals,
		valRows:     actRows,
	}
	return tv, tv.init(ins)
}

func (q *mulColIndexQuerier) init(ins tidb.Instance) error {
	for i := range q.indexes {
		cols := strings.Join(q.indexCols[i], ", ")
		sql := fmt.Sprintf("SELECT %v, COUNT(*) FROM %v.%v GROUP BY %v ORDER BY %v", cols, q.db, q.indexTables[i], cols, cols)
		rows, err := ins.Query(sql)
		if err != nil {
			return err
		}
		for rows.Next() {
			colVals := make([]string, len(cols))
			var cnt int
			args := make([]interface{}, len(cols)+1)
			for i := 0; i < len(cols); i++ {
				args[i] = &colVals[i]
			}
			args[len(cols)] = &cnt
			q.orderedVals[i] = append(q.orderedVals[i], colVals)
			q.valRows[i] = append(q.valRows[i], cnt)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (q *mulColIndexQuerier) collectMulColIndexEstResult(indexIdx int, rangeQuery bool, ins tidb.Instance, ers []EstResult, ignoreErr bool) ([]EstResult, error) {
	for i := 0; i < len(q.valRows[indexIdx]); i++ {
		var cond string
		var act int
		if rangeQuery {
			cond, act = q.rangeCond(indexIdx, i)
		} else {
			cond, act = q.pointCond(indexIdx, i)
		}
		sql := fmt.Sprintf("SELECT * FROM %v.%v WHERE %v", q.db, q.indexTables[indexIdx], cond)
		est, err := getEstRowFromExplain(ins, sql)
		if err != nil {
			if !ignoreErr {
				panic(err)
			}
			fmt.Println(q, err)
			continue
		}
		ers = append(ers, EstResult{sql, est, float64(act)})
	}
	return ers, nil
}

func (q *mulColIndexQuerier) rangeCond(indexIdx, rowIdx int) (string, int) {
	panic("TODO")
}

func (q *mulColIndexQuerier) pointCond(indexIdx, rowIdx int) (string, int) {
	cond := ""
	cols := q.indexCols[indexIdx]
	types := q.colTypes[indexIdx]
	vals := q.orderedVals[indexIdx][rowIdx]
	for i := 0; i < len(cols); i++ {
		if i > 0 {
			cond += ", "
		}
		pattern := "%v=%v"
		if types[i] == DTString {
			pattern = "%v='%v'"
		}
		cond += fmt.Sprintf(pattern, cols[i], vals[i])
	}
	return cond, q.valRows[indexIdx][rowIdx]
}
