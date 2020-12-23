package cetest

import (
	"fmt"
	"strings"
	"sync"

	"github.com/qw4990/OptimizerTester/tidb"
)

// mulColIndexQuerier supports QTMulColsPointQueryOnIndex, QTMulColsRangeQueryOnIndex.
// It generates queries like:
//	SELECT * FROM t WHERE idx1Col1=? 
//	SELECT * FROM t WHERE idx1Col1=? and idx1Col2=?
type mulColIndexQuerier struct {
	db          string
	indexes     []string
	indexTables []string
	indexCols   [][]string   // idID, colNames
	colTypes    [][]DATATYPE // idxID, colID, type
	qMap        map[QueryType]int

	orderedVals [][][]string // idxID, rowID, colValues
	valRows     [][]int      // idxID, rowID, numOfRows
	initOnce    sync.Once
}

func newMulColIndexQuerier(
	db string,              // the database name
	indexes []string,       // index names
	tbs []string,           // table names of these indexes
	indexCols [][]string,   // column names of these indexes
	colTypes [][]DATATYPE,  // types of these columns
	qMap map[QueryType]int, //  idxIdx used to generate specified type of SQLs
) *mulColIndexQuerier {
	distVals := make([][][]string, len(indexCols))
	actRows := make([][]int, len(indexCols))
	for i := range indexCols {
		distVals[i] = make([][]string, len(indexCols[i]))
	}

	return &mulColIndexQuerier{
		db:          db,
		indexes:     indexes,
		indexTables: tbs,
		indexCols:   indexCols,
		colTypes:    colTypes,
		qMap:        qMap,
		orderedVals: distVals,
		valRows:     actRows,
	}
}

func (q *mulColIndexQuerier) init(ins tidb.Instance) (rerr error) {
	q.initOnce.Do(func() {
		for i := range q.indexes {
			cols := strings.Join(q.indexCols[i], ", ")
			sql := fmt.Sprintf("SELECT %v, COUNT(*) FROM %v.%v GROUP BY %v ORDER BY %v", cols, q.db, q.indexTables[i], cols, cols)
			rows, err := ins.Query(sql)
			if err != nil {
				rerr = err
				return
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
			if rerr = rows.Close(); rerr != nil {
				return
			}
		}
	})
	return
}

func (q *mulColIndexQuerier) Collect(qt QueryType, ers []EstResult, ins tidb.Instance, ignoreErr bool) ([]EstResult, error) {
	indexIdx := q.qMap[qt]
	rangeQuery := qt == QTMulColsRangeQueryOnIndex
	return q.collect(indexIdx, rangeQuery, ins, ers, ignoreErr)
}

func (q *mulColIndexQuerier) collect(indexIdx int, rangeQuery bool, ins tidb.Instance, ers []EstResult, ignoreErr bool) ([]EstResult, error) {
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
