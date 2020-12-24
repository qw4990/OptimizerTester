package cetest

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

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
		distVals[i] = make([][]string, 0, len(indexCols[i]))
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
			begin := time.Now()
			nCols := len(q.indexCols[i])
			cols := strings.Join(q.indexCols[i], ", ")
			whereCond := ""
			for j, col := range q.indexCols[i] {
				if j > 0 {
					whereCond += " AND "
				}
				whereCond += fmt.Sprintf("%v IS NOT NULL", col)
			}
			sql := fmt.Sprintf("SELECT %v, COUNT(*) FROM %v.%v WHERE %v GROUP BY %v ORDER BY %v", cols, q.db, q.indexTables[i], whereCond, cols, cols)
			rows, err := ins.Query(sql)
			if err != nil {
				rerr = err
				return
			}
			for rows.Next() {
				colVals := make([]string, nCols)
				var cnt int
				args := make([]interface{}, nCols+1)
				for i := 0; i < nCols; i++ {
					args[i] = &colVals[i]
				}
				args[nCols] = &cnt
				if rerr = rows.Scan(args...); rerr != nil {
					return
				}
				q.orderedVals[i] = append(q.orderedVals[i], colVals)
				q.valRows[i] = append(q.valRows[i], cnt)
			}
			if rerr = rows.Close(); rerr != nil {
				return
			}
			fmt.Printf("[MulColIndexQuerier-Init] index=%v, sql=%v, cost=%v\n", q.indexes[i], sql, time.Since(begin))
		}
	})
	return
}

func (q *mulColIndexQuerier) Collect(nSamples int, qt QueryType, ers []EstResult, ins tidb.Instance, ignoreErr bool) ([]EstResult, error) {
	if err := q.init(ins); err != nil {
		return nil, err
	}
	indexIdx := q.qMap[qt]
	nRows := len(q.valRows[indexIdx])
	if nSamples == 0 {
		nSamples = nRows
	}
	sampleRate := float64(nSamples) / float64(nRows)
	if sampleRate > 1 {
		sampleRate = 1
	}

	begin := time.Now()
	concurrency := 64
	var resultLock sync.Mutex
	processed := 0
	var wg sync.WaitGroup

	for workID := 0; workID < concurrency; workID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for rowIdx := id; rowIdx < nRows; rowIdx += concurrency {
				if rand.Float64() > sampleRate {
					continue
				}

				var cond string
				var act int
				if qt == QTMulColsRangeQueryOnIndex {
					cond, act = q.rangeCond(indexIdx, rowIdx, int(math.Min(float64(rowIdx+rand.Intn(20)), float64(nRows-1))))
				} else {
					cond, act = q.pointCond(indexIdx, rowIdx)
				}

				sql := fmt.Sprintf("SELECT * FROM %v.%v WHERE %v", q.db, q.indexTables[indexIdx], cond)
				est, err := getEstRowFromExplain(ins, sql)
				if err != nil {
					if !ignoreErr {
						panic(err)
					}
					fmt.Println(sql, err)
					continue
				}

				resultLock.Lock()
				ers = append(ers, EstResult{sql, est, float64(act)})
				processed++
				if processed%5000 == 0 {
					fmt.Printf("[MulColIndexQuerier-Process] ins=%v, index=%v, qt=%v, concurrency=%v, time-cost=%v, progress (%v/%v)\n",
						ins.Opt().Label, q.indexTables[indexIdx], qt, concurrency, time.Since(begin), processed, nSamples)
				}
				resultLock.Unlock()
			}
		}(workID)
	}

	wg.Wait()
	return ers, nil
}

func (q *mulColIndexQuerier) rangeCond(indexIdx, beginRowIdx, endRowIdx int) (string, int) {
	beginRowVals := q.orderedVals[indexIdx][beginRowIdx]
	endRowVals := q.orderedVals[indexIdx][endRowIdx]
	cond := ""
	cols := q.indexCols[indexIdx]
	types := q.colTypes[indexIdx]

	for c := 0; c < len(cols); c++ {
		if c > 0 {
			cond += " AND "
		}
		if beginRowVals[c] == endRowVals[c] {
			pattern := "%v=%v"
			if types[c] == DTString {
				pattern = "%v='%v'"
			}
			cond += fmt.Sprintf(pattern, cols[c], beginRowVals[c])
		} else {
			pattern := "%v>=%v AND %v<=%v"
			if types[c] == DTString {
				pattern = "%v>='%v' AND %v<='%v'"
			}
			cond += fmt.Sprintf(pattern, cols[c], beginRowVals[c], cols[c], endRowVals[c])
			break
		}
	}

	rows := 0
	for i := beginRowIdx; i <= endRowIdx; i++ {
		rows += q.valRows[indexIdx][i]
	}
	return cond, rows
}

func (q *mulColIndexQuerier) pointCond(indexIdx, rowIdx int) (string, int) {
	cond := ""
	cols := q.indexCols[indexIdx]
	types := q.colTypes[indexIdx]
	colVals := q.orderedVals[indexIdx][rowIdx]
	for i := 0; i < len(cols); i++ {
		if i > 0 {
			cond += " AND "
		}
		pattern := "%v=%v"
		if types[i] == DTString {
			pattern = "%v='%v'"
		}
		cond += fmt.Sprintf(pattern, cols[i], colVals[i])
	}
	return cond, q.valRows[indexIdx][rowIdx]
}
