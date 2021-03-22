package cetest

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

// singleColQuerier supports QTSingleColPointQueryOnCol, QTSingleColPointQueryOnIndex, QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex
// It generates queries like:
//	SELECT * FROM t WHERE col = ?
type singleColQuerier struct {
	db       string
	tbs      []string   // table names
	cols     [][]string // table columns' names
	colTypes [][]DATATYPE
	qMap     map[QueryType][2]int

	orderedDistVals [][][]string // ordered distinct values
	valActRows      [][][]int    // actual row count
	initOnce        sync.Once
}

func newSingleColQuerier(
	db string,                 // the database name
	tbs []string,              // tables used to generate SQLs
	cols [][]string,           // column names of these tables
	colTypes [][]DATATYPE,     // types of these columns
	qMap map[QueryType][2]int, // tbIdx and colIdx used to generate specified type of SQLs
) *singleColQuerier {
	distVals := make([][][]string, len(cols))
	actRows := make([][][]int, len(cols))
	for i := range cols {
		distVals[i] = make([][]string, len(cols[i]))
		actRows[i] = make([][]int, len(cols[i]))
	}

	return &singleColQuerier{
		db:              db,
		tbs:             tbs,
		cols:            cols,
		colTypes:        colTypes,
		qMap:            qMap,
		orderedDistVals: distVals,
		valActRows:      actRows,
	}
}

func (tv *singleColQuerier) init(ins tidb.Instance) (rerr error) {
	tv.initOnce.Do(func() {
		for i, tb := range tv.tbs {
			for j, col := range tv.cols[i] {
				begin := time.Now()
				q := fmt.Sprintf("SELECT %v, COUNT(*) FROM %v.`%v` where %v is not null GROUP BY %v ORDER BY COUNT(*)", col, tv.db, tb, col, col)
				rows, err := ins.Query(q)
				if err != nil {
					rerr = err
					return
				}
				for rows.Next() {
					var val string
					var cnt int
					if rerr = rows.Scan(&val, &cnt); rerr != nil {
						rows.Close()
						return
					}
					tv.orderedDistVals[i][j] = append(tv.orderedDistVals[i][j], val)
					tv.valActRows[i][j] = append(tv.valActRows[i][j], cnt)
				}
				if rerr = rows.Close(); rerr != nil {
					return
				}
				fmt.Printf("[SingleColQuerier-Init] table=%v, col=%v, sql=%v, cost=%v\n", tb, col, q, time.Since(begin))
			}
		}
	})
	return nil
}

func (tv *singleColQuerier) Collect(nSamples int, qt QueryType, ers []EstResult, ins tidb.Instance, ignoreErr bool) ([]EstResult, error) {
	if err := tv.init(ins); err != nil {
		return nil, err
	}

	tbIdx, colIdx := tv.qMap[qt][0], tv.qMap[qt][1]
	rowBegin, rowEnd := 0, tv.ndv(tbIdx, colIdx)
	if qt == QTSingleColMCVPointOnCol || qt == QTSingleColMCVPointOnIndex {
		numNDVs := tv.ndv(tbIdx, colIdx)
		numMCVs := numNDVs * 10 / 100 // 10%
		rowBegin = rowEnd - numMCVs
	}

	if nSamples == 0 {
		nSamples = rowEnd - rowBegin
	}
	sampleRate := float64(nSamples) / float64(rowEnd-rowBegin)
	if sampleRate > 1 {
		sampleRate = 1
	}

	concurrency := 64
	var wg sync.WaitGroup
	var resultLock sync.Mutex
	processed := 0

	begin := time.Now()
	for workerID := 0; workerID < concurrency; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for rowIdx := rowBegin + id; rowIdx < rowEnd; rowIdx += concurrency {
				if rand.Float64() > sampleRate {
					continue
				}
				cond, act := tv.pointCond(tbIdx, colIdx, rowIdx)
				q := fmt.Sprintf("SELECT * FROM %v.`%v` WHERE %v", tv.db, tv.tbs[tbIdx], cond)
				est, err := getEstRowFromExplain(ins, q)
				if err != nil {
					if !ignoreErr {
						panic(err)
					}
					fmt.Println(q, err)
					continue

				}
				resultLock.Lock()
				ers = append(ers, EstResult{q, est, float64(act)})
				processed++
				if processed%5000 == 0 {
					fmt.Printf("[SingleColQuerier-Process] ins=%v, table=%v, col=%v, qt=%v, concurrency=%v, time-cost=%v, progress (%v/%v)\n",
						ins.Opt().Label, tv.tbs[tbIdx], tv.cols[tbIdx][colIdx], qt, concurrency, time.Since(begin), processed, nSamples)
				}
				resultLock.Unlock()
			}
		}(workerID)
	}

	wg.Wait()
	return ers, nil
}

func (tv *singleColQuerier) ndv(tbIdx, colIdx int) int {
	return len(tv.orderedDistVals[tbIdx][colIdx])
}

func (tv *singleColQuerier) pointCond(tbIdx, colIdx, rowIdx int) (cond string, actRows int) {
	pattern := "%v=" + tv.colPlaceHolder(tbIdx, colIdx)
	cond = fmt.Sprintf(pattern, tv.cols[tbIdx][colIdx], tv.orderedDistVals[tbIdx][colIdx][rowIdx])
	actRows = tv.valActRows[tbIdx][colIdx][rowIdx]
	return
}

func (tv *singleColQuerier) colPlaceHolder(tbIdx, colIdx int) string {
	if tv.colTypes[tbIdx][colIdx] == DTString {
		return "'%v'"
	}
	return "%v"
}
