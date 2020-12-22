package cetest

import (
	"fmt"
	"sync"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

// singleColQuerier supports QTSingleColPointQueryOnCol, QTSingleColPointQueryOnIndex, QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex
type singleColQuerier struct {
	db              string
	tbs             []string   // table names
	cols            [][]string // table columns' names
	colTypes        [][]DATATYPE
	orderedDistVals [][][]string // ordered distinct values
	valActRows      [][][]int    // actual row count
}

func newSingleColQuerier(ins tidb.Instance, db string, tbs []string, cols [][]string, colTypes [][]DATATYPE) (*singleColQuerier, error) {
	distVals := make([][][]string, len(cols))
	actRows := make([][][]int, len(cols))
	for i := range cols {
		distVals[i] = make([][]string, len(cols[i]))
		actRows[i] = make([][]int, len(cols[i]))
	}

	tv := &singleColQuerier{
		db:              db,
		tbs:             tbs,
		cols:            cols,
		colTypes:        colTypes,
		orderedDistVals: distVals,
		valActRows:      actRows,
	}
	return tv, tv.init(ins)
}

func (tv *singleColQuerier) init(ins tidb.Instance) error {
	for i, tb := range tv.tbs {
		for j, col := range tv.cols[i] {
			q := fmt.Sprintf("SELECT %v, COUNT(*) FROM %v.%v where %v is not null GROUP BY %v ORDER BY COUNT(*)", col, tv.db, tb, col, col)
			rows, err := ins.Query(q)
			if err != nil {
				return err
			}
			for rows.Next() {
				var val string
				var cnt int
				if err := rows.Scan(&val, &cnt); err != nil {
					rows.Close()
					return err
				}
				tv.orderedDistVals[i][j] = append(tv.orderedDistVals[i][j], val)
				tv.valActRows[i][j] = append(tv.valActRows[i][j], cnt)
			}
			if err := rows.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tv *singleColQuerier) numNDVs(tbIdx, colIdx int) int {
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

func (tv *singleColQuerier) collectPointQueryEstResult(tbIdx, colIdx, rowBegin, rowEnd int, ins tidb.Instance, ers []EstResult, ignoreErr bool) ([]EstResult, error) {
	begin := time.Now()
	concurrency := 64
	var wg sync.WaitGroup
	taskCh := make(chan int, concurrency)
	resultCh := make(chan EstResult, concurrency)
	for workerID := 0; workerID < concurrency; workerID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				rowIdx, ok := <-taskCh
				if !ok {
					return
				}

				cond, act := tv.pointCond(tbIdx, colIdx, rowIdx)
				q := fmt.Sprintf("SELECT * FROM %v.%v WHERE %v", tv.db, tv.tbs[tbIdx], cond)
				est, err := getEstRowFromExplain(ins, q)
				if err != nil {
					if !ignoreErr {
						panic(err)
					}
					fmt.Println(q, err)
					continue

				}
				resultCh <- EstResult{q, est, float64(act)}
			}
		}()
	}

	wg.Add(1)
	go func() { // task deliverer
		defer wg.Done()
		for i := rowBegin; i < rowEnd; i++ {
			taskCh <- i
		}
	}()

	for i := rowBegin; i < rowEnd; i++ {
		er := <-resultCh
		ers = append(ers, er)
		if i-rowBegin > 0 && (i-rowBegin)%5000 == 0 {
			fmt.Printf("[CollectPointQueryEstResult] access ins=%v, table=%v, col=%v, concurrency=%v, time-cost=%v, progress (%v/%v)\n",
				ins.Opt().Label, tv.tbs[tbIdx], tv.cols[tbIdx][colIdx], concurrency, time.Since(begin), i-rowBegin, rowEnd-rowBegin)
		}
	}

	close(taskCh)
	wg.Wait()
	return ers, nil
}