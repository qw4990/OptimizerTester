package cetest

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
	"strings"
	"sync"
)

// Dataset ...
type Dataset interface {
	// Name returns the name of the dataset
	Name() string

	// Init ...
	Init(instances []tidb.Instance, queryTypes []QueryType) error

	// GenEstResults ...
	GenEstResults(ins tidb.Instance, qt QueryType) ([]EstResult, error)
}

type DATATYPE int

const (
	DTInt DATATYPE = iota
	DTDouble
	DTString
)

type tableVals struct {
	tbs             []string   // table names
	cols            [][]string // table columns' names
	colTypes        [][]DATATYPE
	orderedDistVals [][][]string // ordered distinct values
	valActRows      [][][]int    // actual row count
}

func newTableVals(ins tidb.Instance, tbs []string, cols [][]string, colTypes [][]DATATYPE) (*tableVals, error) {
	tv := &tableVals{
		tbs:             tbs,
		cols:            cols,
		colTypes:        colTypes,
		orderedDistVals: newStrArray(cols),
		valActRows:      newIntArray(cols),
	}
	return tv, fillTableVals(ins, tv)
}

func newStrArray(cols [][]string) [][][]string {
	vals := make([][][]string, len(cols))
	for i := range cols {
		vals[i] = make([][]string, len(cols[i]))
	}
	return vals
}

func newIntArray(cols [][]string) [][][]int {
	vals := make([][][]int, len(cols))
	for i := range cols {
		vals[i] = make([][]int, len(cols[i]))
	}
	return vals
}

func fillTableVals(ins tidb.Instance, tv *tableVals) error {
	for i, tb := range tv.tbs {
		for j, col := range tv.cols[i] {
			q := fmt.Sprintf("SELECT %v, COUNT(*) FROM %v where %v is not null GROUP BY %v ORDER BY COUNT(*)", col, tb, col, col)
			rows, err := ins.Query(q)
			if err != nil {
				return err
			}
			for rows.Next() {
				var val string
				var cnt int
				if err := rows.Scan(&val, &cnt); err != nil {
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

func (tv *tableVals) numNDVs(tbIdx, colIdx int) int {
	return len(tv.orderedDistVals[tbIdx][colIdx])
}

func (tv *tableVals) pointCond(tbIdx, colIdx, rowIdx int) (cond string, actRows int) {
	pattern := "%v=" + tv.colPlaceHolder(tbIdx, colIdx)
	cond = fmt.Sprintf(pattern, tv.cols[tbIdx][colIdx], tv.orderedDistVals[tbIdx][colIdx][rowIdx])
	actRows = tv.valActRows[tbIdx][colIdx][rowIdx]
	return
}

func (tv *tableVals) colPlaceHolder(tbIdx, colIdx int) string {
	if tv.colTypes[tbIdx][colIdx] == DTString {
		return "'%v'"
	}
	return "%v"
}

func (tv *tableVals) collectPointQueryEstResult(tbIdx, colIdx, rowBegin, rowEnd int, ins tidb.Instance, ers []EstResult, ignoreErr bool) ([]EstResult, error) {
	concurrency := 128
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
				q := fmt.Sprintf("SELECT * FROM %v WHERE %v", tv.tbs[tbIdx], cond)
				est, err := getEstRowFromExplain(ins, q)
				if err != nil && !ignoreErr {
					panic(err)
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
			fmt.Printf("[CollectPointQueryEstResult] access ins=%v, table=%v, col=%v, progress (%v/%v)\n", ins.Opt().Label, tv.tbs[tbIdx], tv.cols[tbIdx][colIdx], i-rowBegin, rowEnd-rowBegin)
		}
	}

	close(taskCh)
	wg.Wait()
	return ers, nil
}

type datasetArgs struct {
	disableAnalyze bool
	ignoreError    bool
}

func parseArgs(args []string) (datasetArgs, error) {
	var da datasetArgs
	for _, arg := range args {
		tmp := strings.Split(arg, "=")
		if len(tmp) != 2 {
			return da, errors.Errorf("invalid argument %v", arg)
		}
		k := tmp[0]
		switch strings.ToLower(k) {
		case "analyze":
			da.disableAnalyze = true
		case "error":
			da.ignoreError = true
		default:
			return da, errors.Errorf("unknown argument %v", arg)
		}
	}
	return da, nil
}
