package cetest

import (
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

type datasetTPCC struct {
	opt  DatasetOpt
	tv   *singleColQuerier
	args datasetArgs

	// fields for single-col-querier
	tbs  []string
	cols [][]string

	analyzed map[string]bool
	mu       sync.Mutex
}

func (ds *datasetTPCC) Name() string {
	return "TPCC"
}

func newDatasetTPCC(opt DatasetOpt) (Dataset, error) {
	tbs := []string{"order_line", "customer"}
	cols := [][]string{{"ol_amount"}, {"c_balance"}}

	args, err := parseArgs(opt.Args)
	if err != nil {
		return nil, err
	}

	return &datasetTPCC{
		opt:  opt,
		args: args,
		tbs:  tbs,
		cols: cols,
	}, nil
}

func (ds *datasetTPCC) lazyInit(ins tidb.Instance, qt QueryType) (err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if !ds.analyzed[ins.Opt().Label] && !ds.args.disableAnalyze {
		for _, tb := range ds.tbs {
			if err = ins.Exec(fmt.Sprintf("ANALYZE TABLE %v.%v", ds.opt.DB, tb)); err != nil {
				return
			}
		}
		ds.analyzed[ins.Opt().Label] = true
	}

	switch qt {
	case QTSingleColPointQueryOnCol, QTSingleColPointQueryOnIndex, QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		if ds.tv != nil {
			return nil
		}
		ds.tv, err = newSingleColQuerier(ins, ds.opt.DB, ds.tbs, ds.cols, nil)
	case QTMulColsPointQueryOnIndex, QTMulColsRangeQueryOnIndex:
		panic("TODO")
	}
	return
}

func (ds *datasetTPCC) GenEstResults(ins tidb.Instance, qt QueryType) (ers []EstResult, err error) {
	defer func(begin time.Time) {
		fmt.Printf("[GenEstResults] dataset=%v, ins=%v, qt=%v, cost=%v\n", ds.opt.Label, ins.Opt().Label, qt, time.Since(begin))
	}(time.Now())

	if err := ds.lazyInit(ins, qt); err != nil {
		return nil, err
	}

	switch qt {
	case QTSingleColPointQueryOnCol, QTSingleColPointQueryOnIndex:
		var tbIdx, colIdx int
		if qt == QTSingleColPointQueryOnCol {
			tbIdx, colIdx = 0, 0
		} else if qt == QTSingleColPointQueryOnIndex {
			tbIdx, colIdx = 1, 0
		}
		numNDVs := ds.tv.ndv(tbIdx, colIdx)
		ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, 0, numNDVs, ins, ers, ds.args.ignoreError)
	case QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		var tbIdx, colIdx int
		if qt == QTSingleColPointQueryOnCol {
			tbIdx, colIdx = 0, 0
		} else if qt == QTSingleColPointQueryOnIndex {
			tbIdx, colIdx = 1, 0
		}
		numNDVs := ds.tv.ndv(tbIdx, colIdx)
		numMCVs := numNDVs * 10 / 100 // 10%
		ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, numNDVs-numMCVs, numNDVs, ins, ers, ds.args.ignoreError)
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return
}
