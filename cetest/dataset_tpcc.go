package cetest

import (
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

type datasetTPCC struct {
	opt DatasetOpt
	tv  *tableVals

	args datasetArgs
	tbs  []string
	cols [][]string
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

func (ds *datasetTPCC) Init(instances []tidb.Instance, queryTypes []QueryType) (err error) {
	// if there are multiple instances, assume they have the same data
	if err := instances[0].Exec(fmt.Sprintf("USE %v", ds.opt.DB)); err != nil {
		return err
	}
	if ds.tv, err = newTableVals(instances[0], ds.tbs, ds.cols, nil); err != nil {
		return
	}

	for _, ins := range instances {
		if err := ins.Exec(fmt.Sprintf("USE %v", ds.opt.DB)); err != nil {
			return err
		}
		for _, tb := range ds.tbs {
			if err = ins.Exec(fmt.Sprintf("ANALYZE TABLE %v", tb)); err != nil {
				return
			}
		}
	}

	return nil
}

func (ds *datasetTPCC) GenEstResults(ins tidb.Instance, qt QueryType) (ers []EstResult, err error) {
	defer func(begin time.Time) {
		fmt.Printf("[GenEstResults] dataset=%v, ins=%v, qt=%v, cost=%v\n", ds.opt.Label, ins.Opt().Label, qt, time.Since(begin))
	}(time.Now())

	if err := ins.Exec(fmt.Sprintf("USE %v", ds.opt.DB)); err != nil {
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
		numNDVs := ds.tv.numNDVs(tbIdx, colIdx)
		ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, 0, numNDVs, ins, ers, ds.args.ignoreError)
	case QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		var tbIdx, colIdx int
		if qt == QTSingleColPointQueryOnCol {
			tbIdx, colIdx = 0, 0
		} else if qt == QTSingleColPointQueryOnIndex {
			tbIdx, colIdx = 1, 0
		}
		numNDVs := ds.tv.numNDVs(tbIdx, colIdx)
		numMCVs := numNDVs * 10 / 100 // 10%
		ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, numNDVs-numMCVs, numNDVs, ins, ers, ds.args.ignoreError)
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return
}
