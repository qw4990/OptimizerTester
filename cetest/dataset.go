package cetest

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

// Dataset ...
type Dataset interface {
	// Name returns the name of the dataset
	Name() string

	// GenEstResults ...
	GenEstResults(ins tidb.Instance, qt QueryType) ([]EstResult, error)
}

type DATATYPE int

const (
	DTInt DATATYPE = iota
	DTDouble
	DTString
)

type datasetArgs struct {
	disableAnalyze bool
	ignoreError    bool
}

func parseArgs(args []string) datasetArgs {
	var da datasetArgs
	for _, arg := range args {
		tmp := strings.Split(arg, "=")
		if len(tmp) != 2 {
			panic(errors.Errorf("invalid argument %v", arg))
		}
		k := tmp[0]
		switch strings.ToLower(k) {
		case "analyze":
			da.disableAnalyze = true
		case "error":
			da.ignoreError = true
		default:
			panic(errors.Errorf("unknown argument %v", arg))
		}
	}
	return da
}

type datasetBase struct {
	opt  DatasetOpt
	args datasetArgs

	scq  *singleColQuerier
	mciq *mulColIndexQuerier

	// fields for lazy init
	analyzed map[string]bool // ins+tbl => inited
	mu       sync.Mutex
}

func (ds *datasetBase) lazyInit(ins tidb.Instance, qt QueryType) (err error) {
	switch qt {
	case QTSingleColPointQueryOnCol, QTSingleColPointQueryOnIndex, QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		return ds.scq.init(ins)
	case QTMulColsPointQueryOnIndex, QTMulColsRangeQueryOnIndex:
		return ds.mciq.init(ins)
	}
	return
}

func (ds *datasetBase) GenEstResults(ins tidb.Instance, qt QueryType) (ers []EstResult, err error) {
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
			tbIdx, colIdx = 0, 0 // SELECT * FROM title WHERE phonetic_code = ?
		} else if qt == QTSingleColPointQueryOnIndex {
			tbIdx, colIdx = 1, 0 // SELECT * FROM cast_info WHERE movie_id = ?
		}
		numNDVs := ds.scq.ndv(tbIdx, colIdx)
		ers, err = ds.scq.collectPointQueryEstResult(tbIdx, colIdx, 0, numNDVs, ins, ers, ds.args.ignoreError)
	case QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		var tbIdx, colIdx int
		if qt == QTSingleColMCVPointOnCol {
			tbIdx, colIdx = 0, 0 // SELECT * FROM title WHERE phonetic_code = ?
		} else if qt == QTSingleColMCVPointOnIndex {
			tbIdx, colIdx = 1, 0 // SELECT * FROM cast_info WHERE movie_id = ?
		}
		numNDVs := ds.scq.ndv(tbIdx, colIdx)
		numMCVs := numNDVs * 10 / 100 // 10%
		ers, err = ds.scq.collectPointQueryEstResult(tbIdx, colIdx, numNDVs-numMCVs, numNDVs, ins, ers, ds.args.ignoreError)
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return
}
