package cetest

import (
	"fmt"
	"strings"
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
}

func (ds *datasetBase) GenEstResults(ins tidb.Instance, qt QueryType) (ers []EstResult, err error) {
	defer func(begin time.Time) {
		fmt.Printf("[GenEstResults] dataset=%v, ins=%v, qt=%v, cost=%v\n", ds.opt.Label, ins.Opt().Label, qt, time.Since(begin))
	}(time.Now())

	switch qt {
	case QTSingleColPointQueryOnCol, QTSingleColPointQueryOnIndex, QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		ers, err = ds.scq.Collect(qt, ers, ins, ds.args.ignoreError)
	case QTMulColsRangeQueryOnIndex, QTMulColsPointQueryOnIndex:
		ers, err = ds.mciq.Collect(qt, ers, ins, ds.args.ignoreError)
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return
}
