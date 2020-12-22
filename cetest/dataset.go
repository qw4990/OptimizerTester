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

type datasetBase struct {
	opt  DatasetOpt
	args datasetArgs

	// fields for single-col-querier
	tbs      []string
	cols     [][]string
	colTypes [][]DATATYPE
	tv       *singleColQuerier

	// fields for mul-col-index-queirer
	idxNames    []string
	idxTables   []string
	idxCols     [][]string
	idxColTypes [][]DATATYPE
	mq          *mulColIndexQuerier

	// fields for lazy init
	analyzed map[string]bool
	mu       sync.Mutex
}

func (ds *datasetBase) lazyInit(ins tidb.Instance, qt QueryType) (err error) {
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
		ds.tv, err = newSingleColQuerier(ins, ds.opt.DB, ds.tbs, ds.cols, ds.colTypes)
	case QTMulColsPointQueryOnIndex, QTMulColsRangeQueryOnIndex:
		if ds.mq != nil {
			return nil
		}
		ds.mq, err = newMulColIndexQuerier(ins, ds.opt.DB, ds.idxNames, ds.idxTables, ds.idxCols, ds.idxColTypes)
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
		numNDVs := ds.tv.ndv(tbIdx, colIdx)
		ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, 0, numNDVs, ins, ers, ds.args.ignoreError)
	case QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		var tbIdx, colIdx int
		if qt == QTSingleColMCVPointOnCol {
			tbIdx, colIdx = 0, 0 // SELECT * FROM title WHERE phonetic_code = ?
		} else if qt == QTSingleColMCVPointOnIndex {
			tbIdx, colIdx = 1, 0 // SELECT * FROM cast_info WHERE movie_id = ?
		}
		numNDVs := ds.tv.ndv(tbIdx, colIdx)
		numMCVs := numNDVs * 10 / 100 // 10%
		ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, numNDVs-numMCVs, numNDVs, ins, ers, ds.args.ignoreError)
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return
}
