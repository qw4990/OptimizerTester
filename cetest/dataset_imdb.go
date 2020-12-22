package cetest

import (
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

type datasetIMDB struct {
	opt  DatasetOpt
	tv   *singleColQuerier
	args datasetArgs

	// fields for single-col-querier
	tbs      []string
	cols     [][]string
	colTypes [][]DATATYPE

	analyzed map[string]bool
	mu       sync.Mutex
}

func (ds *datasetIMDB) Name() string {
	return "IMDB"
}

func newDatasetIMDB(opt DatasetOpt) (Dataset, error) {
	tbs := []string{"title", "cast_info"}
	cols := [][]string{{"phonetic_code"}, {"movie_id"}}
	colTypes := [][]DATATYPE{{DTString}, {DTInt}}
	args, err := parseArgs(opt.Args)
	if err != nil {
		return nil, err
	}
	return &datasetIMDB{
		opt:      opt,
		args:     args,
		tbs:      tbs,
		cols:     cols,
		colTypes: colTypes,
	}, nil
}

func (ds *datasetIMDB) lazyInit(ins tidb.Instance, qt QueryType) (err error) {
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
		panic("TODO")
	}
	return
}

func (ds *datasetIMDB) GenEstResults(ins tidb.Instance, qt QueryType) (ers []EstResult, err error) {
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
