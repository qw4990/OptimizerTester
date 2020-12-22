package cetest

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

/*
	datasetZipFX's schemas are:
		CREATE TABLE tint ( a INT, b INT, KEY(a), KEY(a, b) )
		CREATE TABLE tdouble ( a DOUBLE, b DOUBLE, KEY(a), KEY(a, b) )
		CREATE TABLE tstring ( a VARCHAR(32), b VARCHAR(32), KEY(a), KEY(a, b) )
		CREATE TABLE tdatetime (a DATETIME, b DATATIME, KEY(a), KEY(a, b))
*/
type datasetZipFX struct {
	opt  DatasetOpt
	tv   *singleColQuerier
	mq   *mulColIndexQuerier
	args datasetArgs

	// fields for single-col-querier
	tbs      []string
	cols     [][]string
	colTypes [][]DATATYPE

	// fields for mul-col-index-queirer
	idxNames    []string
	idxTables   []string
	idxCols     [][]string
	idxColTypes [][]DATATYPE

	analyzed map[string]bool
	mu       sync.Mutex
}

func newDatasetZipFX(opt DatasetOpt) (Dataset, error) {
	tbs := []string{"tint", "tdouble", "tstring", "tdatetime"}
	cols := [][]string{{"a", "b"}, {"a", "b"}, {"a", "b"}, {"a", "b"}}
	colTypes := [][]DATATYPE{{DTInt, DTInt}, {DTDouble, DTDouble}, {DTString, DTString}, {DTInt, DTInt}}
	args, err := parseArgs(opt.Args)
	if err != nil {
		return nil, err
	}

	idxNames := []string{"a2"} // only support int now
	idxTables := []string{"tint"}
	idxCols := [][]string{{"a", "b"}}
	idxColTypes := [][]DATATYPE{{DTInt, DTInt}}

	for _, arg := range opt.Args {
		tmp := strings.Split(arg, "=")
		if len(tmp) != 2 {
			return nil, errors.Errorf("invalid argument %v", arg)
		}
		k, v := tmp[0], tmp[1]
		switch strings.ToLower(k) {
		case "types":
			vs := strings.Split(v, ",")
			newTbs := make([]string, 0, len(tbs))
			newCols := make([][]string, 0, len(cols))
			for tbIdx, tb := range tbs {
				picked := false
				for _, v := range vs {
					if strings.Contains(tb, strings.ToLower(v)) {
						picked = true
						break
					}
				}
				if picked {
					newTbs = append(newTbs, tbs[tbIdx])
					newCols = append(newCols, cols[tbIdx])
				}
				tbs, cols = newTbs, newCols
			}
		}
	}

	return &datasetZipFX{
		opt:         opt,
		args:        args,
		tbs:         tbs,
		cols:        cols,
		colTypes:    colTypes,
		idxNames:    idxNames,
		idxTables:   idxTables,
		idxCols:     idxCols,
		idxColTypes: idxColTypes,
	}, nil
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}

func (ds *datasetZipFX) lazyInit(ins tidb.Instance, qt QueryType) (err error) {
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

func (ds *datasetZipFX) GenEstResults(ins tidb.Instance, qt QueryType) (ers []EstResult, err error) {
	defer func(begin time.Time) {
		fmt.Printf("[GenEstResults] dataset=%v, ins=%v, qt=%v, cost=%v\n", ds.opt.Label, ins.Opt().Label, qt, time.Since(begin))
	}(time.Now())

	if err := ds.lazyInit(ins, qt); err != nil {
		return nil, err
	}

	switch qt {
	case QTSingleColPointQueryOnCol, QTSingleColPointQueryOnIndex:
		for tbIdx := 0; tbIdx < len(ds.tbs); tbIdx++ {
			var colIdx int
			if qt == QTSingleColPointQueryOnCol {
				colIdx = 1
			} else if qt == QTSingleColPointQueryOnIndex {
				colIdx = 0
			}
			numNDVs := ds.tv.ndv(tbIdx, colIdx)
			ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, 0, numNDVs, ins, ers, ds.args.ignoreError)
		}
	case QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		for tbIdx := 0; tbIdx < len(ds.tbs); tbIdx++ {
			var colIdx int
			if qt == QTSingleColMCVPointOnCol {
				colIdx = 1
			} else if qt == QTSingleColMCVPointOnIndex {
				colIdx = 0
			}
			numNDVs := ds.tv.ndv(tbIdx, colIdx)
			numMCVs := numNDVs * 10 / 100 // 10%
			ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, numNDVs-numMCVs, numNDVs, ins, ers, ds.args.ignoreError)
		}
	case QTMulColsPointQueryOnIndex, QTMulColsRangeQueryOnIndex:
		useRange := false
		if qt == QTMulColsRangeQueryOnIndex {
			useRange = true
		}
		ers, err = ds.mq.collectMulColIndexEstResult(0, useRange, ins, ers, ds.args.ignoreError) // only support int now
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return
}
