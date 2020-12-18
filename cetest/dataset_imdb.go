package cetest

import (
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

type datasetIMDB struct {
	opt DatasetOpt
	tv  *tableVals

	args     datasetArgs
	tbs      []string
	cols     [][]string
	colTypes [][]DATATYPE
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

func (ds *datasetIMDB) Init(instances []tidb.Instance, queryTypes []QueryType) (err error) {
	// if there are multiple instances, assume they have the same data
	if err := instances[0].Exec(fmt.Sprintf("USE %v", ds.opt.DB)); err != nil {
		return err
	}
	if ds.tv, err = newTableVals(instances[0], ds.tbs, ds.cols, ds.colTypes); err != nil {
		return
	}

	if !ds.args.disableAnalyze {
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
	}

	return nil
}

func (ds *datasetIMDB) GenEstResults(ins tidb.Instance, qt QueryType) (ers []EstResult, err error) {
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
			tbIdx, colIdx = 0, 0 // SELECT * FROM title WHERE phonetic_code = ?
		} else if qt == QTSingleColPointQueryOnIndex {
			tbIdx, colIdx = 1, 0 // SELECT * FROM cast_info WHERE movie_id = ?
		}
		numNDVs := ds.tv.numNDVs(tbIdx, colIdx)
		ers, err = ds.tv.collectEstResults(tbIdx, colIdx, 0, numNDVs, ins, ers, ds.args.ignoreError)
	case QTSingleColMCVPointOnCol, QTSingleColMCVPointOnIndex:
		var tbIdx, colIdx int
		if qt == QTSingleColMCVPointOnCol {
			tbIdx, colIdx = 0, 0 // SELECT * FROM title WHERE phonetic_code = ?
		} else if qt == QTSingleColMCVPointOnIndex {
			tbIdx, colIdx = 1, 0 // SELECT * FROM cast_info WHERE movie_id = ?
		}
		numNDVs := ds.tv.numNDVs(tbIdx, colIdx)
		numMCVs := numNDVs * 10 / 100 // 10%
		ers, err = ds.tv.collectEstResults(tbIdx, colIdx, numNDVs-numMCVs, numNDVs, ins, ers, ds.args.ignoreError)
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return
}
