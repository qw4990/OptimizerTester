package cetest

import (
	"fmt"
	"strings"
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
	opt DatasetOpt
	tv  *tableVals

	args     datasetArgs
	tbs      []string
	cols     [][]string
	colTypes [][]DATATYPE
}

func newDatasetZipFX(opt DatasetOpt) (Dataset, error) {
	tbs := []string{"tint", "tdouble", "tstring", "tdatetime"}
	cols := [][]string{{"a", "b"}, {"a", "b"}, {"a", "b"}, {"a", "b"}}
	colTypes := [][]DATATYPE{{DTInt, DTInt}, {DTDouble, DTDouble}, {DTString, DTString}, {DTInt, DTInt}}
	args, err := parseArgs(opt.Args)
	if err != nil {
		return nil, err
	}
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
		opt:      opt,
		args:     args,
		tbs:      tbs,
		cols:     cols,
		colTypes: colTypes,
	}, nil
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}

func (ds *datasetZipFX) Init(instances []tidb.Instance, queryTypes []QueryType) (err error) {
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
	return
}

func (ds *datasetZipFX) GenEstResults(ins tidb.Instance, qt QueryType) (ers []EstResult, err error) {
	defer func(begin time.Time) {
		fmt.Printf("[GenEstResults] dataset=%v, ins=%v, qt=%v, cost=%v\n", ds.opt.Label, ins.Opt().Label, qt, time.Since(begin))
	}(time.Now())

	if err := ins.Exec(fmt.Sprintf("USE %v", ds.opt.DB)); err != nil {
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
			numNDVs := ds.tv.numNDVs(tbIdx, colIdx)
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
			numNDVs := ds.tv.numNDVs(tbIdx, colIdx)
			numMCVs := numNDVs * 10 / 100 // 10%
			ers, err = ds.tv.collectPointQueryEstResult(tbIdx, colIdx, numNDVs-numMCVs, numNDVs, ins, ers, ds.args.ignoreError)
		}
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return
}
