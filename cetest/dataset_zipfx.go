package cetest

import (
	"fmt"
	"math/rand"
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

	disableAnalyze bool
	tbs            []string
	cols           [][]string
}

func newDatasetZipFX(opt DatasetOpt) (Dataset, error) {
	tbs := []string{"tint", "tdouble", "tstring", "tdatetime"}
	cols := [][]string{{"a", "b"}, {"a", "b"}, {"a", "b"}, {"a", "b"}}
	disableAnalyze := false
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
		case "analyze":
			disableAnalyze = true
		default:
			return nil, errors.Errorf("unknown argument %v", arg)
		}
	}

	return &datasetZipFX{
		opt:            opt,
		disableAnalyze: disableAnalyze,
		tbs:            tbs,
		cols:           cols,
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
	if ds.tv, err = newTableVals(instances[0], ds.tbs, ds.cols); err != nil {
		return
	}

	if !ds.disableAnalyze {
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

func (ds *datasetZipFX) GenEstResults(n int, ins tidb.Instance, qt QueryType) ([]EstResult, error) {
	defer func(begin time.Time) {
		fmt.Printf("[GenEstResults] n=%v, dataset=%v, ins=%v, qt=%v, cost=%v\n", n, ds.opt.Label, ins.Opt().Label, qt, time.Since(begin))
	}(time.Now())

	if err := ins.Exec(fmt.Sprintf("USE %v", ds.opt.DB)); err != nil {
		return nil, err
	}

	ers := make([]EstResult, 0, n)
	switch qt {
	case QTSingleColPointQueryOnCol:
		for i := 0; i < n; i++ {
			tbIdx := rand.Intn(len(ds.tbs))
			cond, act := ds.tv.randPointCond(tbIdx, 1)
			q := fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
			est, err := getEstRowFromExplain(ins, q)
			if err != nil {
				return nil, err
			}
			ers = append(ers, EstResult{q, est, float64(act)})
		}
	case QTSingleColPointQueryOnIndex:
		for i := 0; i < n; i++ {
			tbIdx := rand.Intn(len(ds.tbs))
			cond, act := ds.tv.randPointCond(tbIdx, 0)
			q := fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
			est, err := getEstRowFromExplain(ins, q)
			if err != nil {
				return nil, err
			}
			ers = append(ers, EstResult{q, est, float64(act)})
		}
	case QTSingleColMCVPointOnCol:
		for i := 0; i < n; i++ {
			tbIdx := rand.Intn(len(ds.tbs))
			cond, act := ds.tv.randMCVPointCond(tbIdx, 1, 10)
			q := fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
			est, err := getEstRowFromExplain(ins, q)
			if err != nil {
				return nil, err
			}
			ers = append(ers, EstResult{q, est, float64(act)})
		}
	case QTSingleColMCVPointOnIndex:
		for i := 0; i < n; i++ {
			tbIdx := rand.Intn(len(ds.tbs))
			cond, act := ds.tv.randMCVPointCond(tbIdx, 0, 10)
			q := fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
			est, err := getEstRowFromExplain(ins, q)
			if err != nil {
				return nil, err
			}
			ers = append(ers, EstResult{q, est, float64(act)})
		}
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return ers, nil
}
