package cetest

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

type datasetIMDB struct {
	opt DatasetOpt
	tv  *tableVals

	disableAnalyze bool
	ignoreError    bool
	tbs            []string
	cols           [][]string
	colTypes       [][]DATATYPE
}

func (ds *datasetIMDB) Name() string {
	return "IMDB"
}

func newDatasetIMDB(opt DatasetOpt) (Dataset, error) {
	tbs := []string{"title", "cast_info"}
	cols := [][]string{{"phonetic_code"}, {"movie_id", "person_id"}}
	colTypes := [][]DATATYPE{{DTString}, {DTInt, DTInt}}
	disableAnalyze := false
	ignoreError := false
	for _, arg := range opt.Args {
		tmp := strings.Split(arg, "=")
		if len(tmp) != 2 {
			return nil, errors.Errorf("invalid argument %v", arg)
		}
		k := tmp[0]
		switch strings.ToLower(k) {
		case "analyze":
			disableAnalyze = true
		case "error":
			ignoreError = true
		default:
			return nil, errors.Errorf("unknown argument %v", arg)
		}
	}
	return &datasetIMDB{
		opt:            opt,
		disableAnalyze: disableAnalyze,
		ignoreError:    ignoreError,
		tbs:            tbs,
		cols:           cols,
		colTypes:       colTypes,
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

	return nil
}

func (ds *datasetIMDB) GenEstResults(n int, ins tidb.Instance, qt QueryType) ([]EstResult, error) {
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
			tbIdx := 0
			cond, act := ds.tv.randPointCond(tbIdx, 0)
			// SELECT * FROM title WHERE phonetic_code = ?
			q := fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
			est, err := getEstRowFromExplain(ins, q)
			if err != nil {
				if ds.ignoreError {
					continue
				}
				return nil, err
			}
			ers = append(ers, EstResult{q, est, float64(act)})
		}
	case QTSingleColPointQueryOnIndex:
		for i := 0; i < n; i++ {
			tbIdx := 1
			colIdx := rand.Intn(2)
			cond, act := ds.tv.randPointCond(tbIdx, colIdx)
			// SELECT * FROM cast_info WHERE {movie_id|person_id} = ?
			q := fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
			est, err := getEstRowFromExplain(ins, q)
			if err != nil {
				if ds.ignoreError {
					continue
				}
				return nil, err
			}
			ers = append(ers, EstResult{q, est, float64(act)})
		}
	case QTSingleColMCVPointOnCol:
		for i := 0; i < n; i++ {
			tbIdx := 0
			cond, act := ds.tv.randMCVPointCond(tbIdx, 0, 10)
			// SELECT * FROM title WHERE phonetic_code = ?
			q := fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
			est, err := getEstRowFromExplain(ins, q)
			if err != nil {
				if ds.ignoreError {
					continue
				}
				return nil, err
			}
			ers = append(ers, EstResult{q, est, float64(act)})
		}
	case QTSingleColMCVPointOnIndex:
		for i := 0; i < n; i++ {
			tbIdx := 1
			colIdx := rand.Intn(2)
			cond, act := ds.tv.randMCVPointCond(tbIdx, colIdx, 10)
			// SELECT * FROM cast_info WHERE {movie_id|person_id} = ?
			q := fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
			est, err := getEstRowFromExplain(ins, q)
			if err != nil {
				if ds.ignoreError {
					continue
				}
				return nil, err
			}
			ers = append(ers, EstResult{q, est, float64(act)})
		}
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt)
	}
	return ers, nil
}
