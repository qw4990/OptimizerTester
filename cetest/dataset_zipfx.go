package cetest

import (
	"fmt"
	"math/rand"

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
	baseDataset
}

func newDatasetZipFX(opt DatasetOpt, ins tidb.Instance) (Dataset, error) {
	tbs := []string{"tint", "tdouble", "tstring", "tdatetime"}
	cols := [][]string{{"a", "b"}, {"a", "b"}, {"a", "b"}, {"a", "b"}}
	used := [][]bool{{true, true}, {true, true}, {true, true}, {true, true}}
	base, err := newBaseDataset(opt, ins, tbs, cols, used)
	return &datasetZipFX{base}, err
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}

func (ds *datasetZipFX) GenCases(n int, qt QueryType) ([]string, error) {
	if ds.orderedVals == nil {
		if err := ds.init(); err != nil {
			return nil, err
		}
	}

	sqls := make([]string, 0, n)
	switch qt {
	case QTSingleColPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randPointQuery(1))
		}
	case QTMultiColsPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randPointQuery(2))
		}
	case QTSingleColRangeQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randRangeQuery(1))
		}
	case QTMultiColsRangeQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randRangeQuery(2))
		}
	case QTMultiColsRangeQueryEQPrefix:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randRangeQueryEQPrefix())
		}
	case QTMCVPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randMCVLCVPointQuery(true))
		}
	case QTLCVPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randMCVLCVPointQuery(false))
		}
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt.String())
	}
	return sqls, nil
}

func (ds *datasetZipFX) randMCVLCVPointQuery(isMCV bool) string {
	tbIdx := rand.Intn(4)
	colIdx := rand.Intn(2)
	val := ""
	if isMCV {
		val = ds.mcv[tbIdx][colIdx][rand.Intn(len(ds.mcv[tbIdx][colIdx]))]
	} else {
		val = ds.lcv[tbIdx][colIdx][rand.Intn(len(ds.lcv[tbIdx][colIdx]))]
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v=%v", ds.tbs[tbIdx], ds.cols[colIdx], val)
}

func (ds *datasetZipFX) randRangeQueryEQPrefix() string {
	tbIdx := rand.Intn(4)
	cond := fmt.Sprintf("%v AND %v", ds.randPointColCond(tbIdx, 0), ds.randRangeColCond(tbIdx, 1))
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}

func (ds *datasetZipFX) randRangeQuery(cols int) string {
	tbIdx := rand.Intn(4)
	cond := ""
	if cols == 1 {
		cond = ds.randRangeColCond(tbIdx, rand.Intn(2))
	} else {
		cond = fmt.Sprintf("%v AND %v", ds.randRangeColCond(tbIdx, 0), ds.randRangeColCond(tbIdx, 1))
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}

func (ds *datasetZipFX) randPointQuery(cols int) string {
	tbIdx := rand.Intn(4)
	cond := ""
	if cols == 1 {
		cond = ds.randPointColCond(tbIdx, rand.Intn(2))
	} else {
		cond = fmt.Sprintf("%v AND %v", ds.randPointColCond(tbIdx, 0), ds.randPointColCond(tbIdx, 1))
	}
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", ds.tbs[tbIdx], cond)
}
