package cetest

import (
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
	cols := [][]string{{"a", "b"}, {"a", "b"}}
	base, err := newBaseDataset(opt, ins, tbs, cols)
	return &datasetZipFX{base}, err
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}
