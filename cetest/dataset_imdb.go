package cetest

import "github.com/qw4990/OptimizerTester/tidb"

type datasetIMDB struct {
	opt DatasetOpt
	ins tidb.Instance
}

func newDatasetIMDB(opt DatasetOpt, ins tidb.Instance) (Dataset, error) {
	return &datasetIMDB{opt, ins}, nil
}

func (ds *datasetIMDB) Name() string {
	return "IMDB"
}

func (ds *datasetIMDB) GenCases(int, QueryType) ([]string, error) {
	// TODO
	return nil, nil
}
