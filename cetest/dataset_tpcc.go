package cetest

import "github.com/qw4990/OptimizerTester/tidb"

type datasetTPCC struct {
	opt DatasetOpt
	ins tidb.Instance
}

func newDatasetTPCC(opt DatasetOpt, ins tidb.Instance) (Dataset, error) {
	return &datasetTPCC{opt, ins}, nil
}

func (ds *datasetTPCC) Name() string {
	return "TPCC"
}

func (ds *datasetTPCC) GenEstResults(n int, insts []tidb.Instance, qts []QueryType) ([][][]EstResult, error) {
	return nil, nil
}

func (ds *datasetTPCC) GenCases(int, QueryType) ([]string, error) {
	//  TODO
	return nil, nil
}
