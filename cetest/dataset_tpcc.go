package cetest

type datasetTPCC struct {
}

func (ds *datasetTPCC) Name() string {
	return "TPCC"
}

func (ds *datasetTPCC) GenCases(int, QueryType) ([]string, error) {
	//  TODO
	return nil, nil
}
