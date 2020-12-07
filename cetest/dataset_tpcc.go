package cetest

type datasetTPCC struct {
}

func (ds *datasetTPCC) Name() string {
	return "TPCC"
}

func (ds *datasetTPCC) GenCases(QueryType) (queries []string) {
	//  TODO
	return nil
}
