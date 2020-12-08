package cetest

type datasetIMDB struct {
}

func (ds *datasetIMDB) Name() string {
	return "IMDB"
}

func (ds *datasetIMDB) GenCases(int, QueryType) ([]string, error) {
	// TODO
	return nil, nil
}
