package cetest

type datasetIMDB struct {
}

func (ds *datasetIMDB) Name() string {
	return "IMDB"
}

func (ds *datasetIMDB) GenCases(QueryType) (queries []string) {
	// TODO
	return nil
}
