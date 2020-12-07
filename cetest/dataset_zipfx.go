package cetest

type datasetZipFX struct {
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}

func (ds *datasetZipFX) GenCases(QueryType) (queries []string) {
	// TODO
	return nil
}
