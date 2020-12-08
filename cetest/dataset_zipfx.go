package cetest

type datasetZipFX struct {
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}

func (ds *datasetZipFX) GenCases(int, QueryType) ([]string, error) {
	// TODO
	return nil, nil
}
