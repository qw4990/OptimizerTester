package cetest

type datasetIMDB struct {
	datasetBase
}

func (ds *datasetIMDB) Name() string {
	return "IMDB"
}

func newDatasetIMDB(opt DatasetOpt) (Dataset, error) {
	tbs := []string{"title", "cast_info"}
	cols := [][]string{{"phonetic_code"}, {"movie_id"}}
	colTypes := [][]DATATYPE{{DTString}, {DTInt}}
	args, err := parseArgs(opt.Args)
	if err != nil {
		return nil, err
	}
	return &datasetIMDB{datasetBase{
		opt:      opt,
		args:     args,
		tbs:      tbs,
		cols:     cols,
		colTypes: colTypes,
	}}, nil
}
