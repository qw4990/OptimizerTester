package cetest

type datasetIMDB struct {
	datasetBase
}

func (ds *datasetIMDB) Name() string {
	return "IMDB"
}

func newDatasetIMDB(opt DatasetOpt) Dataset {
	return &datasetIMDB{datasetBase{
		opt:  opt,
		args: parseArgs(opt.Args),
		scq: newSingleColQuerier(opt.DB,
			[]string{"title", "cast_info"},
			[][]string{{"phonetic_code"}, {"movie_id"}},
			[][]DATATYPE{{DTString}, {DTInt},
			}),
	}}
}
