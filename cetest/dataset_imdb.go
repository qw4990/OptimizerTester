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
			[][]string{{"phonetic_code"}, {"person_id"}},
			[][]DATATYPE{{DTString}, {DTInt}},
			map[QueryType][2]int{
				QTSingleColPointQueryOnCol:   {0, 0}, // SELECT * FROM title WHERE phonetic_code=?
				QTSingleColPointQueryOnIndex: {1, 0}, // SELECT * FROM cast_info WHERE person_id=?
				QTSingleColMCVPointOnCol:     {0, 0}, // SELECT * FROM title WHERE phonetic_code=?
				QTSingleColMCVPointOnIndex:   {1, 0}, // SELECT * FROM cast_info WHERE person_id=?
			}),
		mciq: newMulColIndexQuerier(opt.DB,
			[]string{"TITLE_production_year_episode_of_id_IDX"},
			[]string{"title"},
			[][]string{{"production_year", "episode_of_id"}},
			[][]DATATYPE{{DTInt, DTInt}},
			map[QueryType]int{
				QTMulColsRangeQueryOnIndex: 0,
				QTMulColsPointQueryOnIndex: 0,
			}),
	}}
}
