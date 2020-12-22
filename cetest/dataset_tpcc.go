package cetest

type datasetTPCC struct {
	datasetBase
}

func (ds *datasetTPCC) Name() string {
	return "TPCC"
}

func newDatasetTPCC(opt DatasetOpt) Dataset {
	return &datasetTPCC{datasetBase{
		opt:  opt,
		args: parseArgs(opt.Args),
		scq: newSingleColQuerier(opt.DB,
			[]string{"order_line", "customer"},
			[][]string{{"ol_amount"}, {"c_balance"}},
			nil),
	}}
}
