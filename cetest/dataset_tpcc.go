package cetest

type datasetTPCC struct {
	datasetBase
}

func (ds *datasetTPCC) Name() string {
	return "TPCC"
}

func newDatasetTPCC(opt DatasetOpt) (Dataset, error) {
	tbs := []string{"order_line", "customer"}
	cols := [][]string{{"ol_amount"}, {"c_balance"}}

	args, err := parseArgs(opt.Args)
	if err != nil {
		return nil, err
	}

	return &datasetTPCC{datasetBase{
		opt:  opt,
		args: args,
		tbs:  tbs,
		cols: cols,
	}}, nil
}
