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
			[][]string{{"ol_amount"}, {"c_ytd_payment"}},
			[][]DATATYPE{{DTDouble}, {DTDouble}},
			map[QueryType][2]int{
				QTSingleColPointQueryOnCol:   {0, 0}, // select * from order_line where ol_amount = ?
				QTSingleColPointQueryOnIndex: {1, 0}, // select * from customer where c_ytd_payment = ?
				QTSingleColMCVPointOnCol:     {0, 0}, // select * from order_line where ol_amount = ?
				QTSingleColMCVPointOnIndex:   {1, 0}, // select * from customer where c_ytd_payment = ?
			}),
		mciq: newMulColIndexQuerier(opt.DB,
			[]string{"idx_c_discount_balance"},
			[]string{"customer"},
			[][]string{{"c_discount", "c_balance"}},
			[][]DATATYPE{{DTDouble, DTDouble}},
			map[QueryType]int{
				QTMulColsRangeQueryOnIndex: 0,
				QTMulColsPointQueryOnIndex: 0,
			}),
	}}
}
