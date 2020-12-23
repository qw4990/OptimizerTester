package cetest

/*
	datasetZipFX's schemas are:
		CREATE TABLE tint ( a INT, b INT, KEY(a), KEY(a, b) )
		CREATE TABLE tdouble ( a DOUBLE, b DOUBLE, KEY(a), KEY(a, b) )
		CREATE TABLE tstring ( a VARCHAR(32), b VARCHAR(32), KEY(a), KEY(a, b) )
		CREATE TABLE tdatetime (a DATETIME, b DATATIME, KEY(a), KEY(a, b))
*/
type datasetZipFX struct {
	datasetBase
}

func newDatasetZipFX(opt DatasetOpt) Dataset {
	// TODO: only support int now
	scqTbs := []string{"tint"}
	scqCols := [][]string{{"a", "b"}}
	scqColTypes := [][]DATATYPE{{DTInt, DTInt}}

	mciqIdxs := []string{"a2"}
	mciqTbs := []string{"tint"}
	mciqIdxCols := [][]string{{"a", "b"}}
	mciqColTypes := [][]DATATYPE{{DTInt, DTInt}}

	return &datasetZipFX{datasetBase{
		opt:  opt,
		args: parseArgs(opt.Args),
		scq:  newSingleColQuerier(opt.DB, scqTbs, scqCols, scqColTypes),
		mciq: newMulColIndexQuerier(opt.DB, mciqIdxs, mciqTbs, mciqIdxCols, mciqColTypes),
	}}
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}
