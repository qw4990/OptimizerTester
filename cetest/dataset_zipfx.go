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
	scqMap := map[QueryType][2]int{
		QTSingleColPointQueryOnCol:   {0, 1}, // SELECT * FROM tint WHERE b=?
		QTSingleColPointQueryOnIndex: {0, 0}, // SELECT * FROM tint WHERE a=?
		QTSingleColMCVPointOnCol:     {0, 1}, // SELECT * FROM tint WHERE b=?
		QTSingleColMCVPointOnIndex:   {0, 0}, // SELECT * FROM tint WHERE a=?
	}

	mciqIdxs := []string{"a_2"}
	mciqTbs := []string{"tint"}
	mciqIdxCols := [][]string{{"a", "b"}}
	mciqColTypes := [][]DATATYPE{{DTInt, DTInt}}
	mciqMap := map[QueryType]int{
		QTMulColsPointQueryOnIndex: 0, // SELECT * FROM tint WHERE a=? AND b=?
		QTMulColsRangeQueryOnIndex: 0, // SELECT * FROM tint WHERE a=? AND b>=? AND b<=?
	}

	return &datasetZipFX{datasetBase{
		opt:  opt,
		args: parseArgs(opt.Args),
		scq:  newSingleColQuerier(opt.DB, scqTbs, scqCols, scqColTypes, scqMap),
		mciq: newMulColIndexQuerier(opt.DB, mciqIdxs, mciqTbs, mciqIdxCols, mciqColTypes, mciqMap),
	}}
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}
