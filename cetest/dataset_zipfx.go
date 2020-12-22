package cetest

import (
	"github.com/pingcap/errors"
	"strings"
)

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
	scqTbs := []string{"tint", "tdouble", "tstring", "tdatetime"}
	scqCols := [][]string{{"a", "b"}, {"a", "b"}, {"a", "b"}, {"a", "b"}}
	scqColTypes := [][]DATATYPE{{DTInt, DTInt}, {DTDouble, DTDouble}, {DTString, DTString}, {DTInt, DTInt}}

	mciqIdxs := []string{"a2"} // TODO: only support int now
	mciqTbs := []string{"tint"}
	mciqIdxCols := [][]string{{"a", "b"}}
	mciqColTypes := [][]DATATYPE{{DTInt, DTInt}}

	for _, arg := range opt.Args {
		tmp := strings.Split(arg, "=")
		if len(tmp) != 2 {
			panic(errors.Errorf("invalid argument %v", arg))
		}
		k, v := tmp[0], tmp[1]
		switch strings.ToLower(k) {
		case "types":
			vs := strings.Split(v, ",")

			// filter for scq
			newTbs := make([]string, 0, len(scqTbs))
			newCols := make([][]string, 0, len(scqCols))
			newColTypes := make([][]DATATYPE, 0, len(scqColTypes))
			for tbIdx, tb := range scqTbs {
				picked := false
				for _, v := range vs {
					if strings.Contains(tb, strings.ToLower(v)) {
						picked = true
						break
					}
				}
				if picked {
					newTbs = append(newTbs, scqTbs[tbIdx])
					newCols = append(newCols, scqCols[tbIdx])
					newColTypes = append(newColTypes, scqColTypes[tbIdx])
				}
				scqTbs, scqCols = newTbs, newCols
			}

			// filter for mciq
			// TODO
		}
	}

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
