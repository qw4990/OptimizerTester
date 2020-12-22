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
	tbs := []string{"tint", "tdouble", "tstring", "tdatetime"}
	cols := [][]string{{"a", "b"}, {"a", "b"}, {"a", "b"}, {"a", "b"}}
	colTypes := [][]DATATYPE{{DTInt, DTInt}, {DTDouble, DTDouble}, {DTString, DTString}, {DTInt, DTInt}}

	idxNames := []string{"a2"} // only support int now
	idxTables := []string{"tint"}
	idxCols := [][]string{{"a", "b"}}
	idxColTypes := [][]DATATYPE{{DTInt, DTInt}}

	for _, arg := range opt.Args {
		tmp := strings.Split(arg, "=")
		if len(tmp) != 2 {
			panic(errors.Errorf("invalid argument %v", arg))
		}
		k, v := tmp[0], tmp[1]
		switch strings.ToLower(k) {
		case "types":
			vs := strings.Split(v, ",")
			newTbs := make([]string, 0, len(tbs))
			newCols := make([][]string, 0, len(cols))
			for tbIdx, tb := range tbs {
				picked := false
				for _, v := range vs {
					if strings.Contains(tb, strings.ToLower(v)) {
						picked = true
						break
					}
				}
				if picked {
					newTbs = append(newTbs, tbs[tbIdx])
					newCols = append(newCols, cols[tbIdx])
				}
				tbs, cols = newTbs, newCols
			}
		}
	}

	return &datasetZipFX{datasetBase{
		opt:  opt,
		args: parseArgs(opt.Args),
		scq:  newSingleColQuerier(opt.DB, tbs, cols, colTypes),
		mciq: newMulColIndexQuerier(opt.DB, idxNames, idxTables, idxCols, idxColTypes),
	}}
}

func (ds *datasetZipFX) Name() string {
	return "ZipFX"
}
