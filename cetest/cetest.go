package cetest

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

type DatasetOpt struct {
	Name  string `toml:"name"`
	DB    string `toml:"db"`
	Label string `toml:"label"`
}

type Option struct {
	QueryTypes []QueryType   `toml:"query-types"`
	Datasets   []DatasetOpt  `toml:"datasets"`
	Instances  []tidb.Option `toml:"instances"`
	ReportDir  string        `toml:"report-dir"`
}

// DecodeOption decodes option content.
func DecodeOption(content string) (Option, error) {
	var opt Option
	if _, err := toml.Decode(content, &opt); err != nil {
		return Option{}, errors.Trace(err)
	}
	return opt, nil
}

// QueryType ...
type QueryType int

const (
	QTSingleColPointQuery            QueryType = 0 // where c = ?; where c in (?, ... ?)
	QTSingleColRangeQuery            QueryType = 1 // where c >= ?; where c > ? and c < ?
	QTMultiColsPointQuery            QueryType = 2 // where c1 = ? and c2 = ?
	QTMultiColsRangeQueryFixedPrefix QueryType = 3 // where c1 = ? and c2 > ?
	QTMultiColsRangeQuery            QueryType = 4 // where c1 > ? and c2 > ?
	QTJoinEQ                         QueryType = 5 // where t1.c = t2.c
	QTJoinNonEQ                      QueryType = 6 // where t1.c > t2.c
	QTGroup                          QueryType = 7 // group by c
)

var (
	qtNameMap = map[QueryType]string{
		QTSingleColPointQuery:            "single-col-point-query",
		QTSingleColRangeQuery:            "single-col-range-query",
		QTMultiColsPointQuery:            "multi-cols-point-query",
		QTMultiColsRangeQueryFixedPrefix: "multi-cols-range-query-fixed-prefix",
		QTMultiColsRangeQuery:            "multi-cols-range-query",
		QTJoinEQ:                         "join-eq",
		QTJoinNonEQ:                      "join-non-eq",
		QTGroup:                          "group",
	}
)

func (qt QueryType) String() string {
	return qtNameMap[qt]
}

func (qt *QueryType) UnmarshalText(text []byte) error {
	for k, v := range qtNameMap {
		if v == string(text) {
			*qt = k
			return nil
		}
	}
	return errors.Errorf("unknown query-type=%v", string(text))
}

// Dataset ...
type Dataset interface {
	// Name returns the name of the dataset
	Name() string

	// GenCases ...
	GenCases(QueryType) (queries []string)
}

var datasetMap = map[string]Dataset{ // read-only
	"zipx": new(datasetZipFX),
	"imdb": new(datasetIMDB),
	"tpcc": new(datasetTPCC),
}

func RunCETestWithConfig(confPath string) error {
	confContent, err := ioutil.ReadFile(confPath)
	if err != nil {
		return errors.Trace(err)
	}
	opt, err := DecodeOption(string(confContent))
	if err != nil {
		return err
	}

	instances, err := tidb.ConnectToInstances(opt.Instances)
	if err != nil {
		return errors.Trace(err)
	}

	collector := NewEstResultCollector(len(instances), len(opt.Datasets), len(opt.QueryTypes))
	// TODO: parallelize this loop to speed up
	for insIdx, ins := range instances {
		for dsIdx, dataset := range opt.Datasets {
			ds := datasetMap[dataset.Name]
			if err := ins.Exec("use " + dataset.DB); err != nil {
				return err
			}
			for qtIdx, qt := range opt.QueryTypes {
				qs := ds.GenCases(qt)
				for _, q := range qs {
					estResult, err := runOneEstCase(ins, q)
					if err != nil {
						return err
					}
					collector.AddEstResult(insIdx, dsIdx, qtIdx, estResult)
				}
			}
		}
	}

	return GenReport(opt, collector)
}

func runOneEstCase(ins tidb.Instance, query string) (EstResult, error) {
	rows, err := ins.Query(query)
	if err != nil {
		return EstResult{}, errors.Trace(err)
	}
	defer rows.Close()
	return parseEstResult(rows)
}

func parseEstResult(rows *sql.Rows) (EstResult, error) {
	// TODO
	return EstResult{}, nil
}

// GenReport generates a report with MarkDown format.
func GenReport(opt Option, collector EstResultCollector) error {
	mdContent := bytes.Buffer{}
	for qtIdx, qt := range opt.QueryTypes {
		picPath, err := DrawBiasBoxPlotGroupByQueryType(opt, collector, qtIdx)
		if err != nil {
			return err
		}
		if _, err := mdContent.WriteString(fmt.Sprintf("%v: ![pic](%v)\n", qt, picPath)); err != nil {
			return errors.Trace(err)
		}
	}
	return ioutil.WriteFile(path.Join(opt.ReportDir, "report.md"), mdContent.Bytes(), 0666)
}
