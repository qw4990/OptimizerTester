package cetest

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"path"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

// QueryType ...
type QueryType string

const (
	QTSingleColPointQuery            QueryType = "single-col-point-query"              // where c = ?; where c in (?, ... ?)
	QTSingleColRangeQuery            QueryType = "single-col-range-query"              // where c >= ?; where c > ? and c < ?
	QTMultiColsPointQuery            QueryType = "multi-cols-point-query"              // where c1 = ? and c2 = ?
	QTMultiColsRangeQueryFixedPrefix QueryType = "multi-cols-range-query-fixed-prefix" // where c1 = ? and c2 > ?
	QTMultiColsRangeQuery            QueryType = "multi-cols-range-query"              // where c1 > ? and c2 > ?
	QTJoinEQ                         QueryType = "join-eq"                             // where t1.c = t2.c
	QTJoinNonEQ                      QueryType = "join-non-eq"                         // where t1.c > t2.c
	QTGroup                          QueryType = "group"                               // group by c
)

type DatasetOpt struct {
	Name string `toml:"name"`
	DB   string `toml:"db"`
}

type Option struct {
	QueryTypes []string      `toml:"query-types"`
	Datasets   []DatasetOpt  `toml:"datasets"`
	Instances  []tidb.Option `toml:"instances"`
	ReportDir  string        `toml:"report-dir"`
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
	opt, err := parseConfig(confPath)
	if err != nil {
		return err
	}

	instances, err := tidb.ConnectToInstances(opt.Instances)
	if err != nil {
		return errors.Trace(err)
	}

	collector := NewEstResultCollector(len(instances), len(opt.Datasets), len(opt.QueryTypes))
	// TODO: parallelize this loop
	for insIdx, ins := range instances {
		for dsIdx, dataset := range opt.Datasets {
			ds := datasetMap[dataset.Name]
			if err := ins.Exec("use " + dataset.DB); err != nil {
				return err
			}
			for qtIdx, qt := range opt.QueryTypes {
				qs := ds.GenCases(QueryType(qt))
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

	return nil
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

func parseConfig(confPath string) (Option, error) {
	confContent, err := ioutil.ReadFile(confPath)
	if err != nil {
		return Option{}, errors.Trace(err)
	}
	var opt Option
	if _, err := toml.Decode(string(confContent), &opt); err != nil {
		return Option{}, errors.Trace(err)
	}
	return opt, nil
}

// genReport generates a report with MarkDown format.
func genReport(opt Option, collector EstResultCollector) error {
	mdContent := bytes.Buffer{}
	for qtIdx, qt := range opt.QueryTypes {
		picPath, err := DrawBiasBoxPlotGroupByQueryType(opt, collector, qtIdx)
		if err != nil {
			return err
		}
		if _, err := mdContent.WriteString(fmt.Sprintf("- %v: %v\n", qt, picPath)); err != nil {
			return errors.Trace(err)
		}
	}
	return ioutil.WriteFile(path.Join(opt.ReportDir, "report.md"), mdContent.Bytes(), 0666)
}
