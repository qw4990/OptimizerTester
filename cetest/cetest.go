package cetest

import (
	"fmt"
	"io/ioutil"
	"strings"
	"sync"

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
	N          int           `toml:"n"`
}

// DecodeOption decodes option content.
func DecodeOption(content string) (Option, error) {
	var opt Option
	if _, err := toml.Decode(content, &opt); err != nil {
		return Option{}, errors.Trace(err)
	}
	for _, ds := range opt.Datasets {
		if _, ok := datasetMap[strings.ToLower(ds.Name)]; !ok {
			return Option{}, fmt.Errorf("unknown dateset=%v", ds.Name)
		}
	}
	return opt, nil
}

// QueryType ...
type QueryType int

const (
	QTSingleColPointQuery         QueryType = iota // where c = ?; where c in (?, ... ?)
	QTSingleColRangeQuery                          // where c >= ?; where c > ? and c < ?
	QTMultiColsPointQuery                          // where c1 = ? and c2 = ?
	QTMultiColsRangeQueryEQPrefix                  // where c1 = ? and c2 > ?
	QTMultiColsRangeQuery                          // where c1 > ? and c2 > ?
	QTMCVPointQuery                                // point query on most common values (10%)
	QTLCVPointQuery                                // point query on least common values (10%)
	QTJoinEQ                                       // where t1.c = t2.c
	QTJoinNonEQ                                    // where t1.c > t2.c
	QTGroup                                        // group by c
)

var (
	qtNameMap = map[QueryType]string{
		QTSingleColPointQuery:         "single-col-point-query",
		QTSingleColRangeQuery:         "single-col-range-query",
		QTMultiColsPointQuery:         "multi-cols-point-query",
		QTMultiColsRangeQueryEQPrefix: "multi-cols-range-query-eq-prefix",
		QTMultiColsRangeQuery:         "multi-cols-range-query",
		QTMCVPointQuery:               "most-common-value-point-query(10%)",
		QTLCVPointQuery:               "least-common-value-point-query(10%)",
		QTJoinEQ:                      "join-eq",
		QTJoinNonEQ:                   "join-non-eq",
		QTGroup:                       "group",
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

var datasetMap = map[string]func(DatasetOpt, tidb.Instance) (Dataset, error){ // read-only
	"zipfx": newDatasetZipFX,
	"imdb":  newDatasetIMDB,
	"tpcc":  newDatasetTPCC,
	"mock":  newDatasetMock,
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
	defer func() {
		for _, ins := range instances {
			ins.Close()
		}
	}()

	datasets := make([][]Dataset, len(instances)*len(opt.Datasets)) // DS[insIdx][dsIdx]
	for i := range instances {
		datasets[i] = make([]Dataset, len(opt.Datasets))
		for j := range opt.Datasets {
			var err error
			datasets[i][j], err = datasetMap[opt.Datasets[j].Name](opt.Datasets[j], instances[i])
			if err != nil {
				return err
			}
		}
	}

	collector := NewEstResultCollector(len(instances), len(opt.Datasets), len(opt.QueryTypes))
	var wg sync.WaitGroup
	insErrs := make([]error, len(instances))
	for insIdx := range instances {
		wg.Add(1)
		go func(insIdx int) {
			defer wg.Done()
			ins := instances[insIdx]
			for dsIdx := range opt.Datasets {
				ds := datasets[insIdx][dsIdx]
				for qtIdx, qt := range opt.QueryTypes {
					qs, err := ds.GenCases(opt.N, qt)
					if err != nil {
						insErrs[insIdx] = err
						return
					}
					for _, q := range qs {
						estResult, err := runOneEstCase(ins, q)
						if err != nil {
							insErrs[insIdx] = err
							return
						}
						collector.AddEstResult(insIdx, dsIdx, qtIdx, estResult)
					}
				}
			}
		}(insIdx)
	}
	wg.Wait()

	for _, err := range insErrs {
		if err != nil {
			return err
		}
	}

	return GenPErrorBarChartsReport(opt, collector)
}

func runOneEstCase(ins tidb.Instance, query string) (r EstResult, re error) {
	rows, err := ins.Query("EXPLAIN ANALYZE " + query)
	if err != nil {
		return EstResult{}, errors.Trace(err)
	}
	defer func() {
		if err := rows.Close(); err != nil && re == nil {
			re = err
		}
	}()

	types, err := rows.ColumnTypes()
	if err != nil {
		return EstResult{}, err
	}
	nCols := len(types)
	results := make([][]string, 0, 8)
	for rows.Next() {
		cols := make([]string, nCols)
		ptrs := make([]interface{}, nCols)
		for i := 0; i < nCols; i++ {
			ptrs[i] = &cols[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return EstResult{}, err
		}
		results = append(results, cols)
	}

	return ExtractEstResult(results, ins.Version())
}
