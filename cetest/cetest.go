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
	Name  string   `toml:"name"`
	DB    string   `toml:"db"`
	Label string   `toml:"label"`
	Args  []string `toml:"args"`
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
	QTSingleColPointQueryOnCol QueryType = iota
	QTSingleColPointQueryOnIndex
	QTSingleColMCVPointOnCol
	QTSingleColMCVPointOnIndex
)

var (
	qtNameMap = map[QueryType]string{
		QTSingleColPointQueryOnCol:   "single-col-point-query-on-col",
		QTSingleColPointQueryOnIndex: "single-col-point-query-on-index",
		QTSingleColMCVPointOnCol:     "single-col-mcv-point-on-col",
		QTSingleColMCVPointOnIndex:   "single-col-mcv-point-on-index",
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

var datasetMap = map[string]func(DatasetOpt) (Dataset, error){ // read-only
	"zipfx": newDatasetZipFX,
	"imdb":  newDatasetIMDB,
	"tpcc":  newDatasetTPCC,
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

	datasets := make([]Dataset, len(opt.Datasets))
	for i := range opt.Datasets {
		var err error
		datasets[i], err = datasetMap[opt.Datasets[i].Name](opt.Datasets[i])
		if err != nil {
			return err
		}
		if err := datasets[i].Init(instances, opt.QueryTypes); err != nil {
			return err
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
				ds := datasets[dsIdx]
				for qtIdx, qt := range opt.QueryTypes {
					ers, err := ds.GenEstResults(opt.N, ins, qt)
					if err != nil {
						insErrs[insIdx] = err
						return
					}
					collector.AppendEstResults(insIdx, dsIdx, qtIdx, ers)
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
