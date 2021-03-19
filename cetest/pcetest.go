package cetest

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

type POption struct {
	ReportDir string    `toml:"report-dir"`
	AnaTables []string  `toml:"analyze-tables"`
	NSamples  int       `toml:"n-samples"`
	QueryType QueryType `toml:"query-type"`
	Dataset   string    `toml:"dataset"`
	DB        string    `toml:"db"`
	Tables    []string  `toml:"tables"`
	Labels    []string  `toml:"labels"`

	Instance tidb.Option `toml:"instances"`
}

func DecodePOption(content string) (POption, error) {
	var opt POption
	if _, err := toml.Decode(content, &opt); err != nil {
		return POption{}, errors.Trace(err)
	}
	return opt, nil
}

func RunCETestPartitionModeWithConfig(confPath string) error {
	confContent, err := ioutil.ReadFile(confPath)
	if err != nil {
		return errors.Trace(err)
	}
	opt, err := DecodePOption(string(confContent))
	if err != nil {
		return err
	}

	ins, err := tidb.ConnectTo(opt.Instance)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		ins.Close()
	}()

	for _, tbl := range opt.AnaTables {
		sql := fmt.Sprintf("ANALYZE TABLE %v", tbl)
		if err := ins.Exec(sql); err != nil {
			panic(fmt.Sprintf("sql=%v, err=%v", sql, err))
		}
	}

	collector := NewPEstResultCollector()

	for tblIdx := range opt.Tables {
		tbl := opt.Tables[tblIdx]
		switch strings.ToLower(opt.Dataset) {
		case "imdb":
			switch opt.QueryType {
			case QTSingleColPointQueryOnCol:
				querier := newSingleColQuerier(opt.DB, []string{tbl}, [][]string{{"phonetic_code"}},
					[][]DATATYPE{{DTString}}, map[QueryType][2]int{
						QTSingleColPointQueryOnCol: {0, 0}, // SELECT * FROM title WHERE phonetic_code=?
					})
				ers, err := querier.Collect(opt.NSamples, QTSingleColPointQueryOnCol, nil, ins, true)
				if err != nil {
					return err
				}
				collector.AppendEstResults(ers)
			default:
				return fmt.Errorf("unsupported query type %v for imdb", opt.QueryType)
			}
		default:
			return fmt.Errorf("unknown dataset: %v", opt.Dataset)
		}
	}

	return nil
}
