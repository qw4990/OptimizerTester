package cetest

import (
	"fmt"
	"io/ioutil"
	"math"
	"sort"
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

	Instance tidb.Option `toml:"instance"`
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

	// enable dynamic pruning
	if err := ins.ExecInNewSession("SET GLOBAL tidb_analyze_version=2"); err != nil {
		return err
	}
	if err := ins.ExecInNewSession("SET GLOBAL tidb_partition_prune_mode='dynamic'"); err != nil {
		return err
	}
	for _, tbl := range opt.AnaTables {
		fmt.Printf("start analyzing table %v...\n", tbl)
		sql := fmt.Sprintf("ANALYZE TABLE %v.`%v`", opt.DB, tbl)
		if err := ins.Exec(sql); err != nil {
			panic(fmt.Sprintf("sql=%v, err=%v", sql, err))
		}
	}

	collector := NewPEstResultCollector()
	switch strings.ToLower(opt.Dataset) {
	case "imdb":
		switch opt.QueryType {
		case QTSingleColPointQueryOnCol:
			for _, tbl := range opt.Tables {
				querier := newSingleColQuerier(opt.DB, []string{tbl}, [][]string{{"phonetic_code"}},
					[][]DATATYPE{{DTString}}, map[QueryType][2]int{
						QTSingleColPointQueryOnCol: {0, 0}, // SELECT * FROM title WHERE phonetic_code=?
					})
				ers, err := querier.Collect(opt.NSamples, QTSingleColPointQueryOnCol, nil, ins, true)
				if err != nil {
					return err
				}
				collector.AppendEstResults(ers)
			}
		case QTSingleColPointQueryOnIndex:
			for _, tbl := range opt.Tables {
				querier := newSingleColQuerier(opt.DB, []string{tbl}, [][]string{{"person_id"}},
					[][]DATATYPE{{DTInt}}, map[QueryType][2]int{
						QTSingleColPointQueryOnIndex: {0, 0}, // SELECT * FROM cast_info WHERE person_id=?
					})
				ers, err := querier.Collect(opt.NSamples, QTSingleColPointQueryOnIndex, nil, ins, true)
				if err != nil {
					return err
				}
				collector.AppendEstResults(ers)
			}
		default:
			return fmt.Errorf("unsupported query type %v for imdb", opt.QueryType)
		}
	case "pzipfx":
		switch opt.QueryType {
		case QTSingleColPointQueryOnCol:
			for _, tbl := range opt.Tables {
				querier := newSingleColQuerier(opt.DB, []string{tbl}, [][]string{{"c"}},
					[][]DATATYPE{{DTInt}}, map[QueryType][2]int{
						// select * from p-{part-type}-zint-{part-cols} where c = ?
						QTSingleColPointQueryOnCol: {0, 0},
					})
				ers, err := querier.Collect(opt.NSamples, QTSingleColPointQueryOnCol, nil, ins, true)
				if err != nil {
					return err
				}
				collector.AppendEstResults(ers)
			}
		case QTSingleColMCVPointOnCol:
			for _, tbl := range opt.Tables {
				querier := newSingleColQuerier(opt.DB, []string{tbl}, [][]string{{"c"}},
					[][]DATATYPE{{DTInt}}, map[QueryType][2]int{
						// select * from p-{part-type}-zint-{part-cols} where c = ?
						QTSingleColMCVPointOnCol: {0, 0},
					})
				ers, err := querier.Collect(opt.NSamples, QTSingleColMCVPointOnCol, nil, ins, true)
				if err != nil {
					return err
				}
				collector.AppendEstResults(ers)
			}
		case QTSingleColPointQueryOnIndex:
			for _, tbl := range opt.Tables {
				querier := newSingleColQuerier(opt.DB, []string{tbl}, [][]string{{"a"}},
					[][]DATATYPE{{DTInt}}, map[QueryType][2]int{
						// select * from p-{part-type}-zint-{part-cols} where a = ?
						QTSingleColPointQueryOnIndex: {0, 0},
					})
				ers, err := querier.Collect(opt.NSamples, QTSingleColPointQueryOnIndex, nil, ins, true)
				if err != nil {
					return err
				}
				collector.AppendEstResults(ers)
			}
		case QTSingleColMCVPointOnIndex:
			for _, tbl := range opt.Tables {
				querier := newSingleColQuerier(opt.DB, []string{tbl}, [][]string{{"a"}},
					[][]DATATYPE{{DTInt}}, map[QueryType][2]int{
						// select * from p-{part-type}-zint-{part-cols} where a = ?
						QTSingleColMCVPointOnIndex: {0, 0},
					})
				ers, err := querier.Collect(opt.NSamples, QTSingleColMCVPointOnIndex, nil, ins, true)
				if err != nil {
					return err
				}
				collector.AppendEstResults(ers)
			}
		default:
			return fmt.Errorf("unsupported query type %v for pzipfx", opt.QueryType)
		}
	default:
		return fmt.Errorf("unknown dataset: %v", opt.Dataset)
	}

	if err := PGenPErrorBarChartsReport(opt, collector); err != nil {
		return err
	}

	// print the worst 10 cases
	for idx, label := range opt.Labels {
		rs := collector.EstResults(idx)
		sort.Slice(rs, func(i, j int) bool {
			return math.Abs(PError(rs[i])) > math.Abs(PError(rs[j]))
		})
		for i := 0; i < 10 && i < len(rs); i++ {
			fmt.Printf("[BadCase-%v]: %v, perror=%v\n", label, rs[i].SQL, PError(rs[i]))
		}
	}
	return nil
}
