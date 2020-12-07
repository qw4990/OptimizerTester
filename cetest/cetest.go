package cetest

import (
	"database/sql"
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

type EstResult struct {
	Query    string  // the original query used to test
	EstCard  float64 // estimated cardinality
	TrueCard float64 // true cardinality
}

type EstResults map[string]map[string][]EstResult // dataset -> query-type -> results

func RunCETests(tidbOpt tidb.Option, datasetNames, queryTypes []string, reportDir string) error {
	ins, err := tidb.ConnectTo(tidbOpt)
	if err != nil {
		return errors.Trace(err)
	}

	// TODO: check datasetNames and queryTypes
	results := make(EstResults)
	for _, dsName := range datasetNames {
		// TODO: parallelize this loop
		ds := datasetMap[dsName]
		if _, ok := results[dsName]; !ok {
			results[dsName] = make(map[string][]EstResult)
		}
		for _, qt := range queryTypes {
			qs := ds.GenCases(QueryType(qt))
			for _, q := range qs {
				estResult, err := runOneEstCase(ins, q)
				if err != nil {
					return err
				}
				results[dsName][qt] = append(results[dsName][qt], estResult)
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
