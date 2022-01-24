package cost

import (
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
)

func genIMDBEvaluationQueries(ins tidb.Instance, db string, n int) (qs Queries) {
	ins.MustExec("use " + db)
	qs = append(qs, genIMDBEvaluationScanQueries(ins, n)...)
	qs = append(qs, genIMDBEvaluationDescScanQueries(ins, n)...)
	qs = append(qs, genIMDBEvaluationLookupQueries(ins, n)...)
	qs = append(qs, genIMDBEvaluationAggQueries(ins, n)...)
	qs = append(qs, genIMDBEvaluationSortQueries(ins, n)...)
	return
}

func genIMDBEvaluationScanQueries(ins tidb.Instance, n int) (qs Queries) {
	var minID, maxID, minMID, maxMID int
	mustReadOneLine(ins, `select min(id), max(id), min(movie_id), max(movie_id) from cast_info`, &minID, &maxID, &minMID, &maxMID)

	//SELECT /*+ use_index(cast_info, primary) */ * FROM cast_info WHERE id>=? AND id<=?; -- table scan
	for i := 0; i < n; i++ {
		l, r := randRange(minID, maxID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(cast_info, primary) */ * FROM cast_info WHERE id>=%v AND id<=%v", l, r),
			Label: "TableScan",
		})
	}

	//SELECT /*+ use_index(cast_info, movie_id_cast_info) */ movie_id FROM cast_info WHERE movie_id>=? AND movie_id<=?; -- index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minMID, maxMID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(cast_info, movie_id_cast_info) */ movie_id FROM cast_info WHERE movie_id>=%v AND movie_id<=%v", l, r),
			Label: "IndexScan",
		})
	}
	return
}

func genIMDBEvaluationDescScanQueries(ins tidb.Instance, n int) (qs Queries) {
	var minID, maxID, minMID, maxMID int
	mustReadOneLine(ins, `select min(id), max(id), min(movie_id), max(movie_id) from cast_info`, &minID, &maxID, &minMID, &maxMID)

	//SELECT /*+ use_index(cast_info, primary) */ * FROM cast_info WHERE id>=? AND id<=? ORDER BY id DESC; -- table scan
	for i := 0; i < n; i++ {
		l, r := randRange(minID, maxID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(cast_info, primary) */ * FROM cast_info WHERE id>=%v AND id<=%v ORDER BY id DESC", l, r),
			Label: "DescTableScan",
		})
	}

	//SELECT /*+ use_index(cast_info, movie_id_cast_info) */ movie_id FROM cast_info WHERE movie_id>=? AND movie_id<=? ORDER BY movie_id DESC; -- index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minMID, maxMID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(cast_info, movie_id_cast_info) */ movie_id FROM cast_info WHERE movie_id>=%v AND movie_id<=%v ORDER BY movie_id DESC", l, r),
			Label: "DescIndexScan",
		})
	}
	return
}

func genIMDBEvaluationLookupQueries(ins tidb.Instance, n int) (qs Queries) {
	var minMID, maxMID int
	mustReadOneLine(ins, `select min(movie_id), max(movie_id) from movie_companies`, &minMID, &maxMID)

	//SELECT /*+ use_index(movie_companies, movie_id_movie_companies) */ * FROM movie_companies WHERE movie_id>=? AND movie_id<=?; -- lookup
	for i := 0; i < n; i++ {
		l, r := randRange(minMID, maxMID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(movie_companies, movie_id_movie_companies) */ * FROM movie_companies WHERE movie_id>=%v AND movie_id<=%v", l, r),
			Label: "IndexLookup",
		})

	}
	return
}

func genIMDBEvaluationAggQueries(ins tidb.Instance, n int) (qs Queries) {
	var minCID, maxCID int
	mustReadOneLine(ins, `select min(company_id), max(company_id) from movie_companies`, &minCID, &maxCID)

	//SELECT /*+ use_index(movie_companies, company_id_movie_companies), stream_agg(), agg_to_cop() */ COUNT(*) FROM movie_companies WHERE company_id>=? AND company_id<=?;
	for i := 0; i < n; i++ {
		l, r := randRange(minCID, maxCID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(movie_companies, company_id_movie_companies), stream_agg(), agg_to_cop() */ COUNT(*) FROM movie_companies WHERE company_id>=%v AND company_id<=%v", l, r),
			Label: "Agg",
		})
	}

	//SELECT /*+ use_index(movie_companies, company_id_movie_companies), stream_agg(), agg_not_to_cop() */ COUNT(*) FROM movie_companies WHERE company_id>=? AND company_id<=?;
	for i := 0; i < n; i++ {
		l, r := randRange(minCID, maxCID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(movie_companies, company_id_movie_companies), stream_agg(), agg_not_to_cop() */ COUNT(*) FROM movie_companies WHERE company_id>=%v AND company_id<=%v", l, r),
			Label: "Agg",
		})
	}
	return
}

func genIMDBEvaluationSortQueries(ins tidb.Instance, n int) (qs Queries) {
	var minMID, maxMID, minCID, maxCID int
	mustReadOneLine(ins, `select min(movie_id), max(movie_id), min(company_id), max(company_id) from movie_companies`, &minMID, &maxMID, &minCID, &maxCID)

	//SELECT /*+ use_index(movie_companies, movie_id_movie_companies), must_reorder() */ movie_id FROM movie_companies WHERE movie_id>=? AND movie_id<=? ORDER BY movie_id; -- sort
	for i := 0; i < n; i++ {
		l, r := randRange(minMID, maxMID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(movie_companies, movie_id_movie_companies), must_reorder() */ movie_id FROM movie_companies WHERE movie_id>=%v AND movie_id<=%v ORDER BY movie_id", l, r),
			Label: "Sort",
		})
	}

	//SELECT /*+ use_index(movie_companies, company_id_movie_companies), must_reorder() */ company_id FROM movie_companies WHERE company_id>=? AND company_id<=? ORDER BY company_id; -- sort
	for i := 0; i < n; i++ {
		l, r := randRange(minCID, maxCID, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf("SELECT /*+ use_index(movie_companies, company_id_movie_companies), must_reorder() */ company_id FROM movie_companies WHERE company_id>=%v AND company_id<=%v ORDER BY company_id", l, r),
			Label: "Sort",
		})
	}
	return
}
