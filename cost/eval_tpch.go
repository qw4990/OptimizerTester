package cost

import (
	"fmt"

	"github.com/qw4990/OptimizerTester/tidb"
)

func genTPCHEvaluationQueries(ins tidb.Instance, db string) Queries {
	ins.MustExec(`use ` + db)
	qs := make(Queries, 0, 100)
	n := 30
	qs = append(qs, genTPCHEvaluationScanQueries(ins, n)...)
	qs = append(qs, genTPCHEvaluationLookupQueries(ins, n)...)
	qs = append(qs, genTPCHEvaluationAggQueries(ins, n)...)
	qs = append(qs, genTPCHEvaluationSortQueries(ins, n)...)
	return qs
}

func genTPCHEvaluationSortQueries(ins tidb.Instance, n int) (qs Queries) {
	var minV, maxV int

	//SELECT /*+ use_index(orders, primary), must_reorder() */ O_ORDERKEY FROM orders WHERE O_ORDERKEY>=? AND O_ORDERKEY<=? ORDER BY O_ORDERKEY; -- index(PK) scan
	mustReadOneLine(ins, `select min(O_ORDERKEY), max(O_ORDERKEY) from orders`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(orders, primary), must_reorder() */ O_ORDERKEY FROM orders WHERE O_ORDERKEY>=%v AND O_ORDERKEY<=%v ORDER BY O_ORDERKEY`, l, r),
			Label: "Sort",
		})
	}

	//SELECT /*+ use_index(customer, primary), must_reorder() */ C_CUSTKEY FROM customer WHERE C_CUSTKEY>=? AND C_CUSTKEY<=? ORDER BY C_CUSTKEY; -- table scan
	mustReadOneLine(ins, `select min(C_CUSTKEY), max(C_CUSTKEY) from customer`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(customer, primary), must_reorder() */ C_CUSTKEY FROM customer WHERE C_CUSTKEY>=%v AND C_CUSTKEY<=%v ORDER BY C_CUSTKEY`, l, r),
			Label: "Sort",
		})
	}
	return
}

func genTPCHEvaluationAggQueries(ins tidb.Instance, n int) (qs Queries) {
	var minV, maxV int

	//SELECT /*+ use_index(lineitem, primary), stream_agg(), agg_to_cop() */ COUNT(*) FROM lineitem WHERE L_ORDERKEY>=? AND L_ORDERKEY<=?; -- pushed-down agg
	mustReadOneLine(ins, `select min(L_ORDERKEY), max(L_ORDERKEY) from lineitem`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(lineitem, primary), stream_agg(), agg_to_cop() */ COUNT(*) FROM lineitem WHERE L_ORDERKEY>=%v AND L_ORDERKEY<=%v`, l, r),
			Label: "Aggregation",
		})
	}

	//SELECT /*+ use_index(lineitem, primary), stream_agg(), agg_not_to_cop() */ COUNT(*) FROM lineitem WHERE L_ORDERKEY>=? AND L_ORDERKEY<=?; -- not-pushed-down agg
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(lineitem, primary), stream_agg(), agg_not_to_cop() */ COUNT(*) FROM lineitem WHERE L_ORDERKEY>=%v AND L_ORDERKEY<=%v`, l, r),
			Label: "Aggregation",
		})
	}

	return
}

func genTPCHEvaluationLookupQueries(ins tidb.Instance, n int) (qs Queries) {
	var minV, maxV int

	//SELECT /*+ use_index(orders, O_CUSTKEY) */ * FROM orders WHERE O_CUSTKEY>=? AND O_CUSTKEY<=?; -- index lookup
	mustReadOneLine(ins, `select min(O_CUSTKEY), max(O_CUSTKEY) from orders`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		r = l + (r-l)/20
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(orders, O_CUSTKEY) */ * FROM orders WHERE O_CUSTKEY>=%v AND O_CUSTKEY<=%v`, l, r),
			Label: "IndexLookup",
		})
	}

	//SELECT /*+ use_index(lineitem, L_SUPPKEY) */ * FROM lineitem WHERE L_SUPPKEY>=? AND L_SUPPKEY<=?; -- index lookup
	mustReadOneLine(ins, `select min(L_SUPPKEY), max(L_SUPPKEY) from lineitem`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		r = l + (r-l)/80
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(lineitem, L_SUPPKEY) */ * FROM lineitem WHERE L_SUPPKEY>=%v AND L_SUPPKEY<=%v`, l, r),
			Label: "IndexLookup",
		})
	}

	return
}

func genTPCHEvaluationScanQueries(ins tidb.Instance, n int) (qs Queries) {
	var minV, maxV int

	//SELECT /*+ use_index(customer, primary) */ * FROM customer WHERE C_CUSTKEY>=? AND C_CUSTKEY<=?; -- table scan
	mustReadOneLine(ins, `select min(C_CUSTKEY), max(C_CUSTKEY) from customer`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(customer, primary) */ * FROM customer WHERE C_CUSTKEY>=%v AND C_CUSTKEY<=%v`, l, r),
			Label: "TableScan",
		})
	}

	//SELECT /*+ use_index(lineitem, primary) */ L_LINENUMBER FROM lineitem WHERE L_ORDERKEY>=? AND L_ORDERKEY<=?; -- index(PK) scan
	mustReadOneLine(ins, `select min(L_ORDERKEY), max(L_ORDERKEY) from lineitem`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(lineitem, primary) */ L_ORDERKEY FROM lineitem WHERE L_ORDERKEY>=%v AND L_ORDERKEY<=%v`, l, r),
			Label: "IndexScan",
		})
	}

	//SELECT /*+ use_index(orders, primary) */ O_ORDERKEY FROM orders WHERE O_ORDERKEY>=? AND O_ORDERKEY<=?; -- index(PK) scan
	mustReadOneLine(ins, `select min(O_ORDERKEY), max(O_ORDERKEY) from orders`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(orders, primary) */ O_ORDERKEY FROM orders WHERE O_ORDERKEY>=%v AND O_ORDERKEY<=%v`, l, r),
			Label: "TableScan",
		})
	}

	//SELECT /*+ use_index(orders, primary), no_reorder() */ O_ORDERKEY FROM orders WHERE O_ORDERKEY>=? AND O_ORDERKEY<=? ORDER BY O_ORDERKEY DESC; -- index(PK) desc scan
	mustReadOneLine(ins, `select min(O_ORDERKEY), max(O_ORDERKEY) from orders`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(orders, primary), no_reorder() */ O_ORDERKEY FROM orders WHERE O_ORDERKEY>=%v AND O_ORDERKEY<=%v ORDER BY O_ORDERKEY DESC`, l, r),
			Label: "DescIndexScan",
		})
	}

	//SELECT /*+ use_index(customer, primary), no_reorder() */ * FROM customer WHERE C_CUSTKEY>=? AND C_CUSTKEY<=? ORDER BY C_CUSTKEY DESC; -- table scan
	mustReadOneLine(ins, `select min(C_CUSTKEY), max(C_CUSTKEY) from customer`, &minV, &maxV)
	for i := 0; i < n; i++ {
		l, r := randRange(minV, maxV, i, n)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`SELECT /*+ use_index(customer, primary), no_reorder() */ * FROM customer WHERE C_CUSTKEY>=%v AND C_CUSTKEY<=%v ORDER BY C_CUSTKEY DESC`, l, r),
			Label: "DescTableScan",
		})
	}

	return qs
}
