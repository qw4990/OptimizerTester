package cost

import (
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
	"math/rand"
	"strings"
	"time"
)

//create table t (
//	a int, 
//	b int,
//	c varchar(128), -- always space(128)
//	d int,
//	primary key(a),
//	key b(b),
//	key bc(b, c)
//);

// select /*+ use_index(t, primary) */ a from t where a>=? and a<=?;						-- TableScan
// select /*+ use_index(t, primary) */ a, c from t where a>=? and a<=?;						-- TableScan + WideCol
// select /*+ use_index(t, b) */ b from t where b>=? and b<=?;								-- IndexScan
// select /*+ use_index(t, bc) */ b, c from t where b>=? and b<=?;							-- IndexScan + WideCol
// select /*+ use_index(t, b) */ b, d from t where b>=? and b<=?;							-- IndexLookup

// DescScan: descScanFactor, netFactor
//   select /*+ use_index(t, primary), no_reorder() */ a from t where a>=? and a<=? order by a desc			(0, 0, estRow*rowSize, 0, estRow*log(rowSize), 0)
//   select /*+ use_index(t, b), no_reorder() */ b from t where b>=? and b<=? order by b desc				(0, 0, estRow*rowSize, 0, estRow*log(rowSize), 0)
// AGG: CPUFactor, copCPUFactor
//   select /*+ use_index(t, b), stream_agg(), agg_to_cop() */ count(1) from t where b>=? and b<=?			(0, estRow, 0, estRow*log(rowSize), 0, 0)
//   select /*+ use_index(t, b), stream_agg(), agg_not_to_cop() */ count(1) from t where b>=? and b<=?		(estRow, 0, estRow*rowSize, estRow*log(rowSize), 0, 0)
// Sort: CPUFactor, MemFactor
//   select /*+ use_index(t, b), must_reorder() */ b from t where b>=? and b<=? order by b					(estRow*log(estRow), 0, estRow*rowSize, estRow*log(rowSize), 0, estRow)

func genSyntheticQueries(ins tidb.Instance, db string) Queries {
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	var n int
	mustReadOneLine(ins, `select max(a) from t`, &n)

	repeat := 5
	qs := make(Queries, 0, 1024)
	qs = append(qs, genSyntheticQuery(n, repeat, "TableScan", db, "a", "primary", "", "a")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "Wide-TableScan", db, "a, c", "primary", "", "a")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexScan", db, "b", "b", "", "b")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "Wide-IndexScan", db, "b, c", "bc", "", "b")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexLookup", db, "b, d", "b", "", "b")...)
	qs = append(qs, genSyntheticDescScanQuries(n, repeat)...)
	qs = append(qs, genSyntheticSortQueries(n, repeat)...)
	qs = append(qs, genSyntheticAggQueries(n, repeat)...)
	return qs
}

func genSyntheticQuery(n, repeat int, label, db, sel, idxhint, orderby string, cols ...string) Queries {
	qs := make(Queries, 0, repeat)
	if orderby != "" {
		orderby = "order by " + orderby
	}

	for i := 0; i < repeat; i++ {
		var conds []string
		for k, col := range cols {
			if k < len(cols)-1 {
				conds = append(conds, fmt.Sprintf("%v=%v", col, rand.Intn(n)))
			} else {
				l, r := randRange(0, n, i, repeat)
				conds = append(conds, fmt.Sprintf("%v>=%v and %v<=%v", col, l, col, r))
			}
		}
		qs = append(qs, Query{Label: label, SQL: fmt.Sprintf(`select /*+ use_index(t, %v) */ %v from %v.t where %v %v`, idxhint, sel, db, strings.Join(conds, " and "), orderby)})
	}
	return qs
}

func genSyntheticDescScanQuries(n, repeat int) Queries {
	qs := make(Queries, 0, repeat)
	for i := 0; i < repeat; i++ {
		l, r := randRange(0, n, i, repeat)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`select /*+ use_index(t, primary), no_reorder() */ a from t where a>=%v and a<=%v order by a desc`, l, r),
			Label: "DescTableScan",
		})
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`select /*+ use_index(t, b), no_reorder() */ b from t where b>=%v and b<=%v order by b desc`, l, r),
			Label: "DescIndexScan",
		})
	}
	return qs
}

func genSyntheticSortQueries(n, repeat int) Queries {
	qs := make(Queries, 0, repeat)
	for i := 0; i < repeat; i++ {
		l, r := randRange(0, n, i, repeat)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`select /*+ use_index(t, b), must_reorder() */ b from t where b>=%v and b<=%v order by b`, l, r),
			Label: "Sort",
		})
	}
	return qs
}

func genSyntheticAggQueries(n, repeat int) Queries {
	qs := make(Queries, 0, repeat)
	for i := 0; i < repeat; i++ {
		l, r := randRange(0, n, i, repeat)
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`select /*+ use_index(t, b), stream_agg(), agg_to_cop() */ count(1) from t where b>=%v and b<=%v`, l, r),
			Label: "AggPushedDown",
		})
		qs = append(qs, Query{
			SQL:   fmt.Sprintf(`select /*+ use_index(t, b), stream_agg(), agg_not_to_cop() */ count(1) from t where b>=%v and b<=%v`, l, r),
			Label: "AggNotPushedDown",
		})
	}
	return qs
}

func genSyntheticData(ins tidb.Instance, n int, db string) {
	ins.MustExec(fmt.Sprintf(`create database if not exists %v`, db))
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	ins.MustExec(`create table if not exists t (
		a int, 
		b int,
		c varchar(128), -- always space(128)
		d int,
		primary key(a),
		key b(b),
		key bc(b, c))`)

	beginAt := time.Now()
	batch := 500
	rows := make([]string, 0, batch)
	for i := 0; i < n; i += batch {
		l := i
		r := i + batch
		if r > n {
			r = n
		}
		fmt.Printf("[cost-eval] gen synthetic data %v-%v, duration from the beginning %v\n", l, r, time.Since(beginAt))
		rows = rows[:0]
		for k := l; k < r; k++ {
			rows = append(rows, fmt.Sprintf("(%v, %v, %v, %v)", k, rand.Intn(n), "space(128)", rand.Intn(n)))
		}
		ins.MustExec(fmt.Sprintf(`insert into t values %v`, strings.Join(rows, ", ")))
	}
}
