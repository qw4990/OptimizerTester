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
//	c int,
//	d int,
//	e varchar(128),
//	f int,
//	primary key(a),
//	key b(b),
//	key bcd(b, c, d),
//	key de(d, e)
//);

// select /*+ use_index(t, primary) */ a from t where a>=? and a<=?;						-- TableScan
// select /*+ use_index(t, primary) */ * from t where a>=? and a<=?;						-- TableScan + width row
// select /*+ use_index(t, b) */ b from t where b>=? and b<=?;								-- Single-Col IndexScan
// select /*+ use_index(t, bcd) */ b, c, d from b=? and c>=? and c<=?;						-- Multi-Col IndexScan
// select /*+ use_index(t, bcd) */ b, c, d from b=? and c=? and d>=? and d<=?;				-- Multi-Col IndexScan
// select /*+ use_index(t, b) */ b, f from t where b>=? and b<=?; 							-- Single-Col IndexLookup
// select /*+ use_index(t, bcd) */ b, c, d, f from b=? and c>=? and c<=?;					-- Multi-Col IndexLookup
// select /*+ use_index(t, bcd) */ b, c, d, f from b=? and c=? and d>=? and d<=?;			-- Multi-Col IndexLookup
// select /*+ use_index(t, b) */ * from t where b>=? and b<=?; 								-- Single-Col IndexLookup + width row
// select /*+ use_index(t, bcd) */ * from b=? and c>=? and c<=?;							-- Multi-Col IndexLookup + width row
// select /*+ use_index(t, bcd) */ * from b=? and c=? and d>=? and d<=?;					-- Multi-Col IndexLookup + width row

// select /*+ use_index(t, primary) */ a, b from t where a>=? and a<=? order by b;						-- TableScan + Order
// select /*+ use_index(t, primary) */ * from t where a>=? and a<=? order by b;							-- TableScan + Order + width row
// select /*+ use_index(t, b) */ b, f from t where b>=? and b<=? order by f; 							-- Single-Col IndexLookup + Order
// select /*+ use_index(t, bcd) */ b, c, d, f from b=? and c>=? and c<=? order by f;					-- Multi-Col IndexLookup + Order
// select /*+ use_index(t, bcd) */ b, c, d, f from b=? and c=? and d>=? and d<=? order by f;			-- Multi-Col IndexLookup + Order
// select /*+ use_index(t, b) */ * from t where b>=? and b<=? order by f; 								-- Single-Col IndexLookup + width row + Order
// select /*+ use_index(t, bcd) */ * from b=? and c>=? and c<=? order by f;								-- Multi-Col IndexLookup + width row + Order
// select /*+ use_index(t, bcd) */ * from b=? and c=? and d>=? and d<=? order by f;						-- Multi-Col IndexLookup + width row + Order

func genSyntheticQueries(ins tidb.Instance, db string) []query {
	ins.MustExec(fmt.Sprintf(`use %v`, db))

	rs := ins.MustQuery(`select max(a) from t`)
	var n int
	rs.Next()
	if err := rs.Scan(&n); err != nil {
		panic(err)
	}

	repeat := 50
	qs := make([]query, 0, 1024)
	qs = append(qs, genSyntheticQuery(n, repeat, "TableScan", db, "a", "primary", "", "a")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "TableScan", db, "*", "primary", "", "a")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexScan", db, "b", "b", "", "b")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexScan", db, "b, c, d", "bcd", "", "b", "c")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexScan", db, "b, c, d", "bcd", "", "b", "c", "d")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexLookUp", db, "b, f", "b", "", "b")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexLookUp", db, "b, c, d, f", "bcd", "", "b", "c")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexLookUp", db, "b, c, d, f", "bcd", "", "b", "c", "d")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexLookUp", db, "*", "b", "", "b")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexLookUp", db, "*", "bcd", "", "b", "c")...)
	qs = append(qs, genSyntheticQuery(n, repeat, "IndexLookUp", db, "*", "bcd", "", "b", "c", "d")...)

	repeatOrder := 5
	qs = append(qs, genSyntheticQuery(n, repeatOrder, "Sort", db, "a, f", "primary", "f", "a")...)
	qs = append(qs, genSyntheticQuery(n, repeatOrder, "Sort", db, "*", "primary", "f", "a")...)
	qs = append(qs, genSyntheticQuery(n, repeatOrder, "Sort", db, "b, f", "b", "f", "b")...)
	qs = append(qs, genSyntheticQuery(n, repeatOrder, "Sort", db, "b, c, d, f", "bcd", "f", "b", "c")...)
	qs = append(qs, genSyntheticQuery(n, repeatOrder, "Sort", db, "b, c, d, f", "bcd", "f", "b", "c", "d")...)
	qs = append(qs, genSyntheticQuery(n, repeatOrder, "Sort", db, "*", "b", "f", "b")...)
	qs = append(qs, genSyntheticQuery(n, repeatOrder, "Sort", db, "*", "bcd", "f", "b", "c")...)
	qs = append(qs, genSyntheticQuery(n, repeatOrder, "Sort", db, "*", "bcd", "f", "b", "c", "d")...)
	return qs
}

func genSyntheticQuery(n, repeat int, label, db, sel, idxhint, orderby string, cols ...string) []query {
	qs := make([]query, 0, repeat)
	if orderby != "" {
		orderby = "order by " + orderby
	}

	rangeStep := n / repeat
	for i := 0; i < repeat; i++ {
		var conds []string
		for k, col := range cols {
			if k < len(cols)-1 {
				conds = append(conds, fmt.Sprintf("%v=%v", col, rand.Intn(n)))
			} else {
				gap := rangeStep * (i + 1)
				l := rand.Intn(n - gap + 1)
				r := l + gap + rand.Intn(rangeStep)
				if r > n {
					r = n
				}
				conds = append(conds, fmt.Sprintf("%v>=%v and %v<=%v", col, l, col, r))
			}
		}
		qs = append(qs, query{label: label, sql: fmt.Sprintf(`select /*+ use_index(t, %v) */ %v from %v.t where %v %v`, idxhint, sel, db, strings.Join(conds, " and "), orderby)})
	}
	return qs
}

func genSyntheticData(ins tidb.Instance, n int, db string) {
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	ins.MustExec(`create table if not exists t (
	a int, 
	b int,
	c int,
	d int,
	e varchar(128),
	f int,
	primary key(a),
	key b(b),
	key bcd(b, c, d),
	key de(d, e))`)

	beginAt := time.Now()
	batch := 400
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
			rows = append(rows, fmt.Sprintf("(%v, %v, %v, %v, %v, %v)", k, rand.Intn(n), rand.Intn(n), rand.Intn(n), "space(128)", rand.Intn(n)))
		}
		ins.MustExec(fmt.Sprintf(`insert into t values %v`, strings.Join(rows, ", ")))
	}
}
