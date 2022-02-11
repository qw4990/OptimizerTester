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

func genSyntheticEvaluationQueries(ins tidb.Instance, db string, n int) Queries {
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	qs := make(Queries, 0, 1024)
	qs = append(qs, genSyntheticEvaluationTableScan(ins, n)...)
	qs = append(qs, genSyntheticEvaluationIndexScan(ins, n)...)
	qs = append(qs, genSyntheticEvaluationIndexLookup(ins, n)...)
	qs = append(qs, genSyntheticEvaluationSort(ins, n)...)
	qs = append(qs, genSyntheticEvaluationStreamAgg(ins, n)...)
	qs = append(qs, genSyntheticEvaluationHashAgg(ins, n)...)
	qs = append(qs, genSyntheticEvaluationHashJoin(ins, n)...)
	qs = append(qs, genSyntheticEvaluationStreamJoin(ins, n)...)
	qs = append(qs, genSyntheticEvaluationIndexJoin(ins, n)...)
	return qs
}

func genSyntheticEvaluationIndexLookup(ins tidb.Instance, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)

	// select /*+ use_index(t, b) */ b, d from t where b>=? and b<=?; -- IndexLookup
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t, b) */ b, d from t where b>=%v and b<=%v", l, r),
			Label:  "IndexLookup",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvaluationTableScan(ins tidb.Instance, n int) (qs Queries) {
	var minA, maxA int
	mustReadOneLine(ins, `select min(a), max(a) from t`, &minA, &maxA)

	tid := genTypeID() // TableScan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t, primary) */ a from t where a>=%v and a<=%v", l, r),
			Label:  "TableScan",
			TypeID: tid,
		})
	}

	tid = genTypeID()
	for i := 0; i < n; i++ { // WideTableScan
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t, primary) */ a, c from t where a>=%v and a<=%v", l, r),
			Label:  "TableScan",
			TypeID: tid,
		})
	}

	tid = genTypeID()
	for i := 0; i < n; i++ { // DescTableScan
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf(`select /*+ use_index(t, primary), no_reorder() */ a from t where a>=%v and a<=%v order by a desc`, l, r),
			Label:  "DescTableScan",
			TypeID: tid,
		})
	}
	return qs
}

func genSyntheticEvaluationIndexScan(ins tidb.Instance, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)

	tid := genTypeID()
	for i := 0; i < n; i++ { // IndexScan
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t, b) */ b from t where b>=%v and b<=%v", l, r),
			Label:  "IndexScan",
			TypeID: tid,
		})
	}

	tid = genTypeID()
	for i := 0; i < n; i++ { // WideIndexScan
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t, bc) */ b, c from t where b>=%v and b<=%v", l, r),
			Label:  "IndexScan",
			TypeID: tid,
		})
	}

	tid = genTypeID()
	for i := 0; i < n; i++ { // DescIndexScan
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf(`select /*+ use_index(t, b), no_reorder() */ b from t where b>=%v and b<=%v order by b desc`, l, r),
			Label:  "DescIndexScan",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvaluationSort(ins tidb.Instance, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)

	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf(`select /*+ use_index(t, b), must_reorder() */ b from t where b>=%v and b<=%v order by b`, l, r),
			Label:  "Sort",
			TypeID: tid,
		})
	}
	return qs
}

func genSyntheticEvaluationStreamAgg(ins tidb.Instance, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)

	tid := genTypeID() // pushed down
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf(`select /*+ use_index(t, b), stream_agg(), agg_to_cop() */ count(1) from t where b>=%v and b<=%v`, l, r),
			Label:  "StreamAgg",
			TypeID: tid,
		})
	}

	tid = genTypeID() // not pushed down
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf(`select /*+ use_index(t, b), stream_agg(), agg_not_to_cop() */ count(1) from t where b>=%v and b<=%v`, l, r),
			Label:  "StreamAgg",
			TypeID: tid,
		})
	}
	return qs
}

func genSyntheticEvaluationHashAgg(ins tidb.Instance, n int) (qs Queries) {
	return
}

func genSyntheticEvaluationHashJoin(ins tidb.Instance, n int) (qs Queries) {
	return
}

func genSyntheticEvaluationStreamJoin(ins tidb.Instance, n int) (qs Queries) {
	return
}

func genSyntheticEvaluationIndexJoin(ins tidb.Instance, n int) (qs Queries) {
	return
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
