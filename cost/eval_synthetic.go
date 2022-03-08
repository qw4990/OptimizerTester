package cost

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
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

var syntheticExecTimeRatio = map[string]float64{
	// for 2000000 rows
	"TableScan":   1,  // 1.2s
	"StreamAgg":   1,  // 1.2s
	"HashAgg":     1,  // 1.2s
	"TiFlashScan": 4,  // 250ms
	"TiFlashAgg":  40, // 25ms
	"MPPScan":     10, // 100ms
	"MPPTiDBAgg":  4,  // 250ms
}

func getSyntheticScale(queryType string) float64 {
	scale := 0.05
	scale *= syntheticExecTimeRatio[queryType]
	if scale > 1 {
		scale = 1
	}
	return scale
}

func genSyntheticEvalQueries(ins tidb.Instance, db string, n int) Queries {
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	qs := make(Queries, 0, 1024)

	// TiKV Plans
	qs = append(qs, genSyntheticEvalTableScan(ins, getSyntheticScale("TableScan"), n)...)
	//qs = append(qs, genSyntheticEvalDescTableScan(ins, 0.75, n)...)
	//qs = append(qs, genSyntheticEvalWideTableScan(ins, 0.3, n)...)
	//qs = append(qs, genSyntheticEvalIndexScan(ins, n)...)
	//qs = append(qs, genSyntheticEvalDescIndexScan(ins, 0.75, n)...)
	//qs = append(qs, genSyntheticEvalWideIndexScan(ins, 0.3, n)...)
	//qs = append(qs, genSyntheticEvalSort(ins, 0.5, n)...)
	qs = append(qs, genSyntheticEvalStreamAgg(ins, getSyntheticScale("StreamAgg"), n)...)
	qs = append(qs, genSyntheticEvalHashAgg(ins, getSyntheticScale("HashAgg"), n)...)
	//qs = append(qs, genSyntheticEvalHashJoin(ins, 0.2, n)...)
	//qs = append(qs, genSyntheticEvalMergeJoin(ins, 0.2, n)...)
	//qs = append(qs, genSyntheticEvalIndexLookup(ins, n)...)
	//qs = append(qs, genSyntheticEvalIndexJoin(ins, n)...)

	// TiFlash & MPP Plans
	qs = append(qs, genSyntheticEvalTiFlashScan(ins, getSyntheticScale("TiFlashScan"), n)...)
	qs = append(qs, genSyntheticEvalTiFlashAgg(ins, getSyntheticScale("TiFlashAgg"), n)...)
	//qs = append(qs, genSyntheticEvalMPPScan(ins, getSyntheticScale("MPPScan"), n)...)
	//qs = append(qs, genSyntheticEvalMPPTiDBAgg(ins, getSyntheticScale("MPPTiDBAgg"), n)...)
	//qs = append(qs, genSyntheticEvalMPP2PhaseAgg(ins, 0.75, n)...)
	//qs = append(qs, genSyntheticEvalMPPHJ(ins, n)...)
	//qs = append(qs, genSyntheticEvalMPPBCJ(ins, n)...)
	return qs
}

func genSyntheticEvalIndexLookup(ins tidb.Instance, n int) (qs Queries) {
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

func genSyntheticEvalTableScan(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minA, maxA int
	mustReadOneLine(ins, `select min(a), max(a) from t`, &minA, &maxA)
	maxA = int(float64(maxA) * scale)
	tid := genTypeID() // TableScan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t, primary) */ a from t where a>=%v and a<=%v", l, r),
			Label:  "TableScan",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvalDescTableScan(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minA, maxA int
	mustReadOneLine(ins, `select min(a), max(a) from t`, &minA, &maxA)
	maxA = int(float64(maxA) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ { // DescTableScan
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf(`select /*+ use_index(t, primary), no_reorder() */ a from t where a>=%v and a<=%v order by a desc`, l, r),
			Label:  "DescTableScan",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvalWideTableScan(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minA, maxA int
	mustReadOneLine(ins, `select min(a), max(a) from t`, &minA, &maxA)
	maxA = int(float64(maxA) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ { // WideTableScan
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t, primary) */ a, c from t where a>=%v and a<=%v", l, r),
			Label:  "WideTableScan",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvalIndexScan(ins tidb.Instance, n int) (qs Queries) {
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
	return
}

func genSyntheticEvalDescIndexScan(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
	tid := genTypeID()
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

func genSyntheticEvalWideIndexScan(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ { // WideIndexScan
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t, bc) */ b, c from t where b>=%v and b<=%v", l, r),
			Label:  "WideIndexScan",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvalSort(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf(`select /*+ use_index(t, b), must_reorder() */ b from t where b>=%v and b<=%v order by b`, l, r),
			Label:  "Sort",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvalStreamAgg(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
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
	return
}

func genSyntheticEvalHashAgg(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf(`select /*+ use_index(t, b), hash_agg() */ count(1) from t where b>=%v and b<=%v`, l, r),
			Label:  "HashAgg",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvalHashJoin(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l1, r1 := randRange(minB, maxB, i, n)
		l2, r2 := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t1, b), use_index(t2, b), tidb_hj(t1, t2) */ t1.b, t2.b from t t1, t t2 where t1.b=t2.b and t1.b>=%v and t1.b<=%v and t2.b>=%v and t2.b<=%v", l1, r1, l2, r2),
			Label:  "HashJoin",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvalMergeJoin(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l1, r1 := randRange(minB, maxB, i, n)
		l2, r2 := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			SQL:    fmt.Sprintf("select /*+ use_index(t1, b), use_index(t2, b), tidb_smj(t1, t2) */ t1.b, t2.b from t t1, t t2 where t1.b=t2.b and t1.b>=%v and t1.b<=%v and t2.b>=%v and t2.b<=%v", l1, r1, l2, r2),
			Label:  "MergeJoin",
			TypeID: tid,
		})
	}
	return
}

func genSyntheticEvalTiFlashScan(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minA, maxA int
	mustReadOneLine(ins, `select min(a), max(a) from t`, &minA, &maxA)
	maxA = int(float64(maxA) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			PreSQLs: []string{`set @@session.tidb_allow_mpp=0`, "set @@session.tidb_enforce_mpp=0"},
			SQL:     fmt.Sprintf(`SELECT /*+ read_from_storage(tiflash[t]) */ a FROM t WHERE a>=%v AND a<=%v`, l, r),
			Label:   "TiFlashScan",
			TypeID:  tid,
		})
	}
	return
}

func genSyntheticEvalTiFlashAgg(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minA, maxA int
	mustReadOneLine(ins, `select min(a), max(a) from t`, &minA, &maxA)
	maxA = int(float64(maxA) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			PreSQLs: []string{`set @@session.tidb_allow_batch_cop=0`, `set @@session.tidb_allow_mpp=0`, "set @@session.tidb_enforce_mpp=0"},
			SQL:     fmt.Sprintf(`SELECT /*+ read_from_storage(tiflash[t]) */ count(*) FROM t WHERE a>=%v AND a<=%v`, l, r),
			Label:   `TiFlashAgg`,
			TypeID:  tid,
		})
	}
	return
}

func genSyntheticEvalMPPScan(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minA, maxA int
	mustReadOneLine(ins, `select min(a), max(a) from t`, &minA, &maxA)
	maxA = int(float64(maxA) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		qs = append(qs, Query{
			PreSQLs: []string{`set @@session.tidb_allow_mpp=1`, "set @@session.tidb_enforce_mpp=1"}, // use MPPScan
			SQL:     fmt.Sprintf(`SELECT /*+ read_from_storage(tiflash[t]) */ a FROM t WHERE a>=%v AND a<=%v`, l, r),
			Label:   "MPPScan",
			TypeID:  tid,
		})
	}
	return
}

func genSyntheticEvalMPPTiDBAgg(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			PreSQLs: []string{`set @@session.tidb_allow_mpp=1`, `set @@session.tidb_enforce_mpp=1`},
			SQL:     fmt.Sprintf(`SELECT /*+ read_from_storage(tiflash[t]) */ COUNT(*) FROM t WHERE b>=%v and b<=%v`, l, r),
			Label:   "MPPTiDBAgg",
			TypeID:  tid,
		})
	}
	return
}

func genSyntheticEvalMPP2PhaseAgg(ins tidb.Instance, scale float64, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	maxB = int(float64(maxB) * scale)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			PreSQLs: []string{`set @@session.tidb_allow_mpp=1`, `set @@session.tidb_enforce_mpp=1`},
			SQL:     fmt.Sprintf(`SELECT /*+ read_from_storage(tiflash[t]) */ COUNT(*), b FROM t WHERE b>=%v and b<=%v GROUP BY b`, l, r),
			Label:   "MPP2PhaseAgg",
			TypeID:  tid,
		})
	}
	return
}

func genSyntheticEvalMPPHJ(ins tidb.Instance, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l1, r1 := randRange(minB, maxB, i, n)
		l2, r2 := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			PreSQLs: []string{`set @@session.tidb_enforce_mpp=1`, `set @@session.tidb_opt_broadcast_join=0`},
			SQL:     fmt.Sprintf(`SELECT /*+ read_from_storage(tiflash[t1, t2]) */ t1.b, t2.b FROM t t1, t t2 WHERE t1.b=t2.b and t1.b>=%v and t1.b<=%v and t2.b>=%v and t2.b<=%v`, l1, r1, l2, r2),
			Label:   "MPPHJ",
			TypeID:  tid,
		})
	}
	return
}

func genSyntheticEvalMPPBCJ(ins tidb.Instance, n int) (qs Queries) {
	var minB, maxB int
	mustReadOneLine(ins, `select min(b), max(b) from t`, &minB, &maxB)
	tid := genTypeID()
	for i := 0; i < n; i++ {
		l1, r1 := randRange(minB, maxB, i, n)
		l2, r2 := randRange(minB, maxB, i, n)
		qs = append(qs, Query{
			PreSQLs: []string{`set @@session.tidb_enforce_mpp=0`, `set @@session.tidb_opt_broadcast_join=1`},
			SQL:     fmt.Sprintf(`SELECT /*+ broadcast_join(t1, t2), read_from_storage(tiflash[t1, t2]) */ t1.b, t2.b FROM t t1, t t2 WHERE t1.b=t2.b and t1.b>=%v and t1.b<=%v and t2.b>=%v and t2.b<=%v`, l1, r1, l2, r2),
			Label:   "MPPBCJ",
			TypeID:  tid,
		})
	}
	return
}

func genSyntheticEvalIndexJoin(ins tidb.Instance, n int) (qs Queries) {
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
