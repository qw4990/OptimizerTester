package cost

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

var (
	typeID     int
	typeIDLock sync.Mutex
)

func genTypeID() int {
	typeIDLock.Lock()
	defer typeIDLock.Unlock()
	typeID += 1
	return typeID
}

func mustReadOneLine(ins tidb.Instance, q string, ret ...interface{}) {
	rs := ins.MustQuery(q)
	rs.Next()
	defer rs.Close()
	if err := rs.Scan(ret...); err != nil {
		panic(err)
	}
}

func mustGetRowCount(ins tidb.Instance, q string) int {
	var cnt int
	mustReadOneLine(ins, q, &cnt)
	return cnt
}

func randRange(minVal, maxVal, iter, totalRepeat int) (int, int) {
	step := (maxVal - minVal + 1) / totalRepeat
	l := 1
	r := step * (iter + 1)
	if r > maxVal {
		r = maxVal
	}
	return l, r
}

func saveTo(f string, r interface{}) {
	data, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(f, data, 0666); err != nil {
		panic(err)
	}
}

func readFrom(f string, r interface{}) error {
	data, err := ioutil.ReadFile(f)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, r); err != nil {
		return err
	}
	return nil
}

var costFactorVars = []string{"tidb_opt_cpu_factor",
	"tidb_opt_copcpu_factor", "tidb_opt_network_factor",
	"tidb_opt_scan_factor", "tidb_opt_desc_factor",
	"tidb_opt_memory_factor", "tidb_opt_seek_factor"}

func setCostFactors(ins tidb.Instance, factors CostFactors) {
	fmt.Println("SET COST FACTORS(CPU, CopCPU, Net, Scan, DescScan, Mem, Seek):", factors)
	for i := 0; i < NumFactors; i++ {
		sql := fmt.Sprintf("set @@%v=%v;", costFactorVars[i], factors[i])
		fmt.Println(sql)
		ins.MustExec(sql)
	}
}

func readCostFactors(ins tidb.Instance) (factors CostFactors) {
	// (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)
	for i := 0; i < NumFactors; i++ {
		ret := ins.MustQuery(fmt.Sprintf("select @@%v", costFactorVars[i]))
		ret.Next()
		if err := ret.Scan(&factors[i]); err != nil {
			panic(err)
		}
		if err := ret.Close(); err != nil {
			panic(err)
		}
	}
	fmt.Println("READ COST FACTORS(CPU, CopCPU, Net, Scan, DescScan, Mem, Seek):", factors)
	return
}

func calculateCost(weights CostWeights, factors CostFactors) float64 {
	var cost float64
	for i := range factors {
		cost += weights[i] * factors[i]
	}
	return cost
}

func extractCostTimeFromQuery(ins tidb.Instance, explainAnalyzeQuery string, repeat, timeLimitMS int, checkRowCount bool, planChecker PlanChecker) (rootOperator string, avgPlanCost, avgTimeMS float64, tle bool) {
	var totalPlanCost, totalTimeMS float64
	for i := 0; i < repeat+1; i++ {
		rs := ins.MustQuery(explainAnalyzeQuery)
		explainResult := ParseExplainAnalyzeResultsWithRows(rs)
		fmt.Printf("[cost-eval/cali] iter: %v, cost: %v, timeMS: %v, query: %v\n", i, explainResult.PlanCost, explainResult.TimeMS, explainAnalyzeQuery)
		if i == 0 {
			continue // ignore the first processing
		}
		if checkRowCount && !explainResult.UseTrueCardinality() {
			panic(fmt.Sprintf(`not true-CE for query=%v`, explainAnalyzeQuery))
		}
		if timeLimitMS > 0 && int(explainResult.TimeMS) > timeLimitMS {
			return "", 0, 0, true
		}
		if planChecker != nil {
			reason, ok := planChecker(explainResult.RawPlan)
			if !ok {
				panic(fmt.Sprintf("unexpected plan for query=%v, reason=%v", explainAnalyzeQuery, reason))
			}
		}
		totalPlanCost += explainResult.PlanCost
		totalTimeMS += explainResult.TimeMS
		rootOperator = explainResult.RootOperator
	}
	return rootOperator, totalPlanCost / float64(repeat), totalTimeMS / float64(repeat), false
}

type ExplainAnalyzeResult struct {
	RootOperator    string
	PlanCost        float64
	TimeMS          float64
	OperatorActRows map[string]float64
	OperatorEstRows map[string]float64
	RawPlan         []string
}

func (r ExplainAnalyzeResult) UseTrueCardinality() bool {
	for op, act := range r.OperatorActRows {
		if r.OperatorEstRows[op] != act {
			return false
		}
	}
	return true
}

func ParseExplainAnalyzeResultsWithRows(rs *sql.Rows) *ExplainAnalyzeResult {
	//mysql> explain analyze select /*+ display_cost(), trace_cost(), stream_agg(), agg_to_cop() */ count(1) from t where b>=1 and b<=10;
	//	+-----------------------------+---------+------------------------------------------------------------------------------+---------+-----------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------+---------------------------------+-----------+------+
	//	| id                          | estRows | estCost                                                                      | actRows | task      | access object       | execution info                                                                                                                    | operator info                   | memory    | disk |
	//	+-----------------------------+---------+------------------------------------------------------------------------------+---------+-----------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------+---------------------------------+-----------+------+
	//	| StreamAgg_13                | 1.00    | 35.08:[1 9.744245524296675 8.125 282.5831202046036 0 0 1]:484.2324168797954  | 1       | root      |                     | time:204.5µs, loops:2                                                                                                             | funcs:count(Column#7)->Column#5 | 380 Bytes | N/A  |
	//	| └─IndexReader_14            | 1.00    | 32.08:[0 9.744245524296675 8.125 282.5831202046036 0 0 1]:481.2324168797954  | 1       | root      |                     | time:196µs, loops:2, cop_task: {num: 1, max: 114.7µs, proc_keys: 0, rpc_num: 1, rpc_time: 102.4µs, copr_cache_hit_ratio: 0.00}    | index:StreamAgg_8               | 176 Bytes | N/A  |
	//	|   └─StreamAgg_8             | 1.00    | 473.11:[0 9.744245524296675 8.125 282.5831202046036 0 0 1]:481.2324168797954 | 1       | cop[tikv] |                     | tikv_task:{time:2.57µs, loops:20}                                                                                                 | funcs:count(1)->Column#7        | N/A       | N/A  |
	//	|     └─IndexRangeScan_11     | 9.74    | 443.87:[0 0 0 282.5831202046036 0 0 1]:443.87468030690536                    | 20      | cop[tikv] | table:t, index:b(b) | tikv_task:{time:1.56µs, loops:20}                                                                                                 | range:[1,10], keep order:false  | N/A       | N/A  |
	//	+-----------------------------+---------+------------------------------------------------------------------------------+---------+-----------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------+---------------------------------+-----------+------+
	//mysql> show warnings;
	//+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	//| Level | Code | Message                                                                                                                                                              |
	//	+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	//| Note  | 1105 | [COST] CostWeights: [1 9.744245524296675 8.125 282.5831202046036 0 0 1]                                                                                              |
	//| Note  | 1105 | [COST] CostCalculation: [CPU, CopCPU, Net, Scan, DescScan, Mem, Seek] [1 9.744245524296675 8.125 282.5831202046036 0 0 1]*[3 3 1 1.5 3 0.001 20] = 484.2324168797954 |
	//+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	var id, estRows, actRows, estCost, task, access, execInfo, opInfo, mem, disk string
	r := &ExplainAnalyzeResult{OperatorActRows: make(map[string]float64), OperatorEstRows: make(map[string]float64)}

	for rs.Next() {
		if err := rs.Scan(&id, &estRows, &estCost, &actRows, &task, &access, &execInfo, &opInfo, &mem, &disk); err != nil {
			panic(err)
		}
		operator, id := parseOperatorName(id)
		r.OperatorEstRows[id] = mustStr2Float(estRows)
		r.OperatorActRows[id] = mustStr2Float(actRows)
		if r.RootOperator == "" {
			r.RootOperator = operator
			r.TimeMS = parseTimeFromExecInfo(execInfo)
			tmp := strings.Split(estCost, ":")
			r.PlanCost = mustStr2Float(tmp[0])
		}
		r.RawPlan = append(r.RawPlan, strings.Join([]string{id, estRows, actRows, estCost, task, access, execInfo, opInfo, mem, disk}, "\t"))
	}
	if err := rs.Close(); err != nil {
		panic(err)
	}
	return r
}

func parseTimeFromExecInfo(execInfo string) (timeMS float64) {
	// time:252.5ms, loops:99, index_task: {total_time: 13
	timeField := strings.Split(execInfo, ",")[0]
	timeField = strings.Split(timeField, ":")[1]
	dur, err := time.ParseDuration(timeField)
	if err != nil {
		panic(fmt.Sprintf("invalid time %v", timeField))
	}
	return float64(dur) / float64(time.Millisecond)
}

func mustStr2Float(str string) float64 {
	v, err := strconv.ParseFloat(str, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func parseOperatorName(str string) (name, nameWithID string) {
	//	├─IndexRangeScan_5(Build)
	begin := 0
	for begin < len(str) {
		if str[begin] < 'A' || str[begin] > 'Z' { // not a upper letter
			begin++
		} else {
			break
		}
	}
	end0 := begin
	for end0 < len(str) {
		if str[end0] != '_' {
			end0++
		} else {
			break
		}
	}

	end1 := end0 + 1
	for end1 < len(str) {
		if str[end1] >= '0' && str[end1] <= '9' {
			end1++
		} else {
			break
		}
	}
	return str[begin:end0], str[begin:end1]
}

func injectHint(query, hint string) string {
	hintBegin := strings.Index(query, "/*+ ")
	hintBegin += len("/*+ ")
	return query[:hintBegin] + hint + ", " + query[hintBegin:]
}

func checkTiFlashScan(rawPlan []string) (reason string, ok bool) {
	for _, line := range rawPlan {
		if strings.Contains(line, "Scan") && strings.Contains(line, "tiflash") {
			return "", true
		}
	}
	return fmt.Sprintf("not a TiFlashScan, plan is \n" + strings.Join(rawPlan, "\n")), false
}

func checkMPPTiDBAgg(rawPlan []string) (reason string, ok bool) {
	var tidbAgg, tiflashAgg bool
	for _, line := range rawPlan {
		if strings.Contains(line, "Agg") {
			if strings.Contains(line, "root") {
				tidbAgg = true
			}
			if strings.Contains(line, "tiflash") {
				tiflashAgg = true
			}
		}
	}
	if tidbAgg && tiflashAgg {
		return "", true
	}
	return fmt.Sprintf("not a MPPTiDBAgg, plan is \n" + strings.Join(rawPlan, "\n")), false
}

func checkMPP2PhaseAgg(rawPlan []string) (reason string, ok bool) {
	var tidbAgg, tiflashAgg bool
	for _, line := range rawPlan {
		if strings.Contains(line, "Agg") {
			if strings.Contains(line, "root") {
				tidbAgg = true
			}
			if strings.Contains(line, "tiflash") {
				tiflashAgg = true
			}
		}
	}
	if !tidbAgg && tiflashAgg { // all agg work is done in TiFlash
		return "", true
	}
	return fmt.Sprintf("not a MPPTiDBAgg, plan is \n" + strings.Join(rawPlan, "\n")), false
}

func checkMPPHJ(rawPlan []string) (reason string, ok bool) {
	for _, line := range rawPlan {
		if strings.Contains(line, "ExchangeType: HashPartition") {
			return "", true
		}
	}
	return fmt.Sprintf("not a MPPHJ, plan is \n" + strings.Join(rawPlan, "\n")), false
}

func checkMPPBCJ(rawPlan []string) (reason string, ok bool) {
	for _, line := range rawPlan {
		if strings.Contains(line, "Broadcast") {
			return "", true
		}
	}
	return fmt.Sprintf("not a MPPBCJ, plan is \n" + strings.Join(rawPlan, "\n")), false
}

func KendallCorrelationByRecords(rs Records) float64 {
	var xs, ys []float64
	for _, r := range rs {
		xs = append(xs, r.TimeMS)
		ys = append(ys, r.Cost)
	}
	return KendallCorrelation(xs, ys)
}

func KendallCorrelation(estCosts, actTimes []float64) float64 {
	n := len(estCosts)
	tot := n * (n - 1) / 2
	var concordant int
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if (estCosts[j] >= estCosts[i] && actTimes[j] >= actTimes[i]) ||
				(estCosts[j] <= estCosts[i] && actTimes[j] <= actTimes[i]) {
				concordant += 1
			}
		}
	}
	discordant := tot - concordant
	return float64(concordant-discordant) / float64(tot)
}

func PearsonCorrelation(estCosts, actTimes []float64) float64 {
	return Covariance(estCosts, actTimes) / (StandardDeviation(estCosts) * StandardDeviation(actTimes))
}

func Covariance(xs, ys []float64) float64 {
	avgx, avgy := Average(xs), Average(ys)
	var acc float64
	for i := range xs {
		acc += (xs[i] - avgx) * (ys[i] - avgy)
	}
	return acc / float64(len(xs)-1)
}

func StandardDeviation(xs []float64) float64 {
	var acc float64
	avg := Average(xs)
	for _, x := range xs {
		acc += (x - avg) * (x - avg)
	}
	acc /= float64(len(xs) - 1)
	return math.Sqrt(acc)
}

func Average(xs []float64) float64 {
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}
