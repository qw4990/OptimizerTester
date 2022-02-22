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
	data, err := json.MarshalIndent(r, "", "    ")
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
	"tidb_opt_memory_factor", "tidb_opt_seek_factor", 
	"tidb_opt_tiflash_scan_factor"}

func setCostFactors(ins tidb.Instance, factors CostFactors) {
	fmt.Println("SET COST FACTORS(CPU, CopCPU, Net, Scan, DescScan, Mem, Seek, TiFlashScan):", factors)
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
	fmt.Println("READ COST FACTORS(CPU, CopCPU, Net, Scan, DescScan, Mem, Seek, TiFlashScan):", factors)
	return
}

func calculateCost(weights CostWeights, factors CostFactors) float64 {
	var cost float64
	for i := range factors {
		cost += weights[i] * factors[i]
	}
	return cost
}

func extractCostTimeFromQuery(ins tidb.Instance, explainAnalyzeQuery string,
	repeat, timeLimitMS int, checkRowCount bool,
	planChecker PlanChecker) (rootOperator string, avgPlanCost, avgTimeMS float64, tle bool, cw CostWeights) {
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
			return "", 0, 0, true, cw
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

		// cost trace
		if cw.IsZero() {
			cw = explainResult.TraceWeights
		} else {
			if !cw.EqualTo(explainResult.TraceWeights) {
				panic(fmt.Sprintf("cost weights changed from %v to %v for q=%v", cw, explainResult.TraceWeights, explainAnalyzeQuery))
			}
		}
		if math.Abs(explainResult.TraceCost-explainResult.PlanCost)/explainResult.PlanCost > 0.10 {
			panic(fmt.Sprintf("wrong calCost %v:%v for q=%v", explainResult.TraceCost, explainResult.PlanCost, explainAnalyzeQuery))
		}
	}
	return rootOperator, totalPlanCost / float64(repeat), totalTimeMS / float64(repeat), false, cw
}

//+------------------------+----------+-------------------------------------------------------------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------+---------+------+
//| id                     | estRows  | estCost                                                           | actRows | task      | access object | execution info                                                                                                                                                                                                                                              | operator info                                   | memory  | disk |
//	+------------------------+----------+-------------------------------------------------------------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------+---------+------+
//| TableReader_6          | 10000.00 | 52418.00:[0.00,0.00,81250.00,470000.00,0.00,0.00,1.00]:786270.00  | 10000   | root      |               | time:9.93ms, loops:11, cop_task: {num: 1, max: 9.75ms, proc_keys: 10000, tot_proc: 5ms, tot_wait: 2ms, rpc_num: 1, rpc_time: 9.7ms, copr_cache: disabled}                                                                                                   | data:TableRangeScan_5                           | 78.5 KB | N/A  |
//| └─TableRangeScan_5     | 10000.00 | 705020.00:[0.00,0.00,81250.00,470000.00,0.00,0.00,1.00]:786270.00 | 10000   | cop[tikv] | table:t       | tikv_task:{time:5ms, loops:14}, scan_detail: {total_process_keys: 10000, total_process_keys_size: 270000, total_keys: 10500, rocksdb: {delete_skipped_count: 0, key_skipped_count: 10499, block: {cache_hit_count: 33, read_count: 0, read_byte: 0 Bytes}}} | range:[1,10000], keep order:false, stats:pseudo | N/A     | N/A  |
//+------------------------+----------+-------------------------------------------------------------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------+---------+------+
type ExplainAnalyzeResult struct {
	RootOperator    string
	PlanCost        float64
	TraceCost       float64
	TraceWeights    CostWeights
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
	//+------------------------+----------+-------------------------------------------------------------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------+---------+------+
	//| id                     | estRows  | estCost                                                           | actRows | task      | access object | execution info                                                                                                                                                                                                                                              | operator info                                   | memory  | disk |
	//	+------------------------+----------+-------------------------------------------------------------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------+---------+------+
	//| TableReader_6          | 10000.00 | 52418.00:[0.00,0.00,81250.00,470000.00,0.00,0.00,1.00]:786270.00  | 10000   | root      |               | time:9.93ms, loops:11, cop_task: {num: 1, max: 9.75ms, proc_keys: 10000, tot_proc: 5ms, tot_wait: 2ms, rpc_num: 1, rpc_time: 9.7ms, copr_cache: disabled}                                                                                                   | data:TableRangeScan_5                           | 78.5 KB | N/A  |
	//| └─TableRangeScan_5     | 10000.00 | 705020.00:[0.00,0.00,81250.00,470000.00,0.00,0.00,1.00]:786270.00 | 10000   | cop[tikv] | table:t       | tikv_task:{time:5ms, loops:14}, scan_detail: {total_process_keys: 10000, total_process_keys_size: 270000, total_keys: 10500, rocksdb: {delete_skipped_count: 0, key_skipped_count: 10499, block: {cache_hit_count: 33, read_count: 0, read_byte: 0 Bytes}}} | range:[1,10000], keep order:false, stats:pseudo | N/A     | N/A  |
	//+------------------------+----------+-------------------------------------------------------------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------+---------+------+
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
			r.TraceCost = mustStr2Float(tmp[2])

			tmp[1] = strings.Trim(tmp[1], " []")
			weightStrs := strings.Split(tmp[1], ",")
			for i := 0; i < NumFactors; i++ {
				r.TraceWeights[i] = mustStr2Float(weightStrs[i])
			}
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

func getPlanChecker(label string) PlanChecker {
	switch strings.ToLower(label) {
	case "tiflashscan":
		return checkTiFlashScan
	case "mppscan":
		return checkMPPScan
	case "mpptidbagg":
		return checkMPPTiDBAgg
	case "mpp2phaseagg":
		return checkMPP2PhaseAgg
	case "mpphj":
		return checkMPPHJ
	case "mppbcj":
		return checkMPPBCJ
	}
	return nil
}

func checkTiFlashScan(rawPlan []string) (reason string, ok bool) {
	var tiflashScan, exchanger bool
	for _, line := range rawPlan {
		if strings.Contains(line, "Scan") && strings.Contains(line, "tiflash") {
			tiflashScan = true
		}
		if strings.Contains(line, "ExchangeSender") {
			exchanger = true
		}
	}
	if tiflashScan && !exchanger {
		return "", true
	}
	return fmt.Sprintf("not a TiFlashScan, plan is \n" + strings.Join(rawPlan, "\n")), false
}

func checkMPPScan(rawPlan []string) (reason string, ok bool) {
	var tiflashScan, exchanger bool
	for _, line := range rawPlan {
		if strings.Contains(line, "Scan") && strings.Contains(line, "tiflash") {
			tiflashScan = true
		}
		if strings.Contains(line, "ExchangeSender") {
			exchanger = true
		}
	}
	if tiflashScan && exchanger {
		return "", true
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

func filterQueriesByLabel(qs Queries, whiteList []string) (ret Queries) {
	for _, q := range qs {
		for _, label := range whiteList {
			if strings.Contains(strings.ToLower(q.Label), strings.ToLower(label)) {
				ret = append(ret, q)
				break
			}
		}
	}
	return
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
