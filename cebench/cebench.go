package cebench

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const fullEstInfoFile = "full_est_info.json"
const reportMDFile = "report.md"
const chartFileTemplate = "chart_%s.png"

var needDedup = true

func logTime() string {
	str, err := time.Now().MarshalText()
	if err != nil {
		panic(err)
	}
	return string(str)
}

func RunCEBench(queryLocation string, dsns []string, jsonLocation, outDir string, dedup bool, threshold, concurrencyForEachDSN uint) error {
	needDedup = dedup
	// 1. Collect estimation information.
	var allEstInfos EstInfos
	estInfoMap := make(map[string]EstInfos)
	if len(jsonLocation) > 0 {
		jsonBytes, err := ioutil.ReadFile(jsonLocation)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(jsonBytes, &estInfoMap)
		if err != nil {
			panic(err)
		}
		for _, infos := range estInfoMap {
			for _, info := range infos {
				allEstInfos = append(allEstInfos, info)
			}
		}
	} else if len(queryLocation) > 0 && len(dsns) > 0 {
		var files []string
		collectFiles := func(path string, d fs.DirEntry, err error) error {
			if !d.IsDir() {
				files = append(files, path)
			}
			return nil
		}
		err := filepath.WalkDir(queryLocation, collectFiles)
		if err != nil {
			return err
		}
		fmt.Printf("[%s] %d sql files found.\n", logTime(), len(files))
		queryTaskChan := make(chan *tidb.QueryTask, 100)
		tracePlanResChan := make(chan *tidb.QueryResult, 100)
		actualCntResChan := make(chan *tidb.QueryResult, 100)
		for i, dsn := range dsns {
			err = tidb.StartQueryRunner(dsn, queryTaskChan, concurrencyForEachDSN, 2, uint(i))
			if err != nil {
				return err
			}
			fmt.Printf("[%s] %d query runners started for DSN#%d: %s.\n", logTime(), concurrencyForEachDSN, i, dsn)
		}
		go SQLProvider(files, queryTaskChan, tracePlanResChan)
		go TraceResultProvider(tracePlanResChan, queryTaskChan, actualCntResChan)
		allEstInfos = CollectEstInfo(actualCntResChan)
	} else {
		return errors.New("should specify one method to get the estimation information.\n(1) SQL file(s) + DSN(s)\n(2) JSON file")
	}
	if needDedup {
		allEstInfos = DedupEstInfo(allEstInfos)
	}
	for _, info := range allEstInfos {
		estInfoMap[info.Type] = append(estInfoMap[info.Type], info)
	}

	// Information needed has been collected.
	// 2. Calculate and sort by p-error. Write all information into a json file.
	CalcPError(estInfoMap)
	for _, infos := range estInfoMap {
		sort.Sort(infos)
	}
	sort.Sort(allEstInfos)
	err := os.MkdirAll(outDir, os.ModePerm)
	if err != nil {
		panic(err)
	}
	fullInfoF, err := os.Create(filepath.Join(outDir, fullEstInfoFile))
	if err != nil {
		panic(err)
	}
	defer func() {
		err = fullInfoF.Close()
		if err != nil {
			panic(err)
		}
	}()
	encoder := json.NewEncoder(fullInfoF)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(estInfoMap)
	if err != nil {
		panic(err)
	}

	// 3. Generate the report.
	reportF, err := os.Create(filepath.Join(outDir, reportMDFile))
	if err != nil {
		panic(err)
	}
	defer func() {
		err = reportF.Close()
		if err != nil {
			panic(err)
		}
	}()

	WriteToFileForInfos(allEstInfos, "All", outDir, reportF)

	_, err = reportF.Write([]byte("\n### Globally Worst 10 cases:\n"))
	if err != nil {
		panic(err)
	}
	WriteWorst10(allEstInfos, reportF)

	for tp, infos := range estInfoMap {
		WriteToFileForInfos(infos, tp, outDir, reportF)
		_, err = reportF.Write([]byte("\n### Worst 10 cases:\n"))
		if err != nil {
			panic(err)
		}
		WriteWorst10(infos, reportF)
	}

	_, err = reportF.Write([]byte(fmt.Sprintf("\n## All cases with p-error above the threshold (%d):\n", threshold)))
	WritePErrorAboveThresh(allEstInfos, reportF, float64(threshold))

	fmt.Printf("[%s] Analyze finished and results are written into files. Tester exited.\n", logTime())
	return nil
}

type EstInfo struct {
	Expr   string
	Type   string
	Est    uint64
	Actual uint64
	pError float64
}

type EstInfos []*EstInfo

func (s EstInfos) Len() int {
	return len(s)
}

func (s EstInfos) Less(i, j int) bool {
	return s[i].pError < s[j].pError
}

func (s EstInfos) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func CollectEstInfo(inChan <-chan *tidb.QueryResult) EstInfos {
	allEstInfos := make(EstInfos, 0, 8)
	cnt := 0
	for queryRes := range inChan {
		queryResVal := queryRes.Result[0][0].([]uint8)
		actualCnt, err := strconv.ParseUint(string(queryResVal), 10, 64)
		if err != nil {
			panic(err)
		}
		cnt++
		if cnt%20 == 0 {
			fmt.Printf("[%s] estimation information for %d records collected.\n", logTime(), cnt)
		}
		traceRecord := queryRes.Payload.(*CETraceRecord)
		estRes := EstInfo{
			Expr:   traceRecord.Expr,
			Type:   traceRecord.Type,
			Est:    traceRecord.RowCount,
			Actual: actualCnt,
		}
		allEstInfos = append(allEstInfos, &estRes)
	}
	fmt.Printf("[%s] All estimation information collected.\n", logTime())
	return allEstInfos
}

func DedupEstInfo(records EstInfos) EstInfos {
	ret := make(EstInfos, 0, len(records))
	exists := make(map[EstInfo]struct{}, len(records))
	for _, rec := range records {
		if _, ok := exists[*rec]; !ok {
			ret = append(ret, rec)
			exists[*rec] = struct{}{}
		}
	}
	return ret
}

func CalcPError(estInfoMap map[string]EstInfos) {
	for _, estInfos := range estInfoMap {
		for _, info := range estInfos {
			est := info.Est
			act := info.Actual
			info.pError = pError(est, act)
		}
	}
}

func pError(est, act uint64) float64 {
	if est == act {
		return 0
	}
	sign := float64(1)
	larger, lower := est, act
	if larger < lower {
		sign = -1
		larger, lower = lower, larger
	}
	if lower == 0 {
		larger++
		lower++
	}
	return sign * (float64(larger)/float64(lower) - 1)
}

type bucket struct {
	hasLowBound   bool
	lowBound      int64
	lowExclusive  bool
	hasHighBound  bool
	highBound     int64
	highExclusive bool
}

var xAxisNames = []string{"(-inf", "[-6561", "[-2187", "[-729", "[-243", "[-81", "[-27", "[-9", "[-3",
	"[-1, 1]",
	"3]", "9]", "27]", "81]", "243]", "729]", "2187]", "6561]", "+inf)",
}

// (-inf, -3^8), [-3^8, -3^7), [-3^7, -3^6), [-3^6, -3^5), [-3^5, -3^4), [-3^4, -3^3), [-3^3, -3^2), [-3^2, -3^1), [-3^1, -3^0),
// [-1, 1],
// (3^0, 3^1], (3^1, 3^2], (3^2, 3^3], (3^3, 3^4], (3^4, 3^5], (3^5, 3^6], (3^6, 3^7], (3^7, 3^8], (3^8, +inf)
var defaultBuckets = []bucket{
	{false, 0, false, true, -6561, true},
	{true, -6561, false, true, -2187, true},
	{true, -2187, false, true, -729, true},
	{true, -729, false, true, -243, true},
	{true, -243, false, true, -81, true},
	{true, -81, false, true, -27, true},
	{true, -27, false, true, -9, true},
	{true, -9, false, true, -3, true},
	{true, -3, false, true, -1, true},

	{true, -1, false, true, 1, false},

	{true, 1, true, true, 3, false},
	{true, 3, true, true, 9, false},
	{true, 9, true, true, 27, false},
	{true, 27, true, true, 81, false},
	{true, 81, true, true, 243, false},
	{true, 243, true, true, 729, false},
	{true, 729, true, true, 2187, false},
	{true, 2187, true, true, 6561, false},
	{true, 6561, true, false, 0, false},
}

func distribution(orderedInfos EstInfos) []float64 {
	res := make([]float64, len(defaultBuckets))
	curBkt := 0
	for _, info := range orderedInfos {
		for goToNextBkt(info.pError, curBkt) {
			curBkt++
		}
		res[curBkt]++
	}
	return res
}

func goToNextBkt(val float64, bktIdx int) bool {
	bkt := defaultBuckets[bktIdx]
	if !bkt.hasHighBound {
		return false
	}
	if bkt.highExclusive {
		return int64(val) >= bkt.highBound
	} else {
		return int64(val) > bkt.highBound
	}
}
