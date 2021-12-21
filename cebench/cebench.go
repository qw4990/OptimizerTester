package cebench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const concurrencyForEachDSN = uint(2)
const fullEstInfoFile = "full_est_info.json"
const reportMDFile = "report.md"
const chartFileTemplate = "chart_%s.png"

func logTime() string {
	str, err := time.Now().MarshalText()
	if err != nil {
		panic(err)
	}
	return string(str)
}

func RunCEBench(queryLocation string, dsns []string, outDir string) error {
	// 1. Collect all files in the specified location.
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
	queryTaskChan := make(chan *QueryTask, 100)
	tracePlanResChan := make(chan *QueryResult, 100)
	actualCntResChan := make(chan *QueryResult, 100)
	for i, dsn := range dsns {
		err = StartQueryRunner(dsn, queryTaskChan, concurrencyForEachDSN, 2, uint(i))
		if err != nil {
			return err
		}
		fmt.Printf("[%s] %d query runners started for DSN#%d: %s.\n", logTime(), concurrencyForEachDSN, i, dsn)
	}
	go SQLProvider(files, queryTaskChan, tracePlanResChan)
	go TraceResultProvider(tracePlanResChan, queryTaskChan, actualCntResChan)
	estInfoMap, allEstInfos := CollectEstInfo(actualCntResChan)

	// Information needed has been collected.
	// 2. Now start to analyze and output them.
	CalcPError(estInfoMap)
	for _, infos := range estInfoMap {
		sort.Sort(infos)
	}
	sort.Sort(allEstInfos)
	err = os.MkdirAll(outDir, os.ModePerm)
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

	// Title
	_, err = reportF.Write([]byte("## All\n"))
	if err != nil {
		panic(err)
	}

	// Bar chart
	chartFileName := fmt.Sprintf(chartFileTemplate, "All")
	chartPath := filepath.Join(outDir, chartFileName)
	p, err := plot.New()
	if err != nil {
		panic(err)
	}
	chartVals := distribution(allEstInfos)
	bar, err := plotter.NewBarChart(plotter.Values(chartVals), vg.Points(25))
	if err != nil {
		panic(err)
	}
	p.Add(bar)
	p.NominalX(xAxisNames...)
	p.Title.Text = "PError distribution"
	err = p.Save(vg.Points(600), vg.Points(150), chartPath)
	if err != nil {
		panic(err)
	}

	_, err = reportF.Write([]byte(fmt.Sprintf("![chart](%s)\n", chartFileName)))
	if err != nil {
		panic(err)
	}

	// Form
	var overEstInfos, underEstInfos EstInfos
	for _, info := range allEstInfos {
		if info.pError < 0 {
			underEstInfos = append(underEstInfos, info)
		} else if info.pError > 0 {
			overEstInfos = append(overEstInfos, info)
		}
	}
	sort.Sort(sort.Reverse(underEstInfos))
	WriteOverOrUnderStats(overEstInfos, true, reportF)
	WriteOverOrUnderStats(underEstInfos, false, reportF)

	_, err = reportF.Write([]byte("\n## Globally Worst 10 case:\n"))
	if err != nil {
		panic(err)
	}
	WriteWorst10(allEstInfos, reportF)

	for tp, infos := range estInfoMap {
		// Title
		_, err = reportF.Write([]byte(fmt.Sprintf("\n## %s\n", tp)))
		if err != nil {
			panic(err)
		}

		// Bar chart
		chartFileName := strings.ReplaceAll(fmt.Sprintf(chartFileTemplate, tp), " ", "-")
		chartPath := filepath.Join(outDir, chartFileName)
		p, err := plot.New()
		if err != nil {
			panic(err)
		}
		chartVals := distribution(infos)
		bar, err := plotter.NewBarChart(plotter.Values(chartVals), vg.Points(25))
		if err != nil {
			panic(err)
		}
		p.Add(bar)
		p.NominalX(xAxisNames...)
		p.Title.Text = "PError distribution"
		err = p.Save(vg.Points(600), vg.Points(150), chartPath)
		if err != nil {
			panic(err)
		}

		_, err = reportF.Write([]byte(fmt.Sprintf("![chart](%s)\n", chartFileName)))
		if err != nil {
			panic(err)
		}

		// Form
		var overEstInfos, underEstInfos EstInfos
		for _, info := range infos {
			if info.pError < 0 {
				underEstInfos = append(underEstInfos, info)
			} else if info.pError > 0 {
				overEstInfos = append(overEstInfos, info)
			}
		}
		sort.Sort(sort.Reverse(underEstInfos))
		WriteOverOrUnderStats(overEstInfos, true, reportF)
		WriteOverOrUnderStats(underEstInfos, false, reportF)
	}

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

func CollectEstInfo(inChan <-chan *QueryResult) (map[string]EstInfos, EstInfos) {
	res := make(map[string]EstInfos)
	allEstInfos := make(EstInfos, 0, 8)
	for queryRes := range inChan {
		queryResVal := queryRes.result[0][0].([]uint8)
		actualCnt, err := strconv.ParseUint(string(queryResVal), 10, 64)
		if err != nil {
			panic(err)
		}
		traceRecord := queryRes.payload.(*CETraceRecord)
		estRes := EstInfo{
			Expr:   traceRecord.Expr,
			Type:   traceRecord.Type,
			Est:    traceRecord.RowCount,
			Actual: actualCnt,
		}
		res[estRes.Type] = append(res[traceRecord.Type], &estRes)
		allEstInfos = append(allEstInfos, &estRes)
	}
	fmt.Printf("[%s] All estimation information collected.\n", logTime())
	return res, allEstInfos
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

func WriteOverOrUnderStats(infos EstInfos, forOverEst bool, writer io.Writer) {
	str := bytes.Buffer{}
	defer func() {
		_, err := str.WriteTo(writer)
		if err != nil {
			panic(err)
		}
	}()
	if forOverEst {
		str.WriteString("### OverEstimation Statistics\n")
	} else {
		str.WriteString("### UnderEstimation Statistics\n")
	}
	str.WriteString("\n| Total | P50 | P90 | P99 | Max |\n")
	str.WriteString("| ---- | ---- | ---- | ---- | ---- |\n")
	if len(infos) == 0 {
		str.WriteString("| 0 | - | - | - | - |\n")
		return
	}
	n := len(infos)
	str.WriteString(fmt.Sprintf("| %d | %.3f | %.3f | %.3f | %.3f |\n",
		n,
		infos[(n*50)/100].pError,
		infos[(n*90)/100].pError,
		infos[(n*99)/100].pError,
		infos[n-1].pError))
}

func WriteWorst10(infos EstInfos, writer io.Writer) {
	str := bytes.Buffer{}
	sort.Slice(infos, func(i, j int) bool {
		iError := infos[i].pError
		jError := infos[j].pError
		return math.Abs(iError) < math.Abs(jError)
	})
	n := 10
	if len(infos) < n {
		n = len(infos)
	}
	str.WriteString("\n| Type | Expr | Est | Actual | PError |\n")
	str.WriteString("| ---- | ---- | ---- | ---- | ---- |\n")
	for i := 0; i < n; i++ {
		info := infos[len(infos)-i-1]
		str.WriteString(fmt.Sprintf("| %s | %s | %d | %d | %.3f |\n", info.Type, info.Expr, info.Est, info.Actual, info.pError))
	}
	_, err := str.WriteTo(writer)
	if err != nil {
		panic(err)
	}
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
