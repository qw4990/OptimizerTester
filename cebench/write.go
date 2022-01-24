package cebench

import (
	"bytes"
	"fmt"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"io"
	"math"
	"path/filepath"
	"sort"
	"strings"
)

func WriteToFileForInfos(infos EstInfos, tp, outDir string, writer io.Writer) {
	// Title
	_, err := writer.Write([]byte(fmt.Sprintf("\n## %s\n", tp)))
	if err != nil {
		panic(err)
	}

	// Bar chart
	chartFileName := strings.ReplaceAll(fmt.Sprintf(chartFileTemplate, tp), " ", "-")
	chartPath := filepath.Join(outDir, chartFileName)
	p := plot.New()
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

	_, err = writer.Write([]byte(fmt.Sprintf("![chart](%s)\n", chartFileName)))
	if err != nil {
		panic(err)
	}

	// Form
	var overEstInfos, underEstInfos EstInfos
	exactCnt := 0
	for _, info := range infos {
		if info.pError < 0 {
			underEstInfos = append(underEstInfos, info)
		} else if info.pError > 0 {
			overEstInfos = append(overEstInfos, info)
		} else {
			exactCnt++
		}
	}
	_, err = writer.Write([]byte(fmt.Sprintf("\n### Exact estimation count: %d\n", exactCnt)))
	if err != nil {
		panic(err)
	}
	sort.Sort(sort.Reverse(underEstInfos))
	WriteOverOrUnderStats(overEstInfos, true, writer)
	WriteOverOrUnderStats(underEstInfos, false, writer)
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

func WritePErrorAboveThresh(infos EstInfos, writer io.Writer, threshold float64) {
	str := bytes.Buffer{}
	sort.Slice(infos, func(i, j int) bool {
		iError := infos[i].pError
		jError := infos[j].pError
		return math.Abs(iError) > math.Abs(jError)
	})
	str.WriteString("\n| Type | Expr | Est | Actual | PError |\n")
	str.WriteString("| ---- | ---- | ---- | ---- | ---- |\n")
	for _, info := range infos {
		if math.Abs(info.pError) < threshold {
			break
		}
		str.WriteString(fmt.Sprintf("| %s | %s | %d | %d | %.3f |\n", info.Type, info.Expr, info.Est, info.Actual, info.pError))
	}
	_, err := str.WriteTo(writer)
	if err != nil {
		panic(err)
	}
}
