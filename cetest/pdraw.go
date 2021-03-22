package cetest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/pingcap/errors"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func PGenPErrorBarChartsReport(opt POption, collector PEstResultCollector) error {
	md := bytes.Buffer{}
	md.WriteString(fmt.Sprintf("# %v\n", opt.QueryType))

	picPath, err := PDrawBarChartsGroup(opt, collector, PError)
	if err != nil {
		return err
	}
	md.WriteString(fmt.Sprintf("![pic](%v)\n", picPath))

	md.WriteString("\nOverEstimation Statistics\n")
	md.WriteString("\n| Label | Total | P50 | P90 | P99 | Max |\n")
	md.WriteString("| ---- | ---- | ---- | ---- | ---- | ---- |\n")
	for idx, label := range opt.Labels {
		stats := analyzePError(collector.EstResults(idx), true)
		md.WriteString(fmt.Sprintf("| %v | %v | %v | %v | %v | %v |\n",
			label, stats["tot"], stats["p50"], stats["p90"], stats["p99"], stats["max"]))
	}

	md.WriteString("\nUnderEstimation Statistics\n")
	md.WriteString("\n| Label | Total | P50 | P90 | P99 | Max |\n")
	md.WriteString("| ---- | ---- | ---- | ---- | ---- | ---- |\n")
	for idx, label := range opt.Labels {
		stats := analyzePError(collector.EstResults(idx), false)
		md.WriteString(fmt.Sprintf("| %v | %v | %v | %v | %v | %v |\n",
			label, stats["tot"], stats["p50"], stats["p90"], stats["p99"], stats["max"]))
	}
	md.WriteString("\n")
	return ioutil.WriteFile(path.Join(opt.ReportDir, "report.md"), md.Bytes(), 0666)
}

func PDrawBarChartsGroup(opt POption, collector PEstResultCollector, calFunc func(EstResult) float64) (string, error) {
	p, err := plot.New()
	if err != nil {
		return "", errors.Trace(err)
	}
	p.Title.Text = "PError distribution"
	p.X.Label.Text = "distribution"
	p.Y.Label.Text = "frequency of occurrence"

	lower, upper := 1.0, -1.0
	for tblIdx := range opt.Tables {
		rs := collector.EstResults(tblIdx)
		lower, upper = updateLowerUpper(lower, upper, rs, calFunc)
	}
	boundaries := adaptiveBoundaries(lower, upper)

	var w float64 = 20
	for tblIdx, label := range opt.Labels {
		rs := collector.EstResults(tblIdx)
		freqs := distribution(rs, boundaries, calFunc)
		bar, err := plotter.NewBarChart(plotter.Values(freqs[1:len(freqs)-2]), vg.Points(w))
		if err != nil {
			return "", errors.Trace(err)
		}
		bar.Color = plotutil.Color(tblIdx)
		bar.Offset = vg.Points(float64(tblIdx-(len(opt.Labels)/2)) * w)
		p.Add(bar)
		p.Legend.Add(label, bar)
	}
	p.Legend.Top = true
	xNames := make([]string, 0, len(boundaries)-1)
	for i := 1; i < len(boundaries); i++ {
		xNames = append(xNames, fmt.Sprintf("[%v, %v)", boundaries[i-1], boundaries[i]))
	}
	p.NominalX(xNames...)

	prefixDir := opt.ReportDir
	if !path.IsAbs(prefixDir) {
		absPrefix, err := os.Getwd()
		if err != nil {
			return "", errors.Trace(err)
		}
		prefixDir = path.Join(absPrefix, prefixDir)
	}

	pngName := fmt.Sprintf("%v-%v-bar.png", opt.QueryType, "partition")
	pngPath := path.Join(prefixDir, pngName)
	return pngName, p.Save(vg.Points(w+(w+5)*float64(len(boundaries)*len(opt.Tables))), 3*vg.Inch, pngPath)
}
