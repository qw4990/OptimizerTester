package cetest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"

	"github.com/pingcap/errors"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

// GenPErrorBarChartsReport ...
func GenPErrorBarChartsReport(opt Option, collector EstResultCollector) error {
	md := bytes.Buffer{}
	for qtIdx, qt := range opt.QueryTypes {
		md.WriteString(fmt.Sprintf("# %v\n", qt))
		for dsIdx, ds := range opt.Datasets {
			md.WriteString(fmt.Sprintf("## %v\n", ds.Label))
			picPath, err := DrawBarChartsGroupByQTAndDS(opt, collector, qtIdx, dsIdx, PError)
			if err != nil {
				return err
			}
			md.WriteString(fmt.Sprintf("![pic](%v)\n", picPath))

			md.WriteString("\nOverEstimation Statistics\n")
			md.WriteString("\n| Instance | Total | P50 | P90 | Max |\n")
			md.WriteString("| ---- | ---- | ---- | ---- | ---- |\n")
			for insIdx, ins := range opt.Instances {
				stats := analyzePError(collector.EstResults(insIdx, dsIdx, qtIdx), true)
				md.WriteString(fmt.Sprintf("| %v | %.4f | %.4f | %.4f | %.4f |\n",
					ins.Label, stats["tot"], stats["p50"], stats["p90"], stats["max"]))
			}

			md.WriteString("\nUnderEstimation Statistics\n")
			md.WriteString("\n| Instance | Total | P50 | P90 | Max |\n")
			md.WriteString("| ---- | ---- | ---- | ---- | ---- |\n")
			for insIdx, ins := range opt.Instances {
				stats := analyzePError(collector.EstResults(insIdx, dsIdx, qtIdx), false)
				md.WriteString(fmt.Sprintf("| %v | %.4f | %.4f | %.4f | %.4f |\n",
					ins.Label, stats["tot"], stats["p50"], stats["p90"], stats["max"]))
			}
			md.WriteString("\n")
		}
	}
	return ioutil.WriteFile(path.Join(opt.ReportDir, "report.md"), md.Bytes(), 0666)
}

func analyzePError(results []EstResult, isOverEst bool) map[string]float64 {
	pes := make([]float64, 0, len(results))
	for i := range results {
		pe := PError(results[i])
		if isOverEst && pe > 0 {
			pes = append(pes, pe)
		} else if !isOverEst && pe < 0 {
			pes = append(pes, pe)
		}
	}
	sort.Float64s(pes)
	if !isOverEst { // reverse
		for i, j := 0, len(pes)-1; i < j; i, j = i+1, j-1 {
			pes[i], pes[j] = pes[j], pes[i]
		}
	}
	n := len(pes)
	if n == 0 {
		return map[string]float64{
			"tot": 0,
			"max": 0,
			"p50": 0,
			"p90": 0,
		}
	}
	return map[string]float64{
		"tot": float64(n),
		"max": pes[n-1],
		"p50": pes[n/2],
		"p90": pes[(n*9)/10],
	}
}

// GenQErrorBoxPlotReport generates a report with MarkDown format.
func GenQErrorBoxPlotReport(opt Option, collector EstResultCollector) error {
	mdContent := bytes.Buffer{}
	for qtIdx, qt := range opt.QueryTypes {
		mdContent.WriteString(fmt.Sprintf("## %v q-error report:\n", qt))
		picPath, err := DrawQErrorBoxPlotGroupByQueryType(opt, collector, qtIdx)
		if err != nil {
			return err
		}
		if _, err := mdContent.WriteString(fmt.Sprintf("![pic](%v)\n", picPath)); err != nil {
			return errors.Trace(err)
		}

		mdContent.WriteString("\n| Dataset | Instance | P50 | P90 | P95 | Max |\n")
		mdContent.WriteString("| ---- | ---- | ---- | ---- | ---- | ---- |\n")
		for dsIdx, ds := range opt.Datasets {
			for insIdx, ins := range opt.Instances {
				stats := analyzeQError(collector.EstResults(insIdx, dsIdx, qtIdx))
				mdContent.WriteString(fmt.Sprintf("| %v | %v | %.4f | %.4f | %.4f | %.4f |\n",
					ds.Label, ins.Label, stats["p50"], stats["p90"], stats["p95"], stats["max"]))
			}
		}
		mdContent.WriteString("\n")
	}
	return ioutil.WriteFile(path.Join(opt.ReportDir, "report.md"), mdContent.Bytes(), 0666)
}

func analyzeQError(results []EstResult) map[string]float64 {
	n := len(results)
	qes := make([]float64, n)
	for i := range results {
		qes[i] = QError(results[i])
	}
	sort.Float64s(qes)
	return map[string]float64{
		"max": qes[n-1],
		"p50": qes[n/2],
		"p90": qes[(n*9)/10],
		"p95": qes[(n*19)/20],
	}
}

// DrawBarChartsGroupByQTAndDS ...
func DrawBarChartsGroupByQTAndDS(opt Option, collector EstResultCollector, qtIdx, dsIdx int, calFunc func(EstResult) float64) (string, error) {
	p, err := plot.New()
	if err != nil {
		return "", errors.Trace(err)
	}
	p.Title.Text = fmt.Sprintf("PError distribution on %v", opt.Datasets[dsIdx].Label)
	p.X.Label.Text = "distribution"
	p.Y.Label.Text = "frequency of occurrence"

	var w float64 = 20
	boundaries := []float64{-10, -4, -2, -1, 0, 1, 2, 4, 10}
	for insIdx, ins := range opt.Instances {
		rs := collector.EstResults(insIdx, dsIdx, qtIdx)
		freqs := distribution(rs, boundaries, calFunc)
		bar, err := plotter.NewBarChart(plotter.Values(freqs), vg.Points(w))
		if err != nil {
			return "", errors.Trace(err)
		}
		bar.Color = plotutil.Color(insIdx)
		bar.Offset = vg.Points(float64(insIdx-(len(opt.Instances)/2)) * w)
		p.Add(bar)
		p.Legend.Add(ins.Label, bar)
	}
	p.Legend.Top = true
	xNames := make([]string, 0, len(boundaries)+1)
	xNames = append(xNames, fmt.Sprintf("<%v", boundaries[0]))
	for i := 1; i < len(boundaries); i++ {
		xNames = append(xNames, fmt.Sprintf("[%v, %v)", boundaries[i-1], boundaries[i]))
	}
	xNames = append(xNames, fmt.Sprintf(">=%v", boundaries[len(boundaries)-1]))
	p.NominalX(xNames...)

	prefixDir := opt.ReportDir
	if !path.IsAbs(prefixDir) {
		absPrefix, err := os.Getwd()
		if err != nil {
			return "", errors.Trace(err)
		}
		prefixDir = path.Join(absPrefix, prefixDir)
	}

	pngPath := path.Join(prefixDir, fmt.Sprintf("%v-%v-bar.png", opt.QueryTypes[qtIdx], opt.Datasets[dsIdx].Label))
	return pngPath, p.Save(vg.Points(10*w*float64(len(opt.Instances)+1)), 3*vg.Inch, pngPath)
}

func distribution(rs []EstResult, boundaries []float64, calFunc func(EstResult) float64) []float64 {
	freqs := make([]float64, len(boundaries)+1)
	for _, r := range rs {
		qe := calFunc(r)
		i := 0
		for ; i < len(boundaries); i++ {
			if qe < boundaries[i] {
				break
			}
		}
		freqs[i]++
	}
	return freqs
}

// DrawQErrorBoxPlotGroupByQueryType draws a box plot and returns the picture's path.
func DrawQErrorBoxPlotGroupByQueryType(opt Option, collector EstResultCollector, qtIdx int) (string, error) {
	p, err := plot.New()
	if err != nil {
		return "", errors.Trace(err)
	}
	p.Title.Text = "QError Box Plot: " + opt.QueryTypes[qtIdx].String()
	p.Y.Label.Text = "QError Values"
	boxes := make([]plot.Plotter, 0, len(opt.Datasets)*len(opt.Instances))
	picNames := make([]string, 0, len(opt.Datasets)*len(opt.Instances))
	for dsIdx, ds := range opt.Datasets {
		for insIdx, ins := range opt.Instances {
			rs := collector.EstResults(insIdx, dsIdx, qtIdx)
			biases := make(plotter.ValueLabels, len(rs))
			for i, r := range rs {
				biases[i].Value = QError(r)
				biases[i].Label = fmt.Sprintf("%4.4f", QError(r))
			}
			box, err := plotter.NewBoxPlot(vg.Points(20), float64(len(boxes)), biases)
			if err != nil {
				return "", errors.Trace(err)
			}
			boxes = append(boxes, box)
			picNames = append(picNames, fmt.Sprintf("%v:%v", ds.Label, ins.Label))
		}
	}
	p.Add(boxes...)
	p.NominalX(picNames...)

	prefixDir := opt.ReportDir
	if !path.IsAbs(prefixDir) {
		absPrefix, err := os.Getwd()
		if err != nil {
			return "", errors.Trace(err)
		}
		prefixDir = path.Join(absPrefix, prefixDir)
	}

	pngPath := path.Join(prefixDir, fmt.Sprintf("%v-box-plot.png", opt.QueryTypes[qtIdx]))
	return pngPath, errors.Trace(p.Save(vg.Length(100+80*len(opt.Datasets)*len(opt.Instances)), 200, pngPath))
}
