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
	"gonum.org/v1/plot/vg"
)

// GenReport generates a report with MarkDown format.
func GenReport(opt Option, collector EstResultCollector) error {
	mdContent := bytes.Buffer{}
	for qtIdx, qt := range opt.QueryTypes {
		mdContent.WriteString(fmt.Sprintf("## %v q-error report:\n", qt))
		picPath, err := DrawBiasBoxPlotGroupByQueryType(opt, collector, qtIdx)
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
		qes[i] = results[i].QError()
	}
	sort.Float64s(qes)
	return map[string]float64{
		"max": qes[n-1],
		"p50": qes[n/2],
		"p90": qes[(n*9)/10],
		"p95": qes[(n*19)/20],
	}
}

// DrawBiasBoxPlotGroupByQueryType draws a box plot and returns the picture's path.
func DrawBiasBoxPlotGroupByQueryType(opt Option, collector EstResultCollector, qtIdx int) (string, error) {
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
				biases[i].Value = r.QError()
				biases[i].Label = fmt.Sprintf("%4.4f", r.Bias())
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
