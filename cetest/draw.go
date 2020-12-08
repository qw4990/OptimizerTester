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
	"gonum.org/v1/plot/vg"
)

// GenReport generates a report with MarkDown format.
func GenReport(opt Option, collector EstResultCollector) error {
	mdContent := bytes.Buffer{}
	for qtIdx, qt := range opt.QueryTypes {
		picPath, err := DrawBiasBoxPlotGroupByQueryType(opt, collector, qtIdx)
		if err != nil {
			return err
		}
		if _, err := mdContent.WriteString(fmt.Sprintf("%v: ![pic](%v)\n", qt, picPath)); err != nil {
			return errors.Trace(err)
		}
	}
	return ioutil.WriteFile(path.Join(opt.ReportDir, "report.md"), mdContent.Bytes(), 0666)
}

// DrawBiasBoxPlotGroupByQueryType draws a box plot and returns the picture's path.
func DrawBiasBoxPlotGroupByQueryType(opt Option, collector EstResultCollector, qtIdx int) (string, error) {
	p, err := plot.New()
	if err != nil {
		return "", errors.Trace(err)
	}
	p.Title.Text = "Bias Box Plot: " + opt.QueryTypes[qtIdx].String()
	p.Y.Label.Text = "Bias Values"
	boxes := make([]plot.Plotter, 0, len(opt.Datasets)*len(opt.Instances))
	picNames := make([]string, 0, len(opt.Datasets)*len(opt.Instances))
	for dsIdx, ds := range opt.Datasets {
		for insIdx, ins := range opt.Instances {
			rs := collector.EstResults(insIdx, dsIdx, qtIdx)
			biases := make(plotter.ValueLabels, len(rs))
			for i, r := range rs {
				biases[i].Value = r.Bias()
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
