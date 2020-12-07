package cetest

import (
	"fmt"
	"path"

	"github.com/pingcap/errors"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// DrawBiasBoxPlotGroupByQueryType draws a box plot and returns the picture's path.
func DrawBiasBoxPlotGroupByQueryType(opt Option, collector EstResultCollector, qtIdx int) (string, error) {
	p, err := plot.New()
	if err != nil {
		return "", errors.Trace(err)
	}
	p.Title.Text = "Bias Box Plot: " + queryType
	p.Y.Label.Text = "Bias Values"
	boxes := make([]plot.Plotter, len(datasetNames))
	for i := range datasetNames {
		results := estResults[i]
		biases := make(plotter.ValueLabels, len(results))
		for i, r := range results {
			biases[i].Value = r.Bias()
			biases[i].Label = fmt.Sprintf("%4.4f", r.Bias())
		}
		box, err := plotter.NewBoxPlot(vg.Points(20), float64(i), biases)
		if err != nil {
			return "", errors.Trace(err)
		}
		boxes[i] = box
	}
	p.Add(boxes...)
	p.NominalX(datasetNames...)

	pngPath := path.Join(dir, fmt.Sprintf("%v-box-plot.png", queryType))
	return pngPath, errors.Trace(p.Save(vg.Length(80*len(datasetNames)), 200, pngPath))
}
