package cost

import (
	"math/rand"
	
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func (r records) Len() int {
	return len(r)
}

func (r records) XY(k int) (x, y float64) {
	return r[k].cost, r[k].timeMS
}

func drawCostRecords(r records) {
	p, err := plot.New()
	if err != nil {
		panic(err)
	}
	p.Title.Text = "cost model accuracy scatter plot"
	p.X.Label.Text = "cost estimation"
	p.Y.Label.Text = "actual exec-time(ms)"

	labledRecords := make(map[string]records)
	for _, record := range r {
		labledRecords[record.label] = append(labledRecords[record.label], record)
	}

	for label, r := range labledRecords {
		s, err := plotter.NewScatter(r)
		if err != nil {
			panic(err)
		}
		s.GlyphStyle.Color = plotutil.DefaultColors[rand.Intn(len(plotutil.DefaultColors))]
		s.GlyphStyle.Shape = plotutil.DefaultGlyphShapes[rand.Intn(len(plotutil.DefaultGlyphShapes))]
		s.GlyphStyle.Radius = vg.Points(3)
		p.Add(s)
		p.Legend.Add(label, s)
	}

	err = p.Save(800, 800, "scatter.png")
	if err != nil {
		panic(err)
	}
}
