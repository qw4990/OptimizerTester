package cost

import (
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"image/color"
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

	s, err := plotter.NewScatter(r)
	if err != nil {
		panic(err)
	}
	s.GlyphStyle.Color = color.RGBA{R: 255, B: 128, A: 255}
	s.GlyphStyle.Radius = vg.Points(1)
	p.Add(s)

	err = p.Save(800, 800, "scatter.png")
	if err != nil {
		panic(err)
	}
}
