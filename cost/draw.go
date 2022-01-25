package cost

import (
	"math/rand"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func (r Records) Len() int {
	return len(r)
}

func (r Records) XY(k int) (x, y float64) {
	return r[k].Cost, r[k].TimeMS
}

func drawCostRecordsTo(r Records, f string) {
	p := plot.New()
	p.Title.Text = "cost model accuracy scatter plot"
	p.Title.TextStyle.Font.Size = 20
	p.X.Label.Text = "cost estimation"
	p.X.Tick.Label.Font.Size = 20
	p.Y.Tick.Label.Font.Size = 20
	p.X.Label.TextStyle.Font.Size = 20
	p.Y.Label.Text = "actual exec-time(ms)"
	p.Y.Label.TextStyle.Font.Size = 20

	labledRecords := make(map[string]Records)
	for _, record := range r {
		labledRecords[record.Label] = append(labledRecords[record.Label], record)
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
		p.Legend.TextStyle.Font.Size = 20
	}

	err := p.Save(800, 800, f)
	if err != nil {
		panic(err)
	}
}
