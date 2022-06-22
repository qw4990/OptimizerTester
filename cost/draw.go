package cost

import (
	"gonum.org/v1/plot/vg/draw"
	"math"
	"math/rand"
	"strings"

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
	p.X.Label.Text = "cost estimation"
	p.Y.Label.Text = "actual exec-time(ms)"
	fontSize := vg.Length(25)
	p.Title.TextStyle.Font.Size = fontSize
	p.X.Tick.Label.Font.Size = fontSize
	p.X.Label.TextStyle.Font.Size = fontSize
	p.Y.Label.TextStyle.Font.Size = fontSize
	p.Y.Tick.Label.Font.Size = fontSize

	var maxX, maxY float64
	for i := 0; i < r.Len(); i++ {
		x, y := r.XY(i)
		maxX = math.Max(maxX, x)
		maxY = math.Max(maxY, y)
	}
	p.X.Max = maxX * 1.5
	p.Y.Max = maxY * 1.2

	labledRecords := make(map[string]Records)
	for _, record := range r {
		labledRecords[record.Label] = append(labledRecords[record.Label], record)
	}

	for label, r := range labledRecords {
		s, err := plotter.NewScatter(r)
		if err != nil {
			panic(err)
		}
		s.GlyphStyle = getGlyPhStyleByLabel(label)
		p.Add(s)
		p.Legend.Add(label, s)
		p.Legend.TextStyle.Font.Size = fontSize
	}

	err := p.Save(800, 800, f)
	if err != nil {
		panic(err)
	}
}

func getGlyPhStyleByLabel(label string) (style draw.GlyphStyle) {
	style.Radius = 4
	switch strings.ToLower(label) {
	case "tablescan":
		style.Shape, style.Color = plotutil.DefaultGlyphShapes[5], plotutil.DarkColors[0]
	case "indexscan":
		style.Shape, style.Color = plotutil.DefaultGlyphShapes[5], plotutil.DarkColors[1]
	case "desctablescan":
		style.Shape, style.Color = plotutil.DefaultGlyphShapes[6], plotutil.DarkColors[2]
	case "descindexscan":
		style.Shape, style.Color = plotutil.DefaultGlyphShapes[6], plotutil.DarkColors[3]
	case "indexlookup", "lookup":
		style.Shape, style.Color = plotutil.DefaultGlyphShapes[3], plotutil.DarkColors[4]
	case "sort":
		style.Shape, style.Color = plotutil.DefaultGlyphShapes[1], plotutil.DarkColors[5]
	case "agg", "aggregation":
		style.Shape, style.Color = plotutil.DefaultGlyphShapes[2], plotutil.DarkColors[6]
	default:
		style.Color = plotutil.DarkColors[rand.Intn(len(plotutil.DarkColors))]
		style.Shape = plotutil.DefaultGlyphShapes[rand.Intn(len(plotutil.DefaultGlyphShapes))]
	}
	return
}
