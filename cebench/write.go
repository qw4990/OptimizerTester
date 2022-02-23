package cebench

import (
	"bytes"
	"fmt"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/font"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"image/color"
	"io"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type LogScale struct{}

func (LogScale) Normalize(min, max, x float64) float64 {
	if min < 0 || max < 0 || x < 0 {
		panic("Values must be greater than 0 for a log scale.")
	}
	logMin := math.Log(min + 1)
	return (math.Log(x+1) - logMin) / (math.Log(max+1) - logMin)
}

type LogTicks struct {
}

// Ticks returns Ticks in a specified range
func (t LogTicks) Ticks(min, max float64) []plot.Tick {
	if min < 0 || max < 0 {
		panic("Values must be greater than 0 for a log scale.")
	}
	if min == 0 {
		min = 1
	}
	if max == 0 {
		max = 1
	}

	val := math.Pow(5, float64(int(math.Log(min)/math.Log(5))))
	max = math.Pow(5, math.Ceil(math.Log(max)/math.Log(5)))
	var ticks []plot.Tick
	ticks = append(ticks, plot.Tick{Value: 0, Label: "0"})
	ticks = append(ticks, plot.Tick{Value: 1, Label: "1"})
	for val < max {
		ticks = append(ticks, plot.Tick{Value: val * 2})
		ticks = append(ticks, plot.Tick{Value: val * 3})
		ticks = append(ticks, plot.Tick{Value: val * 4})
		val *= 5
		ticks = append(ticks, plot.Tick{Value: val, Label: strconv.FormatFloat(val, 'g', -1, 64)})
	}

	return ticks
}

func WriteToFileForInfos(infos EstInfos, tp, outDir string, writer io.Writer) {
	// Title
	_, err := writer.Write([]byte(fmt.Sprintf("\n## %s\n", tp)))
	if err != nil {
		panic(err)
	}

	// Bar chart
	chartFileName := strings.ReplaceAll(fmt.Sprintf(chartFileTemplate, tp), " ", "-")
	chartPath := filepath.Join(outDir, chartFileName)
	plot.DefaultFont.Variant = "Sans"
	p := plot.New()
	chartVals := distribution(infos)
	bar, err := plotter.NewBarChart(plotter.Values(chartVals), vg.Points(25))
	if err != nil {
		panic(err)
	}
	bar.Color = color.RGBA{R: 61, G: 63, B: 234}
	bar.LineStyle.Width = vg.Points(1)
	grid := plotter.NewGrid()
	grid.Vertical.Width = 0
	p.Add(bar, grid)
	p.NominalX(xAxisNames...)
	p.X.Tick.Label.Rotation = 0.4
	p.X.Tick.Label.YAlign = -1.6
	p.X.Tick.Label.XAlign = -0.7
	p.X.Label.Text = "p-error range (negative for underestimation, positive for overestimation)"
	p.Y.Label.Text = "count"
	p.Title.Text = "p-error distribution"
	p.Title.Padding = vg.Points(5)
	p.Y.Scale = LogScale{}
	p.Y.Tick.Marker = LogTicks{}
	p.Y.Padding = vg.Points(5)
	p.Title.TextStyle.Font = font.From(plot.DefaultFont, 16)
	p.X.Tick.Label.Font = font.From(plot.DefaultFont, 14)
	p.X.Label.TextStyle.Font = font.From(plot.DefaultFont, 14)
	p.Y.Tick.Label.Font = font.From(plot.DefaultFont, 14)
	p.Y.Label.TextStyle.Font = font.From(plot.DefaultFont, 14)
	err = p.Save(vg.Points(1000), vg.Points(350), chartPath)
	if err != nil {
		panic(err)
	}

	_, err = writer.Write([]byte(fmt.Sprintf("![chart](%s)\n", chartFileName)))
	if err != nil {
		panic(err)
	}

	// Form
	tmpInfos := make([]*EstInfo, len(infos))
	copy(tmpInfos, infos)
	sort.Slice(tmpInfos, func(i, j int) bool {
		iError := tmpInfos[i].pError
		jError := tmpInfos[j].pError
		return math.Abs(iError) < math.Abs(jError)
	})

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
	sort.Sort(sort.Reverse(underEstInfos))
	WriteOverOrUnderStats(tmpInfos, underEstInfos, overEstInfos, exactCnt, writer)
}

func WriteOverOrUnderStats(infos, underEstInfos, overEstInfos EstInfos, exactEstCnt int, writer io.Writer) {
	str := bytes.Buffer{}
	defer func() {
		_, err := str.WriteTo(writer)
		if err != nil {
			panic(err)
		}
	}()

	str.WriteString("\n|       | Count | P50 | P90 | P95 | P99 | Max |\n")
	str.WriteString("| ---- | ---- | ---- | ---- | ---- | ---- | ---- |\n")
	if len(infos) == 0 {
		str.WriteString("| Overall | 0 | - | - | - | - | - |\n")
	} else {
		n := len(infos)
		str.WriteString(fmt.Sprintf("| Overall | %d | %.3f | %.3f | %.3f | %.3f | %.3f |\n",
			n,
			math.Abs(infos[(n*50)/100].pError),
			math.Abs(infos[(n*90)/100].pError),
			math.Abs(infos[(n*95)/100].pError),
			math.Abs(infos[(n*99)/100].pError),
			math.Abs(infos[n-1].pError)))
	}
	str.WriteString(fmt.Sprintf("| Exact estimation | %d | 0 | 0 | 0 | 0 | 0 |\n", exactEstCnt))
	if len(overEstInfos) == 0 {
		str.WriteString("| Overestimation | 0 | - | - | - | - | - |\n")
	} else {
		n := len(overEstInfos)
		str.WriteString(fmt.Sprintf("| Overestimation | %d | %.3f | %.3f | %.3f | %.3f | %.3f |\n",
			n,
			math.Abs(overEstInfos[(n*50)/100].pError),
			math.Abs(overEstInfos[(n*90)/100].pError),
			math.Abs(overEstInfos[(n*95)/100].pError),
			math.Abs(overEstInfos[(n*99)/100].pError),
			math.Abs(overEstInfos[n-1].pError)))
	}
	if len(underEstInfos) == 0 {
		str.WriteString("| Underestimation | 0 | - | - | - | - | - |\n")
	} else {
		n := len(underEstInfos)
		str.WriteString(fmt.Sprintf("| Underestimation | %d | %.3f | %.3f | %.3f | %.3f | %.3f |\n",
			n,
			math.Abs(underEstInfos[(n*50)/100].pError),
			math.Abs(underEstInfos[(n*90)/100].pError),
			math.Abs(underEstInfos[(n*95)/100].pError),
			math.Abs(underEstInfos[(n*99)/100].pError),
			math.Abs(underEstInfos[n-1].pError)))
	}
}

func WriteWorstN(infos EstInfos, writer io.Writer, n int) {
	str := bytes.Buffer{}
	tmpInfos := make([]*EstInfo, len(infos))
	copy(tmpInfos, infos)
	sort.Slice(tmpInfos, func(i, j int) bool {
		iError := tmpInfos[i].pError
		jError := tmpInfos[j].pError
		return math.Abs(iError) < math.Abs(jError)
	})
	if len(tmpInfos) < n {
		n = len(tmpInfos)
	}
	str.WriteString("\n| Type | Expr | Table | Est | Actual | PError |\n")
	str.WriteString("| ---- | ---- | ---- | ---- | ---- | ---- |\n")
	for i := 0; i < n; i++ {
		info := tmpInfos[len(tmpInfos)-i-1]
		str.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d | %.3f |\n", info.Type, info.Expr, info.TableName, info.Est, info.Actual, info.pError))
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
	str.WriteString("\n| Type | Expr | Table | Est | Actual | PError |\n")
	str.WriteString("| ---- | ---- | ---- | ---- | ---- | ---- |\n")
	for _, info := range infos {
		if math.Abs(info.pError) < threshold {
			break
		}
		str.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d | %.3f |\n", info.Type, info.Expr, info.TableName, info.Est, info.Actual, info.pError))
	}
	_, err := str.WriteTo(writer)
	if err != nil {
		panic(err)
	}
}

func WriteCompareToFileForInfos(infosSlice []EstInfos, labels []string, tp, outDir string, writer io.Writer) {
	// Title
	_, err := writer.Write([]byte(fmt.Sprintf("\n\n## %s\n", tp)))
	if err != nil {
		panic(err)
	}

	// Bar chart
	chartFileName := strings.ReplaceAll(fmt.Sprintf(chartFileTemplate, tp), " ", "-")
	chartPath := filepath.Join(outDir, chartFileName)
	plot.DefaultFont.Variant = "Sans"
	p := plot.New()
	n := float64(len(infosSlice))
	width := 40.0 / n
	head := -(n - 1) / 2.0 * width
	colors := []color.RGBA{
		{R: 61, G: 63, B: 234},
		{R: 1, G: 174, B: 239},
		{R: 23, G: 45, B: 114},
		{R: 249, G: 116, B: 108},
		{R: 105, G: 195, B: 132},
		{R: 206, G: 159, B: 55},
	}
	for i, infos := range infosSlice {
		chartVals := distribution(infos)
		bar, err := plotter.NewBarChart(plotter.Values(chartVals), vg.Points(width))
		if err != nil {
			panic(err)
		}
		bar.Color = colors[i%len(colors)]
		bar.LineStyle.Width = vg.Points(1)
		bar.LineStyle.Color = color.RGBA{R: 44, G: 44, B: 44}
		bar.Offset = vg.Points(head + float64(i)*width)
		p.Add(bar)
		p.Legend.Add(labels[i], bar)
	}
	grid := plotter.NewGrid()
	grid.Vertical.Width = 0
	p.Add(grid)
	p.NominalX(xAxisNames...)
	p.X.Tick.Label.Rotation = 0.4
	p.X.Tick.Label.YAlign = -1.6
	p.X.Tick.Label.XAlign = -0.7
	p.X.Label.Text = "p-error range (negative for underestimation, positive for overestimation)"
	p.Y.Label.Text = "count"
	p.Title.Text = "p-error distribution"
	p.Title.Padding = vg.Points(5)
	p.Y.Scale = LogScale{}
	p.Y.Tick.Marker = LogTicks{}
	p.Y.Padding = vg.Points(5)
	p.Title.TextStyle.Font = font.From(plot.DefaultFont, 16)
	p.X.Tick.Label.Font = font.From(plot.DefaultFont, 14)
	p.X.Label.TextStyle.Font = font.From(plot.DefaultFont, 14)
	p.Y.Tick.Label.Font = font.From(plot.DefaultFont, 14)
	p.Y.Label.TextStyle.Font = font.From(plot.DefaultFont, 14)
	p.Legend.Top = true
	p.Legend.TextStyle.Font = font.From(plot.DefaultFont, 14)
	p.Legend.XOffs = vg.Points(-10)
	p.Legend.Padding = vg.Points(3)
	err = p.Save(vg.Points(1000), vg.Points(350), chartPath)
	if err != nil {
		panic(err)
	}

	_, err = writer.Write([]byte(fmt.Sprintf("![chart](%s)\n", chartFileName)))
	if err != nil {
		panic(err)
	}

	// Form
	var allEstInfosSlice, overEstInfosSlice, underEstInfosSlice []EstInfos
	var exactCntSlice []int
	for _, infos := range infosSlice {
		allInfos := make([]*EstInfo, len(infos))
		copy(allInfos, infos)
		sort.Slice(allInfos, func(i, j int) bool {
			iError := allInfos[i].pError
			jError := allInfos[j].pError
			return math.Abs(iError) < math.Abs(jError)
		})

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
		sort.Sort(sort.Reverse(underEstInfos))
		allEstInfosSlice = append(allEstInfosSlice, allInfos)
		overEstInfosSlice = append(overEstInfosSlice, overEstInfos)
		underEstInfosSlice = append(underEstInfosSlice, underEstInfos)
		exactCntSlice = append(exactCntSlice, exactCnt)
	}
	WriteCompareOverOrUnderStats(allEstInfosSlice, underEstInfosSlice, overEstInfosSlice, labels, exactCntSlice, writer)
}

func WriteCompareOverOrUnderStats(infosSlice, underEstInfosSlice, overEstInfosSlice []EstInfos, labels []string, exactEstCntSlice []int, writer io.Writer) {
	str := bytes.Buffer{}
	defer func() {
		_, err := str.WriteTo(writer)
		if err != nil {
			panic(err)
		}
	}()

	str.WriteString("\n<table>")
	str.WriteString("\n<thead>")
	str.WriteString("\n<tr><th></th><th>Label</th><th>Count</th><th>P50</th><th>P90</th><th>P95</th><th>P99</th><th>Max</th></tr>")
	str.WriteString("\n</thead>")
	str.WriteString("\n<tbody>")
	for i, infos := range infosSlice {
		str.WriteString("\n<tr>")
		if i == 0 {
			str.WriteString(fmt.Sprintf("<td rowspan=%d>Overall</td>", len(infosSlice)))
		}
		if len(infos) == 0 {
			str.WriteString(fmt.Sprintf("<td>%s</td><td>0</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>", labels[i]))
		} else {
			n := len(infos)
			str.WriteString(fmt.Sprintf("<td>%s</td><td>%d</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td>",
				labels[i],
				n,
				math.Abs(infos[(n*50)/100].pError),
				math.Abs(infos[(n*90)/100].pError),
				math.Abs(infos[(n*95)/100].pError),
				math.Abs(infos[(n*99)/100].pError),
				math.Abs(infos[n-1].pError)))
		}
		str.WriteString("</tr>")
	}
	for i, exactEstCnt := range exactEstCntSlice {
		str.WriteString("\n<tr>")
		if i == 0 {
			str.WriteString(fmt.Sprintf("<td rowspan=%d>Exact estimation</td>", len(infosSlice)))
		}
		str.WriteString(fmt.Sprintf("<td>%s</td><td>%d</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>", labels[i], exactEstCnt))
		str.WriteString("</tr>")
	}
	for i, overEstInfos := range overEstInfosSlice {
		str.WriteString("\n<tr>")
		if i == 0 {
			str.WriteString(fmt.Sprintf("<td rowspan=%d>Overestimation</td>", len(infosSlice)))
		}
		if len(overEstInfos) == 0 {
			str.WriteString(fmt.Sprintf("<td>%s</td><td>0</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>", labels[i]))
		} else {
			n := len(overEstInfos)
			str.WriteString(fmt.Sprintf("<td>%s</td><td>%d</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td>",
				labels[i],
				n,
				math.Abs(overEstInfos[(n*50)/100].pError),
				math.Abs(overEstInfos[(n*90)/100].pError),
				math.Abs(overEstInfos[(n*95)/100].pError),
				math.Abs(overEstInfos[(n*99)/100].pError),
				math.Abs(overEstInfos[n-1].pError)))
		}
		str.WriteString("</tr>")
	}
	for i, underEstInfos := range underEstInfosSlice {
		str.WriteString("\n<tr>")
		if i == 0 {
			str.WriteString(fmt.Sprintf("<td rowspan=%d>Underestimation</td>", len(infosSlice)))
		}
		if len(underEstInfos) == 0 {
			str.WriteString(fmt.Sprintf("<td>%s</td><td>0</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>", labels[i]))
		} else {
			n := len(underEstInfos)
			str.WriteString(fmt.Sprintf("<td>%s</td><td>%d</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td>",
				labels[i],
				n,
				math.Abs(underEstInfos[(n*50)/100].pError),
				math.Abs(underEstInfos[(n*90)/100].pError),
				math.Abs(underEstInfos[(n*95)/100].pError),
				math.Abs(underEstInfos[(n*99)/100].pError),
				math.Abs(underEstInfos[n-1].pError)))
		}
		str.WriteString("</tr>")
	}
	str.WriteString("\n</tbody>")
	str.WriteString("\n</table>")
}
