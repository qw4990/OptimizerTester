package cost

import (
	"fmt"
	"gorgonia.org/gorgonia"
	"gorgonia.org/tensor"
)

func normalization(rs CaliRecords) (ret CaliRecords) {
	minY, maxY := rs[0].TimeNS, rs[0].TimeNS
	for _, r := range rs {
		if r.TimeNS < minY {
			minY = r.TimeNS
		}
		if r.TimeNS > maxY {
			maxY = r.TimeNS
		}
	}

	for _, r := range rs {
		//r.TimeNS = (r.TimeNS - minY) / (maxY - minY)
		r.TimeNS /= 1e6
		for i := range r.Weights {
			r.Weights[i] /= 1e6
		}
		fmt.Println("Record>> ", r.Label, r.SQL, r.Weights.String(), r.Cost, r.TimeNS)
		ret = append(ret, r)
	}
	return
}

func regressionCostFactors(rs CaliRecords) CostFactors {
	rs = normalization(rs)
	x, y := convert2XY(rs)
	g := gorgonia.NewGraph()
	xNode := gorgonia.NodeFromAny(g, x, gorgonia.WithName("x"))
	yNode := gorgonia.NodeFromAny(g, y, gorgonia.WithName("y"))

	costFactor := gorgonia.NewVector(g, gorgonia.Float64,
		gorgonia.WithName("cost-factor"),
		gorgonia.WithShape(xNode.Shape()[1]),
		gorgonia.WithInit(func(dt tensor.Dtype, s ...int) interface{} {
			switch dt {
			case tensor.Float64: // (CPU, CopCPU, Net, Scan, DescScan, Mem)
				return []float64{430, 430, 4, 120, 180, 1}
			default:
				panic("invalid type")
			}
			return nil
		}))
	//gorgonia.WithInit(gorgonia.Zeroes()))
	//gorgonia.WithInit(gorgonia.Uniform(0, 300)))
	//strictFactor, err := gorgonia.LeakyRelu(costFactor, 0)
	//if err != nil {
	//	panic(err)
	//}

	pred := must(gorgonia.Mul(xNode, costFactor))
	var predicated gorgonia.Value
	gorgonia.Read(pred, &predicated)

	diff := must(gorgonia.Abs(must(gorgonia.Sub(pred, yNode))))
	relativeDiff := must(gorgonia.Div(diff, yNode))
	loss := must(gorgonia.Mean(relativeDiff))
	_, err := gorgonia.Grad(loss, costFactor)
	if err != nil {
		panic(fmt.Sprintf("Failed to backpropagate: %v", err))
	}

	solver := gorgonia.NewAdamSolver(gorgonia.WithLearnRate(0.01))
	model := []gorgonia.ValueGrad{costFactor}

	machine := gorgonia.NewTapeMachine(g, gorgonia.BindDualValues(costFactor))
	defer machine.Close()

	fmt.Println("init theta: ", costFactor.Value())

	iter := 100000
	for i := 0; i < iter; i++ {
		if err := machine.RunAll(); err != nil {
			panic(fmt.Sprintf("Error during iteration: %v: %v\n", i, err))
		}

		if err := solver.Step(model); err != nil {
			panic(err)
		}

		machine.Reset()
		lossV := loss.Value().Data().(float64)
		if i%1000 == 0 {
			fmt.Printf("theta: %v, Iter: %v Loss: %.4f%%\n",
				costFactor.Value(),
				i,
				lossV*100)
		}
	}

	var fv CostFactors
	for i := range fv {
		fv[i] = costFactor.Value().Data().([]float64)[i]
	}

	return fv
}

func convert2XY(rs CaliRecords) (*tensor.Dense, *tensor.Dense) {
	by := make([]float64, 0, len(rs))
	for _, r := range rs {
		by = append(by, r.TimeNS)
	}
	y := tensor.New(tensor.WithShape(len(rs)), tensor.WithBacking(by))

	bx := make([]float64, 0, len(rs)*len(rs[0].Weights))
	for _, r := range rs {
		for _, w := range r.Weights {
			bx = append(bx, w)
		}
	}
	x := tensor.New(tensor.WithShape(len(rs), len(rs[0].Weights)), tensor.WithBacking(bx))

	return x, y
}

func must(n *gorgonia.Node, err error) *gorgonia.Node {
	if err != nil {
		panic(err)
	}
	return n
}

func one(size int) []float64 {
	one := make([]float64, size)
	for i := 0; i < size; i++ {
		one[i] = 1.0
	}
	return one
}
