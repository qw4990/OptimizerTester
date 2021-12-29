package cost

import (
	"fmt"
	"math"

	"gorgonia.org/gorgonia"
	"gorgonia.org/tensor"
)

func regressionCostFactors(rs CaliRecords) FactorVector {
	x, y := convert2XY(rs)
	g := gorgonia.NewGraph()
	xNode := gorgonia.NodeFromAny(g, x, gorgonia.WithName("x"))
	yNode := gorgonia.NodeFromAny(g, y, gorgonia.WithName("y"))

	fmt.Println("--->>> ", xNode.Value().Data(), yNode.Value().Data())

	costFactor := gorgonia.NewVector(g, gorgonia.Float64,
		gorgonia.WithName("cost-factor"),
		gorgonia.WithShape(xNode.Shape()[1]),
		gorgonia.WithInit(gorgonia.Uniform(0, 1)))

	pred := must(gorgonia.Mul(xNode, costFactor))
	var predicated gorgonia.Value
	gorgonia.Read(pred, &predicated)

	squaredError := must(gorgonia.Square(must(gorgonia.Sub(pred, yNode))))
	loss := must(gorgonia.Mean(squaredError))
	_, err := gorgonia.Grad(loss, costFactor)
	if err != nil {
		panic(fmt.Sprintf("Failed to backpropagate: %v", err))
	}

	solver := gorgonia.NewVanillaSolver(gorgonia.WithLearnRate(0.0000000000001))
	model := []gorgonia.ValueGrad{costFactor}

	machine := gorgonia.NewTapeMachine(g, gorgonia.BindDualValues(costFactor))
	defer machine.Close()

	fmt.Println("init theta: ", costFactor.Value())

	iter := 100
	for i := 0; i < iter; i++ {
		if err := machine.RunAll(); err != nil {
			panic(fmt.Sprintf("Error during iteration: %v: %v\n", i, err))
		}

		if err := solver.Step(model); err != nil {
			panic(err)
		}

		machine.Reset()
		fmt.Printf("theta: %v, Iter: %v Loss: %v, Pred: %v Accuracy: %v \n",
			costFactor.Value(),
			i,
			loss.Value(),
			predicated.Data(),
			accuracy(predicated.Data().([]float64), yNode.Value().Data().([]float64)))
	}

	return FactorVector{}
}

func convert2XY(rs CaliRecords) (*tensor.Dense, *tensor.Dense) {
	by := make([]float64, 0, len(rs))
	for _, r := range rs {
		by = append(by, r.TimeMS)
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

func accuracy(prediction, y []float64) float64 {
	var ok float64
	for i := 0; i < len(prediction); i++ {
		if math.Round(prediction[i]-y[i]) == 0 {
			ok += 1.0
		}
	}
	return ok / float64(len(y))
}
