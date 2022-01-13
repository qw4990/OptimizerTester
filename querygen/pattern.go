package querygen

import (
	"math/rand"
	"strings"
)

type pattern struct {
	cols []*colPattern
}

func (p *pattern) generate() string {
	exprs := make([]string, 0, len(p.cols))
	for _, col := range p.cols {
		exprs = append(exprs, "("+col.generate()+")")
	}
	return strings.Join(exprs, " and ")
}

type colPattern struct {
	col *column
	tp  exprType
}

func (cp *colPattern) generate() string {
	if cp.tp == equal {
		return cp.generateEqual()
	} else if cp.tp == interval {
		return cp.generateInterval()
	}
	return ""
}

func (cp *colPattern) generateEqual() string {
	vals := cp.col.RandVals
	val1 := vals[rand.Intn(len(vals))]
	r := rand.Float64()
	n := cp.col.Name
	if r < 0.5 {
		return n + " = " + val1
	}
	val2 := vals[rand.Intn(len(vals))]
	val3 := vals[rand.Intn(len(vals))]
	if r < 0.75 {
		return n + " = " + val1 + " or " + n + " = " + val2 + " or " + n + " = " + val3
	}
	if r < 0.95 {
		return n + " in(" + val1 + "," + val2 + "," + val3 + ")"
	}
	return n + " is null"
}

func (cp *colPattern) generateInterval() string {
	vals := cp.col.RandVals
	val1 := vals[rand.Intn(len(vals))]
	r := rand.Float64()
	n := cp.col.Name
	if r < 0.05 {
		return n + " > " + cp.col.Max
	}
	if r < 0.1 {
		return n + " > " + cp.col.Min
	}
	if r < 0.15 {
		return n + " < " + cp.col.Max
	}
	if r < 0.2 {
		return n + " < " + cp.col.Min
	}
	if r < 0.4 {
		return n + " < " + val1
	}
	if r < 0.6 {
		return n + " > " + val1
	}
	rIdx1, rIdx2 := rand.Intn(len(vals)), rand.Intn(len(vals))
	if rIdx1 > rIdx2 {
		rIdx1, rIdx2 = rIdx2, rIdx1
	}
	return n + " > " + vals[rIdx1] + " and " + n + " < " + vals[rIdx2]
}

type exprType int

const (
	invalid exprType = iota
	equal
	interval
)
