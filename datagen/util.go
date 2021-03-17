package datagen

import (
	"bytes"
	"math/rand"
)

const timeLayout = "2006-01-02 15:04:05"

func prepareIntNDV(ndv int) []int {
	ints := make([]int, ndv)
	intMap := make(map[int]bool)
	for i := range ints {
		var ok bool
		var data int
		for {
			data = rand.Intn(int(2e9))
			_, ok = intMap[data];
			if !ok {
				break
			}
		}
		ints[i] = data
		intMap[data] = true
	}
	return ints
}

func prepareDoubleNDV(ndv int) []float64 {
	doubles := make([]float64, ndv)
	doubleMap := make(map[float64]bool)
	for i := range doubles {
		var ok bool
		var data float64
		for {
			data = rand.Float64() * 2e9
			_, ok = doubleMap[data]
			if !ok {
				break
			}
		}
		doubles[i] = data
		doubleMap[data] = true
	}
	return doubles
}


func uint2Str(v uint64) string {
	buf := new(bytes.Buffer)
	for v > 0 {
		buf.WriteByte(byte(uint64('a') + (v % 10)))
		v /= 10
	}
	return buf.String()
}
