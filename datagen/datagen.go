package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type DATAType int

const totTypeNum = 4

const (
	TypeInt DATAType = iota
	TypeDouble
	TypeDateTime
	TypeString
)

var (
	typeNameMap = map[DATAType]string{
		TypeInt:      "int",
		TypeDouble:   "double",
		TypeDateTime: "datetime",
		TypeString:   "string",
	}
)

func main() {
	var (
		x       = float64(3)
		numRows = int64(10000)
		err     error
	)
	for idx, arg := range os.Args[1:] {
		if idx == 1 {
			x, err = strconv.ParseFloat(arg, 64)
			if err != nil {
				panic(err)
			}
		} else if idx == 2 {
			numRows, err = strconv.ParseInt(arg, 10, 64)
			if err != nil {
				panic(err)
			}
		}
	}
	ZipfXDataGen(x, numRows)
}

func ZipfXDataGen(x float64, numRows int64) {
	maxVal := uint64(100000)

	r := rand.New(rand.NewSource(time.Now().Unix()))
	ZipfX := rand.NewZipf(r, x, 2, maxVal)
	for typeIdx := 0; typeIdx < totTypeNum; typeIdx++ {
		dataType := DATAType(typeIdx)
		CSVFileName := "./datagen/testdata/" + typeNameMap[dataType] + "_table.csv"

		// open CSV file to store test data
		f, err := os.OpenFile(CSVFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		// Write UTF-8 BOM to prevent Chinese garbled
		f.WriteString("\xEF\xBB\xBF")
		w := csv.NewWriter(f)
		insertValue := make([]string, 0, 2)
		for cnt := int64(0); cnt < numRows; cnt++ {
			insertValue = insertValue[:0]
			tmpVal1 := ZipfX.Uint64()
			tmpVal2 := ZipfX.Uint64()
			switch dataType {
			case TypeInt:
				insertValue = append(insertValue, strconv.FormatUint(tmpVal1, 10), strconv.FormatUint(tmpVal2, 10))
			case TypeDouble:
				insertValue = append(insertValue, strconv.FormatFloat(float64(tmpVal1), 'f', -1, 64), strconv.FormatFloat(float64(tmpVal2), 'f', -1, 64))
			case TypeDateTime:
				t1 := time.Unix(int64(tmpVal1), 0)
				t2 := time.Unix(int64(tmpVal2), 0)
				insertValue = append(insertValue, t1.Format("2006-01-02 15:04:05"), t2.Format("2006-01-02 15:04:05"))
			case TypeString:
				insertValue = append(insertValue, fmt.Sprintf("\"%d\"", tmpVal1), fmt.Sprintf("\"%d\"", tmpVal2))
			}
			w.Write(insertValue)
		}
		w.Flush()
	}
}
