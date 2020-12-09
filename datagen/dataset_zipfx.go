package datagen

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/pingcap/errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"time"
)

// Please see https://docs.google.com/document/d/1ynUQsaFsOUhr7Zp_Ke0aexe1r688GpFjFh4_D1ihy4w/edit# 
// to get more details about ZipfX dataset.

func GenZipfXData(x float64, n int, dir string) error {
	if err := GenZipfXSchema(dir); err != nil {
		return err
	}

	tbNames := []string{"tint", "tdouble", "tstring", "tdatetime"}
	for tbIdx, tp := range []DATAType{TypeInt, TypeDouble, TypeString, TypeDateTime} {
		tb := tbNames[tbIdx]
		csvFile := path.Join(dir, fmt.Sprintf("zipfx_%v.csv", tb))
		f, err := os.OpenFile(csvFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		w := csv.NewWriter(f)

		const maxVal = uint64(100000) // TODO: make it configurable
		r := rand.New(rand.NewSource(time.Now().Unix()))
		zipfx := rand.NewZipf(r, x, 2, maxVal)
		cols := make([]string, 0, 2)

		intFactor := uint64(r.Intn(1000)) + 1
		doubleFactor := float64(r.Intn(1000))
		const layout = "2006-01-02 15:04:05"
		datetimeFactor, _ := time.Parse(layout, "2010-01-01 00:00:00")
		datetimeFactor = datetimeFactor.Add(time.Hour * time.Duration(r.Intn(1000)))
		strFactor := uint64(r.Intn(10000)) + 1
		for i := 0; i < n; i++ {
			cols = cols[:0]
			c1, c2 := zipfx.Uint64(), zipfx.Uint64()
			switch tp {
			case TypeInt:
				cols = append(cols, strconv.FormatUint(intFactor+c1, 10), strconv.FormatUint(intFactor+c2, 10))
			case TypeDouble:
				cols = append(cols, strconv.FormatFloat(doubleFactor/float64(c1+1), 'f', 4, 64),
					strconv.FormatFloat(doubleFactor/float64(c2+1), 'f', 4, 64))
			case TypeDateTime:
				t1 := datetimeFactor.Add(time.Second * time.Duration(c1))
				t2 := datetimeFactor.Add(time.Second * time.Duration(c2))
				cols = append(cols, t1.Format(layout), t2.Format(layout))
			case TypeString:
				cols = append(cols, uint2Str(c1+strFactor), uint2Str(c2+strFactor))
			}
			if err := w.Write(cols); err != nil {
				return err
			}
		}
		w.Flush()
	}
	return GenZipfXLoadSQL(dir)
}

func uint2Str(v uint64) string {
	buf := new(bytes.Buffer)
	for v > 0 {
		buf.WriteByte(byte(uint64('a') + (v % 10)))
		v /= 10
	}
	return buf.String()
}

func GenZipfXSchema(dir string) error {
	content := `CREATE TABLE tint ( a INT, b INT, KEY(a), KEY(a, b) );
CREATE TABLE tdouble ( a DOUBLE, b DOUBLE, KEY(a), KEY(a, b) );
CREATE TABLE tstring ( a VARCHAR(32), b VARCHAR(32), KEY(a), KEY(a, b) );
CREATE TABLE tdatetime (a DATETIME, b DATETIME, KEY(a), KEY(a, b));
`
	schemaFile := path.Join(dir, "zipfx_schema.sql")
	return ioutil.WriteFile(schemaFile, []byte(content), 0666)
}

// load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/OptimizerTester/datagen/test/zipfx_tint.csv' into table tint;
func GenZipfXLoadSQL(dir string) error {
	var buf bytes.Buffer
	tbNames := []string{"tint", "tdouble", "tstring", "tdatetime"}

	if !path.IsAbs(dir) {
		absPrefix, err := os.Getwd()
		if err != nil {
			return errors.Trace(err)
		}
		dir = path.Join(absPrefix, dir)
	}

	for _, tb := range tbNames {
		csvFile := path.Join(dir, fmt.Sprintf("zipfx_%v.csv", tb))
		buf.WriteString(fmt.Sprintf("LOAD DATA LOCAL INFILE '%v' INTO TABLE %v FIELDS TERMINATED BY ',';\n", csvFile, tb))
	}

	loadFile := path.Join(dir, "zipfx_load.sql")
	return ioutil.WriteFile(loadFile, buf.Bytes(), 0666)
}
