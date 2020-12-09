package datagen

import (
	"encoding/csv"
	"fmt"
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

		// Write UTF-8 BOM to prevent Chinese garbled
		if _, err := f.WriteString("\xEF\xBB\xBF"); err != nil {
			return err
		}
		w := csv.NewWriter(f)

		const maxVal = uint64(100000) // TODO: make it configurable
		r := rand.New(rand.NewSource(time.Now().Unix()))
		zipfx := rand.NewZipf(r, x, 2, maxVal)
		cols := make([]string, 0, 2)
		for i := 0; i < n; i++ {
			cols = cols[:0]
			c1, c2 := zipfx.Uint64(), zipfx.Uint64()
			switch tp {
			case TypeInt:
				cols = append(cols, strconv.FormatUint(c1, 10), strconv.FormatUint(c2, 10))
			case TypeDouble:
				cols = append(cols, strconv.FormatFloat(float64(c1), 'f', -1, 64), strconv.FormatFloat(float64(c2), 'f', -1, 64))
			case TypeDateTime:
				t1 := time.Unix(int64(c1), 0)
				t2 := time.Unix(int64(c2), 0)
				cols = append(cols, t1.Format("2006-01-02 15:04:05"), t2.Format("2006-01-02 15:04:05"))
			case TypeString:
				cols = append(cols, fmt.Sprintf("\"%v\"", c1), fmt.Sprintf("\"%v\"", c2))
			}
			if err := w.Write(cols); err != nil {
				return err
			}
		}
		w.Flush()
	}
	return nil
}

func GenZipfXSchema(dir string) error {
	content := `
	CREATE TABLE tint ( a INT, b INT, KEY(a), KEY(a, b) );
	CREATE TABLE tdouble ( a DOUBLE, b DOUBLE, KEY(a), KEY(a, b) );
	CREATE TABLE tstring ( a VARCHAR(32), b VARCHAR(32), KEY(a), KEY(a, b) );
	CREATE TABLE tdatetime (a DATETIME, b DATATIME, KEY(a), KEY(a, b));`

	schemaFile := path.Join(dir, "zipfx_schema.sql")
	return ioutil.WriteFile(schemaFile, []byte(content), 0666)
}
