package datagen

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
)

type PartType string

const (
	HashPart  PartType = "hash"
	RangePart PartType = "range"
)

type tableMeta struct {
	name     string
	dataType DATAType
	parCol   string
	parType  PartType
}

var tables = []tableMeta{
	{
		name:     "p_hash_zint_a",
		dataType: TypeInt,
		parCol:   "a",
		parType:  HashPart,
	},
	{
		name:     "p_hash_zint_c",
		dataType: TypeInt,
		parCol:   "c",
		parType:  HashPart,
	},
	{
		name:     "p_range_zint_a",
		dataType: TypeInt,
		parCol:   "a",
		parType:  RangePart,
	},
	{
		name:     "p_range_zint_c",
		dataType: TypeInt,
		parCol:   "c",
		parType:  RangePart,
	},
	{
		name:     "p_range_zdouble_a",
		dataType: TypeDouble,
		parCol:   "a",
		parType:  RangePart,
	},
	{
		name:     "p_range_zdouble_c",
		dataType: TypeDouble,
		parCol:   "c",
		parType:  RangePart,
	},
	{
		name:     "p_range_zstring_a",
		dataType: TypeString,
		parCol:   "a",
		parType:  RangePart,
	},
	{
		name:     "p_range_zstring_c",
		dataType: TypeString,
		parCol:   "c",
		parType:  RangePart,
	},
	{
		name:     "p_range_zdatetime_a",
		dataType: TypeDateTime,
		parCol:   "a",
		parType:  RangePart,
	},
	{
		name:     "p_range_zdatetime_c",
		dataType: TypeDateTime,
		parCol:   "c",
		parType:  RangePart,
	},
}

type pZipfXOpt struct {
	x            float64
	n            int64
	ndv          int64
	partitionNum int64
}

func parsePZipfXOpt(args string) (opt pZipfXOpt, err error) {
	kvs := strings.Split(args, ",")
	for _, kv := range kvs {
		tmp := strings.Split(kv, "=")
		if len(tmp) != 2 {
			return opt, errors.Errorf("invalide kv=%v", kv)
		}
		k, v := tmp[0], tmp[1]
		switch strings.ToLower(k) {
		case "x":
			if opt.x, err = strconv.ParseFloat(v, 64); err != nil {
				return opt, errors.Errorf("invalid x=%v", v)
			}
		case "n":
			if opt.n, err = strconv.ParseInt(v, 10, 64); err != nil {
				return opt, errors.Errorf("invalid n=%v", v)
			}
		case "ndv":
			if opt.ndv, err = strconv.ParseInt(v, 10, 64); err != nil {
				return opt, errors.Errorf("invalid ndv=%v", v)
			}
		case "partition_num":
			if opt.partitionNum, err = strconv.ParseInt(v, 10, 64); err != nil {
				return opt, errors.Errorf("invalid partition_num=%v", v)
			}
		}
	}
	return
}

func genPartitionSchema(t tableMeta, opt pZipfXOpt, ints []int, doubles []float64) (string, error) {

	content := fmt.Sprintf("CREATE TABLE %s (a %s, b %s, c %s, key(a), key(a, b), key(b), key(b, c))\n",
		t.name,
		t.dataType,
		t.dataType,
		t.dataType)

	switch t.parType {
	case HashPart:
		content = fmt.Sprintf("%s PARTITION BY HASH(%s) PARTITION %d;", content, t.parCol, opt.partitionNum)
	case RangePart:
		content = fmt.Sprintf("%s PARTITION BY RANGE(%s) (\n", content, t.parCol)

		begin, _ := time.Parse(timeLayout, "2000-01-01 00:00:00")
		end, _ := time.Parse(timeLayout, "2100-01-01 00:00:00")
		interval := end.Sub(begin)

		var i int64
		for i = 0; i < opt.partitionNum-1; i++ {
			var v string
			switch t.dataType {
			case TypeInt:
				v = strconv.FormatUint(uint64(ints[int64(len(ints))*i/opt.partitionNum]), 10)
			case TypeDouble:
				v = strconv.FormatFloat(doubles[int64(len(doubles))*i/opt.partitionNum], 'f', 4, 64)
			case TypeDateTime:
				t := begin.Add(interval * time.Duration(i) / time.Duration(opt.partitionNum))
				v = t.Format(timeLayout)
			case TypeString:
				v = uint2Str(uint64(i + 10000))
			}
			content = fmt.Sprintf("%s PARTITION p%d VALUES LESS THAN (%s),\n", content, i, v)
		}
		content = fmt.Sprintf("%s PARTITION p%d VALUES LESS THAN (MAXVALUE));", content, i)
	default:
		return "", errors.Errorf("unknown partition type")
	}
	return content, nil
}

func GenPZipfXSchema(dir string, opt pZipfXOpt, ints []int, doubles []float64) error {
	var content string

	for _, table := range tables {
		s, err := genPartitionSchema(table, opt, ints, doubles)
		if err != nil {
			return err
		}
		content = fmt.Sprintf("%s%s\n", content, s)
	}

	schemaFile := path.Join(dir, "pzipfx_schema.sql")
	return ioutil.WriteFile(schemaFile, []byte(content), 0666)
}

func GenPZipfXData(args, dir string) error {
	opt, err := parsePZipfXOpt(args)
	if err != nil {
		return err
	}

	num := opt.ndv
	if opt.partitionNum > opt.ndv {
		num = opt.partitionNum
	}
	ints := prepareIntNDV(int(num + 1))
	sortedInts := make([]int, len(ints))
	copy(sortedInts, ints)
	sort.Ints(sortedInts)

	doubles := prepareDoubleNDV(int(num + 1))
	sortedDoubles := make([]float64, len(doubles))
	copy(sortedDoubles, doubles)
	sort.Float64s(sortedDoubles)

	if err := GenPZipfXSchema(dir, opt, sortedInts, sortedDoubles); err != nil {
		return err
	}

	begin, _ := time.Parse(timeLayout, "2000-01-01 00:00:00")
	end, _ := time.Parse(timeLayout, "2100-01-01 00:00:00")
	interval := end.Sub(begin)
	datetimeFactor := interval / time.Duration(opt.ndv)

	colNum := 3
	for _, table := range tables {
		csvFile := path.Join(dir, fmt.Sprintf("%s.csv", table.name))
		f, err := os.OpenFile(csvFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		w := csv.NewWriter(f)
		r := rand.New(rand.NewSource(time.Now().Unix()))
		zips := make([]*rand.Zipf, 0, colNum)
		for i := 0; i < colNum; i++ {
			zips = append(zips, rand.NewZipf(r, opt.x, 2, uint64(opt.ndv)))
		}
		cols := make([]string, 0, colNum)

		strFactor := uint64(r.Intn(10000)) + 1

		for i := 0; i < int(opt.n); i++ {
			cols := cols[:0]
			for _, zip := range zips {
				c := zip.Uint64()
				var s string
				switch table.dataType {
				case TypeInt:
					s = strconv.FormatUint(uint64(ints[c]), 10)
				case TypeDouble:
					s = strconv.FormatFloat(doubles[c], 'f', 4, 64)
				case TypeDateTime:
					t := begin.Add(datetimeFactor * time.Duration(c))
					s = t.Format(timeLayout)
				case TypeString:
					s = uint2Str(c + strFactor)
				}
				cols = append(cols, s)
			}
			if err := w.Write(cols); err != nil {
				return err
			}
		}
		w.Flush()
	}
	return GenPZipfXLoadSQL(dir)
}

func GenPZipfXLoadSQL(dir string) error {
	var buf bytes.Buffer
	buf.WriteString("SET @@tidb_dml_batch_size=500000;\n")

	if !path.IsAbs(dir) {
		absPrefix, err := os.Getwd()
		if err != nil {
			return errors.Trace(err)
		}
		dir = path.Join(absPrefix, dir)
	}

	for _, tb := range tables {
		csvFile := path.Join(dir, fmt.Sprintf("%s.csv", tb.name))
		buf.WriteString(fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',';\n", csvFile, tb.name))
	}

	loadFile := path.Join(dir, "pzipfx_load.sql")
	return ioutil.WriteFile(loadFile, buf.Bytes(), 0666)
}
