package datagen

import (
	"strings"

	"github.com/pingcap/errors"
)

type DATAType int

const (
	TypeInt DATAType = iota
	TypeDouble
	TypeDateTime
	TypeString
)

func (dt DATAType) String() string {
	return typeNameMap[dt]
}

var (
	typeNameMap = map[DATAType]string{
		TypeInt:      "int",
		TypeDouble:   "double",
		TypeDateTime: "datetime",
		TypeString:   "string",
	}
)

// Generate ...
func Generate(dataset, args, dir string) error {
	switch strings.ToLower(dataset) {
	case "zipfx":
		return GenZipfXData(args, dir)
	case "pzipfx":
		return GenPZipfXData(args, dir)
	}
	return errors.Errorf("unsupported dataset=%v", dataset)
}
