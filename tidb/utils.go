package tidb

import (
	"strconv"
	"strings"
)

// ToComparableVersion converts this version string to a comparable number.
//	vX.Y.Z => x*10000 + Y*100 + Z
//	v3.0.15 => 300015 < 400002 <= v4.0.2
func ToComparableVersion(ver string) int {
	xs := strings.Split(ver[1:], ".")
	x, _ := strconv.Atoi(xs[0])
	y, _ := strconv.Atoi(xs[1])
	z, _ := strconv.Atoi(xs[2])
	return x*10000 + y*100 + z
}
