package cost

import (
	"fmt"

	"github.com/qw4990/OptimizerTester/tidb"
)

// CostEval ...
func CostEval() {
	opt := tidb.Option{
		Addr:     "172.16.5.173",
		Port:     4000,
		User:     "root",
		Password: "",
		Label:    "",
	}

	ins, err := tidb.ConnectTo(opt)
	if err != nil {
		panic(err)
	}

	qs := genIMDBQueries(ins, "imdb")
	for _, q := range qs {
		fmt.Println(q)
	}
}
