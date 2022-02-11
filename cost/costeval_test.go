package cost

import (
	"fmt"
	"testing"

	"github.com/qw4990/OptimizerTester/tidb"
)

func TestRunCostEvalQuery(t *testing.T) {
	opt := tidb.Option{
		Addr:     "172.16.5.173",
		Port:     4000,
		User:     "root",
		Password: "",
		Label:    "",
	}
	//opt.Addr = "127.0.0.1"

	ins, err := tidb.ConnectTo(opt)
	if err != nil {
		panic(err)
	}

	qs := Queries{Query{
		SQL:    "select /*+ use_index(t, b) */ b, c from t where b>=1 and b<=6666",
		Label:  "",
		TypeID: 0,
	}}

	// select /*+ use_index(t, b) */ b, c from t where b>=1 and b<=6666
	rs := runCostEvalQueries(ins, "synthetic", qs, []string{}, nil, 2, 500)
	r := rs[0]
	fmt.Println(">>>>>>> r.Label ", r.Label)
}
