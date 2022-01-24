module github.com/qw4990/OptimizerTester

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/tidb v1.1.0-beta.0.20220111060941-50dfe6b7bfbb
	github.com/pingcap/tidb/parser v0.0.0-20220111060941-50dfe6b7bfbb
	github.com/spf13/cobra v1.1.1
	go.uber.org/atomic v1.9.0
	gonum.org/v1/plot v0.10.0
	gorgonia.org/gorgonia v0.9.17
	gorgonia.org/tensor v0.9.17
)

replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
