package cmd

import (
	"github.com/qw4990/OptimizerTester/querygen"
	"github.com/spf13/cobra"
)

func newQueryGenCmd() *cobra.Command {
	var dsn []string
	var outDir, dbName, tableName string
	var n uint
	cmd := &cobra.Command{
		Use:   "querygen",
		Short: "Cardinality Estimation Benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			return querygen.RunQueryGen(dsn, outDir, dbName, tableName, n)
		},
	}
	cmd.Flags().StringSliceVar(&dsn, "dsn", nil, "DSN")
	cmd.Flags().StringVarP(&outDir, "output-file", "o", "query.sql", "Directory to store the results")
	cmd.Flags().StringVar(&dbName, "db", "", "Database Name")
	cmd.Flags().StringVar(&tableName, "table", "", "Table Name")
	cmd.Flags().UintVarP(&n, "query-num", "n", 300, "Table Name")
	cmd.MarkFlagRequired("dsn")
	cmd.MarkFlagRequired("db")
	cmd.MarkFlagRequired("table")
	return cmd
}
