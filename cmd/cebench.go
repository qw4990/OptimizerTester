package cmd

import (
	"github.com/qw4990/OptimizerTester/cebench"
	"github.com/spf13/cobra"
)

func newCEBenchCmd() *cobra.Command {
	var queryLocation string
	var dsn []string
	var outDir string
	cmd := &cobra.Command{
		Use:   "cebench",
		Short: "Cardinality Estimation Benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cebench.RunCEBench(queryLocation, dsn, outDir)
		},
	}
	cmd.Flags().StringVarP(&queryLocation, "sql-file", "s", "", "SQL file or directory containing SQL files")
	cmd.Flags().StringSliceVar(&dsn, "dsn", nil, "DSN")
	cmd.Flags().StringVarP(&outDir, "output-dir", "o", "result", "Directory to store the results")
	cmd.MarkFlagRequired("sql-file")
	cmd.MarkFlagRequired("dsn")
	return cmd
}
