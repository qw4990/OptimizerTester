package cmd

import (
	"github.com/qw4990/OptimizerTester/cebench"
	"github.com/spf13/cobra"
)

func newCEBenchCmd() *cobra.Command {
	var queryLocation string
	var dsn []string
	var outDir string
	var jsonLocation string
	var needDedup bool
	var badEstThreshold uint
	var concurrencyForEachDSN uint
	cmd := &cobra.Command{
		Use:   "cebench [-s xxx.sql -dsn \"root@tcp(127.0.0.1:4000)/imdb\" | -j xxx.json] [-o result]",
		Short: "Cardinality Estimation Benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cebench.RunCEBench(queryLocation, dsn, jsonLocation, outDir, needDedup, badEstThreshold, concurrencyForEachDSN)
		},
	}
	cmd.Flags().StringVarP(&queryLocation, "sql-file", "s", "", "SQL file or directory containing SQL files")
	cmd.Flags().StringSliceVar(&dsn, "dsn", nil, "DSN")
	cmd.Flags().StringVarP(&outDir, "output-dir", "o", "result", "Directory to store the results")
	cmd.Flags().StringVarP(&jsonLocation, "json", "j", "", "The JSON file containing bench intermediate result")
	cmd.Flags().BoolVar(&needDedup, "dedup", true, "Whether deduplicate the estimation results")
	cmd.Flags().UintVar(&badEstThreshold, "threshold", 10, "The estimation results with p-error higher than the threshold will be printed")
	cmd.Flags().UintVar(&concurrencyForEachDSN, "concurrency", 4, "The connections opened for each DSN")
	return cmd
}
