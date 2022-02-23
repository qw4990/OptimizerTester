package cmd

import (
	"github.com/qw4990/OptimizerTester/cebench"
	"github.com/spf13/cobra"
)

func newCEBenchCmd() *cobra.Command {
	var queryLocation string
	var dsn []string
	var outDir string
	var jsonLocations []string
	var needDedup bool
	var badEstThreshold uint
	var concurrencyForEachDSN uint
	cmd := &cobra.Command{
		Use:   "cebench [-s xxx.sql -dsn \"root@tcp(127.0.0.1:4000)/imdb\" | -j xxx.json] [-o result]",
		Short: "Cardinality Estimation Benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			inputOpt := &cebench.InputOption{
				QueryPath: queryLocation,
				DSNs:      dsn,
				JSONPaths: jsonLocations,
			}
			otherOpt := &cebench.OtherOption{
				OutPath:               outDir,
				Dedup:                 needDedup,
				PErrorThreshold:       badEstThreshold,
				ConcurrencyForEachDSN: concurrencyForEachDSN,
			}
			return cebench.RunCEBench(inputOpt, otherOpt)
		},
	}
	cmd.Flags().StringVarP(&queryLocation, "sql-file", "s", "", "SQL file or directory containing SQL files")
	cmd.Flags().StringSliceVar(&dsn, "dsn", nil, "DSN")
	cmd.Flags().StringVarP(&outDir, "output-dir", "o", "result", "Directory to store the results")
	cmd.Flags().StringArrayVarP(&jsonLocations, "json", "j", nil, "The JSON file containing bench intermediate result")
	cmd.Flags().BoolVar(&needDedup, "dedup", true, "Whether deduplicate the estimation results")
	cmd.Flags().UintVar(&badEstThreshold, "threshold", 10, "The estimation results with p-error higher than the threshold will be printed")
	cmd.Flags().UintVar(&concurrencyForEachDSN, "concurrency", 4, "The connections opened for each DSN")
	return cmd
}

func newCEBenchCompareCmd() *cobra.Command {
	var outDir string
	var jsonLocations, labels []string
	var needDedup bool
	var badEstThreshold uint
	cmd := &cobra.Command{
		Use:   "cecmp -j xxx.json -j xxx.json -j xxx.json [-o result]",
		Short: "Cardinality Estimation Benchmark Compare",
		RunE: func(cmd *cobra.Command, args []string) error {
			inputOpt := &cebench.InputOption{
				JSONPaths: jsonLocations,
			}
			otherOpt := &cebench.OtherOption{
				OutPath:         outDir,
				Dedup:           needDedup,
				PErrorThreshold: badEstThreshold,
				Labels:          labels,
			}
			return cebench.RunCECompare(inputOpt, otherOpt)
		},
	}

	cmd.Flags().StringVarP(&outDir, "output-dir", "o", "result", "Directory to store the results")
	cmd.Flags().StringArrayVarP(&jsonLocations, "json", "j", nil, "The JSON file containing bench intermediate result")
	cmd.Flags().StringArrayVarP(&labels, "label", "l", nil, "The label for each JSON file")
	return cmd
}
