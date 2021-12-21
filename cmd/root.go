package cmd

import (
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "optimizer-tester",
		Short: "TiDB Optimizer Tester",
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize()
	rootCmd.AddCommand(newCETestCmd())
	rootCmd.AddCommand(newDatagenCmd())
	rootCmd.AddCommand(newCEBenchCmd())
	rootCmd.AddCommand(newCostEvalCmd())
}
