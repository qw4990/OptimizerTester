package cmd

import (
	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/cetest"
	"github.com/spf13/cobra"
)

func newCETestCmd() *cobra.Command {
	var conf string
	var partitionMode bool
	cmd := &cobra.Command{
		Use:   "cetest",
		Short: "Cardinality Estimation Test",
		RunE: func(cmd *cobra.Command, args []string) error {
			if conf == "" {
				return errors.New("no config")
			}
			if partitionMode {
				return cetest.RunCETestPartitionModeWithConfig(conf)
			}
			return cetest.RunCETestWithConfig(conf)
		},
	}
	cmd.Flags().StringVar(&conf, "config", "", "CETester config path")
	cmd.Flags().BoolVar(&partitionMode, "partition-mode", false, "Whether to use partition mode")
	return cmd
}
