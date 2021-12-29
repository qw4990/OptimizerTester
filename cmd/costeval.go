package cmd

import (
	"github.com/qw4990/OptimizerTester/cost"
	"github.com/spf13/cobra"
)

func newCostEvalCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cost-eval",
		Short: "Cost Model Evaluation",
		RunE: func(cmd *cobra.Command, args []string) error {
			//cost.CostEval()
			cost.CostCalibration()
			return nil
		},
	}
	return cmd
}
