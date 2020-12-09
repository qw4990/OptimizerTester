package cmd

import (
	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/datagen"
	"github.com/spf13/cobra"
)

func newDatagenCmd() *cobra.Command {
	var dataset string
	var n int
	var dir string
	cmd := &cobra.Command{
		Use:   "datagen",
		Short: "Data Generator",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataset == "" || n <= 0 || dir == "" {
				return errors.Errorf("invalid arguments")
			}
			return datagen.Generate(dataset, n, dir)
		},
	}
	cmd.Flags().StringVar(&dataset, "dataset", "", "Dataset name to generate")
	cmd.Flags().IntVar(&n, "n", 100000, "Number of rows to generate")
	cmd.Flags().StringVar(&dir, "dir", "", "Directory to store data")
	return cmd
}
