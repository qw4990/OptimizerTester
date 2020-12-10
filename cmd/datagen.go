package cmd

import (
	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/datagen"
	"github.com/spf13/cobra"
)

func newDatagenCmd() *cobra.Command {
	var dataset string
	var args string
	var dir string
	cmd := &cobra.Command{
		Use:   "datagen",
		Short: "Data Generator",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataset == "" || dir == "" {
				return errors.Errorf("invalid arguments")
			}
			return datagen.Generate(dataset, args, dir)
		},
	}
	cmd.Flags().StringVar(&dataset, "dataset", "", "Dataset name to generate")
	cmd.Flags().StringVar(&args, "args", "", "Arguments")
	cmd.Flags().StringVar(&dir, "dir", "", "Directory to store data")
	return cmd
}
