package main

import (
	"fmt"
	
	"github.com/qw4990/OptimizerTester/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
	}
}
