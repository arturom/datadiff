package processing

import (
	"fmt"

	"github.com/arturom/datadiff/datasource"
)

func Process(primary, secondary datasource.DataSource, interval int) error {
	priHistogram, err := primary.FetchHistogramAll(interval)
	if err != nil {
		return err
	}
	secHistogram, err := secondary.FetchHistogramAll(interval)
	if err != nil {
		return err
	}

	merged := priHistogram.Merge(secHistogram)

	fmt.Printf(
		"|%9s | %9s | %9s | %9s | %9s |\n",
		"Min", "Max", "Primary", "Secondary", "Diff")

	for _, b := range merged.UnresolvedPairs() {
		diff := b.DiffCount()
		fmt.Printf(
			"|%9d | %9d | %9d | %9d | %9d |\n",
			b.Key, b.Key+interval-1, b.CountFromPrimary, b.CountFromSecondary, diff)
	}

	return nil
}

func processRange(primary, secondary datasource.DataSource, gte, lt, interval int) error {
	return nil
}
