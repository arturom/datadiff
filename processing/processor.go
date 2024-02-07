package processing

import (
	"fmt"

	"github.com/arturom/datadiff/datasource"
	"github.com/arturom/datadiff/histogram"
)

func Process(primary, secondary datasource.DataSource, interval int) error {
	// fmt.Printf("FetchAll    Interval: %2d\n", interval)
	priHistogram, err := primary.FetchHistogramAll(interval)
	if err != nil {
		return err
	}
	secHistogram, err := secondary.FetchHistogramAll(interval)
	if err != nil {
		return err
	}
	/*
		fmt.Printf(
			"|%9s | %9s | %9s | %9s | %9s | %9s |\n",
			"Interval", "Min", "Max", "Primary", "Secondary", "Diff")
	*/
	return processHistograms(priHistogram, secHistogram, primary, secondary, interval)
}

func fetchRange(primary, secondary datasource.DataSource, gte, lt, interval int) error {
	// fmt.Printf("FetchRange  Interval: %3d  gte: %3d  lt: %3d\n", interval, gte, lt)
	priHistogram, err := primary.FetchHistogramRange(gte, lt, interval)
	if err != nil {
		return err
	}
	secHistogram, err := secondary.FetchHistogramRange(gte, lt, interval)
	if err != nil {
		return err
	}
	return processHistograms(priHistogram, secHistogram, primary, secondary, interval)
}

func processHistograms(priHistogram, secHistogram histogram.Histogram, primary, secondary datasource.DataSource, interval int) error {
	merged := priHistogram.Merge(secHistogram)
	// printMergedSummary(merged, interval)
	for _, pair := range merged.UnresolvedPairs() {
		err := fetchNext(primary, secondary, pair.Key, pair.Key+interval, interval/10)
		if err != nil {
			return err
		}
	}
	return nil
}

func fetchIDs(primary, secondary datasource.DataSource, gte, lt int) error {
	// fmt.Printf("FetchIDs   gte: %3d  lt: %3d\n", gte, lt)
	primaryIDs, err := primary.FetchIDRange(gte, lt)
	if err != nil {
		return err
	}
	secondaryIds, err := secondary.FetchIDRange(gte, lt)
	if err != nil {
		return err
	}

	m := make(map[int]int)
	for _, id := range primaryIDs {
		m[id] = -1
	}
	for _, id := range secondaryIds {
		_, ok := m[id]
		if ok {
			m[id] = 0
		} else {
			m[id] = 1
		}
	}

	// w := csv.NewWriter(os.Stdout)
	for id, flag := range m {
		if flag != 0 {
			/*
				values := []string{strconv.Itoa(id), strconv.Itoa(flag)}
				if err = w.Write(values); err != nil {
					return nil
				}
			*/
			fmt.Printf("%d,%d\n", id, flag)
		}
	}
	// w.Flush()

	return nil
}

func printMergedSummary(merged histogram.MergedHistogram, interval int) {
	for _, b := range merged.UnresolvedPairs() {
		diff := b.DiffCount()
		fmt.Printf(
			"|%9d | %9d | %9d | %9d | %9d | %9d |\n",
			interval, b.Key, b.Key+interval-1, b.CountFromPrimary, b.CountFromSecondary, diff)
	}
}

func fetchNext(primary, secondary datasource.DataSource, gte, lt, interval int) error {
	// fmt.Printf("FetchNext   Interval: %9d  gte: %9d  lt: %9d\n", interval, gte, lt)
	if interval > 1 {
		return fetchRange(primary, secondary, gte, lt, interval)
	} else {
		return fetchIDs(primary, secondary, gte, lt)
	}
}
