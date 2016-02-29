package datasource

import "github.com/arturom/datadiff/histogram"

type DataSource interface {
	FetchHistogramAll(interval int) (histogram.Histogram, error)
	FetchHistogramRange(gte, lt, interval int) (histogram.Histogram, error)
	FetchIdRange(gte, lt int) ([]int, error)
}
