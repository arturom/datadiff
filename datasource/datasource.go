package datasource

import "github.com/arturom/datadiff/histogram"

// DataSource describes a source of data containing records with numeric IDs
type DataSource interface {
	FetchHistogramAll(interval int) (histogram.Histogram, error)
	FetchHistogramRange(gte, lt, interval int) (histogram.Histogram, error)
	FetchIDRange(gte, lt int) ([]int, error)
}
