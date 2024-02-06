package datasource

import (
	"github.com/arturom/datadiff/histogram"
	"gopkg.in/olivere/elastic.v1"
)

const facetLabel = "ids"

// ES0DataSource uses an Elasticsearch 0.90 backend to implement the datasource interface
type ES0DataSource struct {
	client    *elastic.Client
	indexName string
	typeName  string
	fieldName string
}

func NewES0DataSource(client *elastic.Client, index, typeName, field string) *ES0DataSource {
	return &ES0DataSource{
		client:    client,
		indexName: index,
		typeName:  typeName,
		fieldName: field,
	}
}

// FetchHistogramAll fetches a histogram of all IDs in an index
func (s ES0DataSource) FetchHistogramAll(interval int) (histogram.Histogram, error) {
	query := s.histogramQuery(interval)
	return s.processQuery(query, interval)
}

// FetchHistogramRange fetches a histogram of a selective range of IDs in an index
func (s ES0DataSource) FetchHistogramRange(gte, lt, interval int) (histogram.Histogram, error) {
	query := s.histogramQuery(interval).Query(s.rangeFilterQuery(gte, lt))
	return s.processQuery(query, interval)
}

// FetchIDRange fetches all the existing IDs in a given range
func (s ES0DataSource) FetchIDRange(gte, lt int) ([]int, error) {
	r, err := s.client.
		Search(s.indexName).
		Query(s.rangeFilterQuery(gte, lt)).
		Type(s.typeName).
		Fields(s.fieldName).
		Size(lt - gte).
		Do()
	if err != nil {
		return nil, err
	}

	hits := &r.Hits.Hits

	ids := make([]int, len(*hits))

	for i, h := range *hits {
		ids[i] = int(h.Fields[s.fieldName].(float64))
	}

	return ids, nil
}

func (s ES0DataSource) facet(interval int) elastic.Facet {
	return elastic.
		NewHistogramFacet().
		Field(s.fieldName).
		Interval(int64(interval))
}

func (s ES0DataSource) rangeFilterQuery(gte, lt int) elastic.Query {
	return elastic.
		NewFilteredQuery(elastic.NewMatchAllQuery()).
		Filter(elastic.NewRangeFilter(s.fieldName))
}

func (s ES0DataSource) histogramQuery(interval int) *elastic.SearchService {
	return s.client.
		Search(s.indexName).
		Type(s.typeName).
		Size(0).
		Facet(facetLabel, s.facet(interval))
}

func (s ES0DataSource) processQuery(query *elastic.SearchService, interval int) (histogram.Histogram, error) {
	r, err := query.Do()
	if err != nil {
		return histogram.Histogram{}, err
	}

	entries := &r.Facets[facetLabel].Entries

	b := make(histogram.Bins, len(*entries))
	for i, e := range *entries {
		b[i] = histogram.Bin{
			Key:   int(e.Key.(float64)),
			Count: e.Count,
		}
	}

	return histogram.Histogram{
		BinCapacity: interval,
		Bins:        b,
	}, nil
}
