package datasource

import (
	"github.com/arturom/datadiff/histogram"
	"gopkg.in/olivere/elastic.v1"
)

const facetLabel = "ids"

type ES0DataSource struct {
	Client    elastic.Client
	IndexName string
	TypeName  string
	FieldName string
}

func (s ES0DataSource) FetchHistogramAll(interval int) (histogram.Histogram, error) {
	query := s.HistogramQuery(interval)
	return s.ProcessQuery(query, interval)
}

func (s ES0DataSource) FetchHistogramRange(gte, lt, interval int) (histogram.Histogram, error) {
	query := s.HistogramQuery(interval).Query(s.RangeFilterQuery(gte, lt))
	return s.ProcessQuery(query, interval)
}

func (s ES0DataSource) FetchIdRange(gte, lt int) ([]int, error) {
	r, err := s.Client.
		Search(s.IndexName).
		Query(s.RangeFilterQuery(gte, lt)).
		Type(s.TypeName).
		Fields(s.FieldName).
		Size(lt - gte).
		Do()
	if err != nil {
		return nil, err
	}

	hits := &r.Hits.Hits

	ids := make([]int, len(*hits))

	for i, h := range *hits {
		ids[i] = int(h.Fields[s.FieldName].(float64))
	}

	return ids, nil
}

func (s ES0DataSource) Facet(interval int) elastic.Facet {
	return elastic.
		NewHistogramFacet().
		Field(s.FieldName).
		Interval(int64(interval))
}

func (s ES0DataSource) RangeFilterQuery(gte, lt int) elastic.Query {
	return elastic.
		NewFilteredQuery(elastic.NewMatchAllQuery()).
		Filter(elastic.NewRangeFilter(s.FieldName))
}

func (s ES0DataSource) HistogramQuery(interval int) *elastic.SearchService {
	return s.Client.
		Search(s.IndexName).
		Type(s.TypeName).
		Size(0).
		Facet(facetLabel, s.Facet(interval))
}

func (s ES0DataSource) ProcessQuery(query *elastic.SearchService, interval int) (histogram.Histogram, error) {
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
