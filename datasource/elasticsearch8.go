package datasource

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	h "github.com/arturom/datadiff/histogram"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

type Elasticsearch8DataSource struct {
	client *elasticsearch.TypedClient
	index  string
	field  string
}

func NewElasticsearch8DataSource(client *elasticsearch.TypedClient, index, field string) *Elasticsearch8DataSource {
	return &Elasticsearch8DataSource{
		client: client,
		index:  index,
		field:  field,
	}
}
func (es Elasticsearch8DataSource) FetchHistogramAll(interval int) (h.Histogram, error) {
	res, err := es.client.Search().
		Index(es.index).
		Request(&search.Request{
			Size: some.Int(0),
			Aggregations: map[string]types.Aggregations{
				"ids": {
					Histogram: &types.HistogramAggregation{
						Field:    &es.field,
						Interval: some.Float64(float64(interval)),
					},
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		return h.Histogram{}, err
	}
	return extractHistogramFromResponse(res, interval)
}

func (es Elasticsearch8DataSource) FetchHistogramRange(gte, lte, interval int) (h.Histogram, error) {
	return h.Histogram{}, nil
}

func (es Elasticsearch8DataSource) FetchIDRange(gte, lt int) ([]int, error) {
	return []int{}, nil
}

var _ DataSource = (*Elasticsearch8DataSource)(nil)

func createRangeQuery(gte, lt int) *types.Query {
	q := types.NewQuery()
	rangeQ := types.NewNumberRangeQuery()
	rangeQ.Gte = some.Float64(float64(gte))
	rangeQ.Lt = some.Float64(float64(lt))
	q.Range["id"] = rangeQ
	return q
}

func createHistogramRequest(field string, interval int) *search.Request {
	return &search.Request{
		Size: some.Int(0),
		Aggregations: map[string]types.Aggregations{
			"ids": {
				Histogram: &types.HistogramAggregation{
					Field:    some.String(field),
					Interval: some.Float64(float64(interval)),
				},
			},
		},
	}
}

func marshallRequest(req *search.Request) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(req)
	if err != nil {
		return nil, err
	}
	return &buf, nil
}

func readBody(body io.ReadCloser) (*search.Response, error) {
	res := search.NewResponse()
	defer body.Close()
	bytes, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	res.UnmarshalJSON(bytes)
	return res, nil
}

func extractHistogramFromResponse(res *search.Response, interval int) (h.Histogram, error) {
	agg := res.Aggregations["ids"]

	// JSON marshall/unmarshall hack to avoid writing type conversion code
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(agg)
	if err != nil {
		return h.Histogram{}, err
	}
	ids := types.NewHistogramAggregate()
	ids.UnmarshalJSON(buf.Bytes())

	return extractHistogram(ids, interval), nil
}

func extractHistogram(agg *types.HistogramAggregate, capacity int) h.Histogram {
	buckets := agg.Buckets.([]types.HistogramBucket)
	bins := make(h.Bins, len(buckets))
	for i, bucket := range buckets {
		bins[i] = h.Bin{
			Key:   int(bucket.Key),
			Count: int(bucket.DocCount),
		}
	}
	return h.Histogram{
		Bins:        bins,
		BinCapacity: capacity,
	}
}
