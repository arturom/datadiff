package datasource

import (
	"bytes"
	"context"
	"encoding/json"

	h "github.com/arturom/datadiff/histogram"
	elasticsearch "github.com/elastic/go-elasticsearch/v7"
)

type Elasticsearch7DataSource struct {
	client *elasticsearch.Client
	index  string
	field  string
}

func NewElasticsearch7DataSource(client *elasticsearch.Client, index, field string) *Elasticsearch7DataSource {
	return &Elasticsearch7DataSource{
		client: client,
		index:  index,
		field:  field,
	}
}

func marshallRequestx(field string, interval int) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	hist := createHistogramRequest(field, interval)
	err := json.NewEncoder(&buf).Encode(hist)
	if err != nil {
		return nil, err
	}
	return &buf, nil
}

func (es Elasticsearch7DataSource) FetchHistogramAll(interval int) (h.Histogram, error) {
	req := createHistogramRequest(es.field, interval)
	buf, err := marshallRequest(req)
	if err != nil {
		return h.Histogram{}, err
	}
	res, err := es.client.Search(
		es.client.Search.WithContext(context.Background()),
		es.client.Search.WithIndex(es.index),
		es.client.Search.WithBody(buf),
	)
	if err != nil {
		return h.Histogram{}, err
	}

	searchRes, err := readBody(res.Body)
	if err != nil {
		return h.Histogram{}, err
	}

	return extractHistogramFromResponse(searchRes, interval)
}

func (es Elasticsearch7DataSource) FetchHistogramRange(gte, lte, interval int) (h.Histogram, error) {
	req := createHistogramRequest(es.field, interval)
	req.Query = createRangeQuery(gte, lte)
	buf, err := marshallRequest(req)
	if err != nil {
		return h.Histogram{}, err
	}
	res, err := es.client.Search(
		es.client.Search.WithContext(context.Background()),
		es.client.Search.WithIndex(es.index),
		es.client.Search.WithBody(buf),
	)
	if err != nil {
		return h.Histogram{}, err
	}

	searchRes, err := readBody(res.Body)
	if err != nil {
		return h.Histogram{}, err
	}

	return extractHistogramFromResponse(searchRes, interval)
}

func (es Elasticsearch7DataSource) FetchIDRange(gte, lt int) ([]int, error) {
	return []int{}, nil
}

var _ DataSource = (*Elasticsearch7DataSource)(nil)
