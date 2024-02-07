package datasource

import (
	"context"

	h "github.com/arturom/datadiff/histogram"
	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
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

func (es Elasticsearch7DataSource) FetchHistogramAll(interval int) (h.Histogram, error) {
	req := createHistogramRequest(es.field, interval)
	res, err := es.search(req)
	if err != nil {
		return h.Histogram{}, err
	}
	return extractHistogramFromResponse(res, interval)
}

func (es Elasticsearch7DataSource) FetchHistogramRange(gte, lte, interval int) (h.Histogram, error) {
	req := createHistogramRequest(es.field, interval)
	req.Query = createRangeQuery(gte, lte)
	res, err := es.search(req)
	if err != nil {
		return h.Histogram{}, err
	}
	return extractHistogramFromResponse(res, interval)
}

func (es Elasticsearch7DataSource) FetchIDRange(gte, lt int) ([]int, error) {
	req := createIDRequest(es.field, gte, lt)
	res, err := es.search(req)
	if err != nil {
		return nil, err
	}
	return extractIDsFromResponse(res, es.field)
}

func (es Elasticsearch7DataSource) search(req *search.Request) (*search.Response, error) {
	buf, err := marshallRequest(req)
	if err != nil {
		return nil, err
	}
	res, err := es.client.Search(
		es.client.Search.WithContext(context.Background()),
		es.client.Search.WithIndex(es.index),
		es.client.Search.WithBody(buf),
	)
	if err != nil {
		return nil, err
	}

	return readBody(res.Body)
}

var _ DataSource = (*Elasticsearch7DataSource)(nil)
