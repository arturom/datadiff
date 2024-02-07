package datasource

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	es7 "github.com/elastic/go-elasticsearch/v7"
	es8 "github.com/elastic/go-elasticsearch/v8"
	es0 "gopkg.in/olivere/elastic.v1"
)

// DataSourceFactory instantiates data sources
type DataSourceFactory struct{}

// Create instantiates a data source based on the driver given
func (f DataSourceFactory) Create(driver, cnxString, config string) (DataSource, error) {
	if driver == "mysql" {
		return f.mySQLSource(cnxString, config)
	}

	if driver == "es0" {
		return f.elasticsearch0Source(cnxString, config)
	}

	if driver == "es7" {
		return f.elasticsearch7Source(cnxString, config)
	}

	if driver == "es8" {
		return f.elasticsearch8Source(cnxString, config)
	}

	return nil, fmt.Errorf("No datasource matching type: %s", driver)
}

type mySQLOpts struct {
	TableName  string   `json:"table_name"`
	FieldName  string   `json:"field_name"`
	Conditions []string `json:"conditions"`
}

func (f DataSourceFactory) mySQLSource(cnxString string, config string) (DataSource, error) {
	// Unmarshal config String
	c := mySQLOpts{}
	err := json.Unmarshal([]byte(config), &c)
	if err != nil {
		return nil, err
	}

	// Instantiate MySQL connection pool
	db, err := sql.Open("mysql", cnxString)
	if err != nil {
		return nil, err
	}

	// Ping the MySQL server
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	// Return instance of DataSource
	return MysqlDataSource{
		DB:         db,
		Tablename:  c.TableName,
		FieldName:  c.FieldName,
		Conditions: c.Conditions,
	}, nil
}

type es0Opts struct {
	IndexName string `json:"index"`
	TypeName  string `json:"type"`
	FieldName string `json:"field"`
}

func (f DataSourceFactory) elasticsearch0Source(cnxString string, config string) (DataSource, error) {
	// Unmarshal config String
	c := es0Opts{}
	err := json.Unmarshal([]byte(config), &c)
	if err != nil {
		return nil, err
	}

	// Instantiate an Elasticsearch client
	client, err := es0.NewClient(http.DefaultClient, cnxString)
	if err != nil {
		return nil, err
	}

	// Ping an Elasticsearch node
	_, _, err = client.Ping().URL(cnxString).Do()
	if err != nil {
		return nil, err
	}

	// Return instance of DataSource
	return NewES0DataSource(client, c.IndexName, c.TypeName, c.FieldName), nil
}

type es7Opts struct {
	Index string `json:"index"`
	Field string `json:"field"`
}

func (f DataSourceFactory) elasticsearch7Source(cnxString string, config string) (DataSource, error) {
	opts := es7Opts{}
	err := json.Unmarshal([]byte(config), &opts)
	if err != nil {
		return nil, err
	}
	client, err := es7.NewClient(es7.Config{
		Addresses: []string{cnxString},
	})
	if err != nil {
		return nil, err
	}

	return NewElasticsearch7DataSource(client, opts.Index, opts.Field), nil
}

type es8Opts struct {
	Index string `json:"index"`
	Field string `json:"field"`
}

func (f DataSourceFactory) elasticsearch8Source(cnxString string, config string) (DataSource, error) {
	opts := es8Opts{}
	err := json.Unmarshal([]byte(config), &opts)
	if err != nil {
		return nil, err
	}

	client, err := es8.NewTypedClient(es8.Config{
		Addresses: []string{cnxString},
	})
	if err != nil {
		return nil, err
	}

	return NewElasticsearch8DataSource(client, opts.Index, opts.Field), nil
}
