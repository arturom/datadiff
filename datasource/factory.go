package datasource

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"gopkg.in/olivere/elastic.v1"
)

// DefaultFactory instantiates data sources
type DefaultFactory struct{}

// Create instantiates a data source based on the driver given
func (f DefaultFactory) Create(driver, cnxString, config string) (DataSource, error) {
	if driver == "mysql" {
		return f.mySQLSource(cnxString, config)
	}

	if driver == "elasticsearch" {
		return f.elasticsearchSource(cnxString, config)
	}

	return nil, fmt.Errorf("No datasource matching type: %s", driver)
}

type mySQLOpts struct {
	TableName  string   `json:"table_name"`
	FieldName  string   `json:"field_name"`
	Conditions []string `json:"conditions"`
}

func (f DefaultFactory) mySQLSource(cnxString string, config string) (DataSource, error) {
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

func (f DefaultFactory) elasticsearchSource(cnxString string, config string) (DataSource, error) {
	// Unmarshal config String
	c := elasticsearchOpts{}
	err := json.Unmarshal([]byte(config), &c)
	if err != nil {
		return nil, err
	}

	// Instantiate an Elasticsearch client
	client, err := elastic.NewClient(http.DefaultClient, cnxString)
	if err != nil {
		return nil, err
	}

	// Ping an Elasticsearch node
	_, _, err = client.Ping().URL(cnxString).Do()
	if err != nil {
		return nil, err
	}

	// Return instance of DataSource
	return ES0DataSource{
		Client:    *client,
		IndexName: c.IndexName,
		TypeName:  c.TypeName,
		FieldName: c.FieldName,
	}, nil
}

type elasticsearchOpts struct {
	IndexName string `json:"index"`
	TypeName  string `json:"type"`
	FieldName string `json:"field"`
}
