package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"

	"github.com/arturom/datadiff/datasource"
	"gopkg.in/olivere/elastic.v1"
)

func main() {
	// Parse flags
	o := cliOpts{}
	o.parseFlags()

	// Initialize data sources
	m, s, err := o.initSources()
	if err != nil {
		panic(err)
	}

	// Do magic here

	mh, err := m.FetchHistogramAll(*o.initialInterval)
	if err != nil {
		panic(err)
	}

	sh, err := s.FetchHistogramAll(*o.initialInterval)
	if err != nil {
		panic(err)
	}

	merged := mh.Merge(sh)

	for _, b := range merged.UnresolvedPairs() {
		fmt.Printf("Range: [%9d %9d]   |   Master: %9d   |   Slave: %9d   |   Diff: %9d\n", b.Key, b.Key+*o.initialInterval, b.CountFromMaster, b.CountFromSlave, b.DiffCount())
	}
}

type sourceOpts struct {
	driver     *string
	config     *string
	configfile *string
}

type cliOpts struct {
	initialInterval *int

	// Options for master source
	masterDriver     *string
	masterConnection *string
	masterConfig     *string

	// Options for slave source
	slaveDriver     *string
	slaveConnection *string
	slaveConfig     *string
}

func (o *cliOpts) parseFlags() {

	// Parse params for the Master data source
	o.masterDriver = flag.String("mdriver", "", "Master driver [elasticsearch|mysql]")
	o.masterConnection = flag.String("mconn", "", "Master connection string")
	o.masterConfig = flag.String("mconf", "{}", "Master configuration string")

	// Parse params for the Slave data source
	o.slaveDriver = flag.String("sdriver", "", "Slave driver [elasticsearch|mysql]")
	o.slaveConnection = flag.String("sconn", "", "Slave connection string")
	o.slaveConfig = flag.String("sconf", "{}", "Slave configuration string")

	// Parse universal params
	o.initialInterval = flag.Int("interval", 1000, "Initial histogram interval")

	flag.Parse()
}

func (o cliOpts) initSources() (m, s datasource.DataSource, err error) {
	m, err = datasourceFactory(*o.masterDriver, *o.masterConnection, *o.masterConfig)
	if err != nil {
		return nil, nil, err
	}
	s, err = datasourceFactory(*o.slaveDriver, *o.slaveConnection, *o.slaveConfig)
	if err != nil {
		return nil, nil, err
	}
	return
}

func datasourceFactory(driver, connection, config string) (datasource.DataSource, error) {
	if driver == "mysql" {
		return initMysql(connection, config)
	}

	if driver == "elasticsearch" {
		return initElasticsearch(connection, config)
	}

	return nil, fmt.Errorf("No datasource matching type: %s", driver)
}

type mySQLSpecificConfig struct {
	TableName  string   `json:"table_name"`
	FieldName  string   `json:"field_name"`
	Conditions []string `json:"conditions"`
}

func initMysql(connection, config string) (datasource.DataSource, error) {
	// Unmarshal config String
	c := mySQLSpecificConfig{}
	err := json.Unmarshal([]byte(config), &c)
	if err != nil {
		return nil, err
	}

	// Instantiate MySQL connection pool
	db, err := sql.Open("mysql", connection)
	if err != nil {
		return nil, err
	}

	// Ping the MySQL server
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	// Return instance of datasource.DataSource
	return datasource.MysqlDataSource{
		DB:         db,
		Tablename:  c.TableName,
		FieldName:  c.FieldName,
		Conditions: c.Conditions,
	}, nil
}

type esSpecificConfig struct {
	IndexName string `json:"index"`
	TypeName  string `json:"type"`
	FieldName string `json:"field"`
}

func initElasticsearch(connection, config string) (datasource.DataSource, error) {
	// Unmarshal config String
	c := esSpecificConfig{}
	err := json.Unmarshal([]byte(config), &c)
	if err != nil {
		return nil, err
	}

	// Instantiate an Elasticsearch client
	client, err := elastic.NewClient(http.DefaultClient, connection)
	if err != nil {
		return nil, err
	}

	// Ping an Elasticsearch node
	_, _, err = client.Ping().URL(connection).Do()
	if err != nil {
		return nil, err
	}

	// Return instance of datasource.DataSource
	return datasource.ES0DataSource{
		Client:    *client,
		IndexName: c.IndexName,
		TypeName:  c.TypeName,
		FieldName: c.FieldName,
	}, nil
}
