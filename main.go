package main

import (
	"flag"

	"github.com/arturom/datadiff/datasource"
	"github.com/arturom/datadiff/processing"
)

func main() {
	// Parse flags
	o := cliOpts{}
	o.parseFlags()

	interval := *o.initialInterval
	if interval != 10 && interval != 100 && interval != 1000 && interval != 10000 {
		panic("Interval must be a multiple of 10 between 10 and 10,000")
	}

	// Initialize datasource factory
	f := datasource.DataSourceFactory{}

	// Initialize primary data source
	primary, err := f.Create(
		*o.masterDriver, *o.masterConnection, *o.masterConfig)
	if err != nil {
		panic(err)
	}

	// Initialize secondary data source
	secondary, err := f.Create(
		*o.slaveDriver, *o.slaveConnection, *o.slaveConfig)
	if err != nil {
		panic(err)
	}

	// Do magic here
	err = processing.Process(primary, secondary, *o.initialInterval)
	if err != nil {
		panic(err)
	}
}

type cliOpts struct {
	initialInterval *int

	// Options for primary source
	masterDriver     *string
	masterConnection *string
	masterConfig     *string

	// Options for secondary source
	slaveDriver     *string
	slaveConnection *string
	slaveConfig     *string
}

func (o *cliOpts) parseFlags() {
	// Parse params for the primary data source
	o.masterDriver = flag.String("mdriver", "", "Primary source driver [elasticsearch|mysql]")
	o.masterConnection = flag.String("mconn", "", "Primary source connection string")
	o.masterConfig = flag.String("mconf", "{}", "Primary source configuration string")

	// Parse params for the secondary data source
	o.slaveDriver = flag.String("sdriver", "", "Secondary source driver [elasticsearch|mysql]")
	o.slaveConnection = flag.String("sconn", "", "Secondary source connection string")
	o.slaveConfig = flag.String("sconf", "{}", "Secondary source configuration string")

	// Parse universal params
	o.initialInterval = flag.Int("interval", 1000, "Initial histogram interval size")

	flag.Parse()
}
