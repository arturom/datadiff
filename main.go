package main

import (
	"flag"
	"fmt"

	"github.com/arturom/datadiff/datasource"
)

func main() {
	// Parse flags
	o := cliOpts{}
	o.parseFlags()

	// Initialize datasource factory
	f := datasource.DefaultFactory{}

	// Initialize master data source
	master, err := f.Create(
		*o.masterDriver, *o.masterConnection, *o.masterConfig)
	if err != nil {
		panic(err)
	}

	// Initialize slave data source
	slave, err := f.Create(
		*o.slaveDriver, *o.slaveConnection, *o.slaveConfig)
	if err != nil {
		panic(err)
	}

	// Do magic here
	mh, err := master.FetchHistogramAll(*o.initialInterval)
	if err != nil {
		panic(err)
	}

	sh, err := slave.FetchHistogramAll(*o.initialInterval)
	if err != nil {
		panic(err)
	}

	merged := mh.Merge(sh)

	for _, b := range merged.UnresolvedPairs() {
		fmt.Printf(
			"Range: [%9d %9d]   |   Master: %9d   |   Slave: %9d   |   Diff: %9d\n",
			b.Key, b.Key+*o.initialInterval, b.CountFromMaster, b.CountFromSlave, b.DiffCount())
	}
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
