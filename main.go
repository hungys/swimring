package main

import (
	"flag"
	"fmt"

	"github.com/hungys/swimring/util"
)

func main() {
	var localIPAddr, externalPort, internalPort string

	flag.StringVar(&externalPort, "export", "7000", "port number for external request")
	flag.StringVar(&internalPort, "inport", "7001", "port number for internal protocal communication")
	flag.Parse()

	localIPAddr = util.GetLocalIP()

	configuration := &configuration{
		Host:           localIPAddr,
		ExternalPort:   externalPort,
		InternalPort:   internalPort,
		BootstrapNodes: []string{fmt.Sprintf("%s:%s", localIPAddr, "7001")},
	}

	swimring := NewSwimRing(configuration)
	swimring.Bootstrap()

	select {}
}
