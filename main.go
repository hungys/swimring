package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hungys/swimring/util"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("swimring")

func main() {
	var localIPAddr, externalPort, internalPort string

	flag.StringVar(&externalPort, "export", "7000", "port number for external request")
	flag.StringVar(&internalPort, "inport", "7001", "port number for internal protocal communication")
	flag.Parse()

	localIPAddr = util.GetLocalIP()

	initializeLogger()

	logger.Infof("IP address: %s", localIPAddr)
	logger.Infof("External port: %s", externalPort)
	logger.Infof("Internal port: %s", internalPort)

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

func initializeLogger() {
	var format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortpkg} ▶ %{level:.4s}%{color:reset} %{message}`,
	)

	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
}
