package main

import (
	"flag"
	"io/ioutil"
	"os"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/hungys/swimring/util"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("swimring")

func main() {
	var externalPort, internalPort int
	var localIPAddr string

	initializeLogger()
	localIPAddr = util.GetLocalIP()
	config := loadConfig()

	flag.IntVar(&externalPort, "export", config.ExternalPort, "port number for external request")
	flag.IntVar(&internalPort, "inport", config.InternalPort, "port number for internal protocal communication")
	flag.Parse()

	config.Host = localIPAddr
	config.ExternalPort = externalPort
	config.InternalPort = internalPort

	logger.Infof("IP address: %s", localIPAddr)
	logger.Infof("External port: %d", config.ExternalPort)
	logger.Infof("Internal port: %d", config.InternalPort)
	logger.Infof("Bootsrap nodes: %v", config.BootstrapNodes)

	swimring := NewSwimRing(config)
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

func loadConfig() *configuration {
	logger.Info("Loading configurations from config.yml")

	config := &configuration{
		Host:               "0.0.0.0",
		ExternalPort:       7000,
		InternalPort:       7001,
		JoinTimeout:        1000,
		SuspectTimeout:     5000,
		PingTimeout:        1500,
		PingRequestTimeout: 5000,
		MinProtocolPeriod:  200,
		PingRequestSize:    3,
		VirtualNodeSize:    5,
		KVSReplicaPoints:   3,
		BootstrapNodes:     []string{},
	}

	data, err := ioutil.ReadFile("config.yml")
	if err != nil {
		logger.Warning("Cannot load config.yml")
	}

	err = yaml.Unmarshal(data, config)
	if err != nil {
		logger.Error("Fail to unmarshal config.yml")
	}

	for i, addr := range config.BootstrapNodes {
		if strings.HasPrefix(addr, ":") {
			config.BootstrapNodes[i] = util.GetLocalIP() + addr
		}
	}

	return config
}
