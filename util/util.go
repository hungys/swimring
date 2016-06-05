package util

import (
	"net"
	"time"
)

const loopbackIP = "127.0.0.1"

// SelectIntOpt takes an option and a default value and returns the default value if
// the option is equal to zero, and the option otherwise.
func SelectIntOpt(opt, def int) int {
	if opt == 0 {
		return def
	}
	return opt
}

// SelectDurationOpt takes an option and a default value and returns the default value if
// the option is equal to zero, and the option otherwise.
func SelectDurationOpt(opt, def time.Duration) time.Duration {
	if opt == time.Duration(0) {
		return def
	}
	return opt
}

// GetLocalIP returns the local IP address.
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return loopbackIP
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return loopbackIP
}
