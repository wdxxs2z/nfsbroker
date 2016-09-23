package main

import (
	"flag"

	"../../utils"
	"../../nfsbroker"

	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/cflager"

	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
	ioutilshim "code.cloudfoundry.org/goshims/ioutil"

	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
	"fmt"
)

var listenAddress = flag.String(
	"listenAddr",
	"0.0.0.0:8980",
	"host:port to serve nfs service broker function",
)

var nfsHost = flag.String(
	"remoteInfo",
	"10.10.130.49",
	"host for nfs remoteInfo server",
)

var remoteMount = flag.String(
	"remoremount",
	"/var/vcap/store",
	"nfs remote mount director",
)

var nfsVer = flag.Int(
	"version",
	4,
	"version number for nfs server",
)

var configPath = flag.String(
	"configPath",
	"/tmp/nfsbroker",
	"config director to store config info",
)

var defaultMountPath = flag.String(
	"defaultMountPath",
	"/tmp/share",
	"local director to mount within",
)

var serviceName = flag.String(
	"serviceName",
	"nfs",
	"name to registrer with cloud controller",
)

var serviceId =flag.String(
	"serviceId",
	"nfs-service-guid",
	"id to nfs service",
)

var planName = flag.String(
	"planName",
	"free",
	"name to the nfs service plan",
)

var planId = flag.String(
	"planId",
	"nfs-plan-guid",
	"guid to the nfs service plan",
)

var planDesc = flag.String(
	"planDesc",
	"free nfs filesystem",
	"description of the service plan to register with cloud controller",
)

var dataDir = flag.String(
	"dataDir",
	"",
	"[REQUIRED] - Broker's state will be stored here to persist across reboots",
)

var username = flag.String(
	"username",
	"admin",
	"basic auth username to verify on incoming requests",
)
var password = flag.String(
	"password",
	"admin",
	"basic auth password to verify on incoming requests",
)

var displayName = flag.String(
	"displayName",
	"common volume driver",
	"display the volume service broker name",
)

var imageUrl = flag.String(
	"imageUrl",
	"",
	"base64 code image or image url",
)

func main() {
	parseCommandLine()

	logger, logSink := cflager.New(fmt.Sprintf("%s-volume-service-broker", *serviceName))
	logger.Info("start")
	defer logger.Info("ends")

	server := createBrokerServer(logger)

	if dbgAddr := debugserver.DebugAddress(flag.CommandLine);dbgAddr != "" {
		server = utils.ProcessRunnerFor(grouper.Members{
			{"debug-server", debugserver.Runner(dbgAddr,logSink)},
			{"broker-api-server", server},
		})
	}
	process := ifrit.Invoke(server)
	logger.Info("started-nfs-serverbroker", lager.Data{"brokerAddress": *listenAddress})
	utils.UntilTerminated(logger, process)
}

func createBrokerServer(logger lager.Logger) ifrit.Runner {
	controller := nfsbroker.NewController(nfsbroker.NewNfsClient(*nfsHost, *remoteMount, *nfsVer, *defaultMountPath))
	serviceBroker := nfsbroker.New(
		logger,
		controller,
		*serviceName,
		*serviceId,
		*planName,
		*planId,
		*planDesc,
		*displayName,
		*imageUrl,
		*dataDir,
		&ioutilshim.IoutilShim{},
	)
	credentials := brokerapi.BrokerCredentials{Username:*username, Password:*password}
	handler := brokerapi.New(serviceBroker, logger.Session("broker-api"), credentials)
	return http_server.New(*listenAddress, handler)
}

func parseCommandLine() {
	cflager.AddFlags(flag.CommandLine)
	debugserver.AddFlags(flag.CommandLine)
	flag.Parse()
}