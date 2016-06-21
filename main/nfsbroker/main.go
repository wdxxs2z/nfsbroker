package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/wdxxs2z/nfsbroker/nfsbrokerhttp"
	"github.com/wdxxs2z/nfsbroker/nfsbrokerlocal"
	"github.com/wdxxs2z/nfsbroker/model"
	"github.com/wdxxs2z/nfsbroker/utils"
	cf_debug_server "github.com/cloudfoundry-incubator/cf-debug-server"
	cf_lager "github.com/cloudfoundry-incubator/cf-lager"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
	"github.com/tedsuo/ifrit/sigmon"
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

func main() {
	parseCommandLine()
	withLogger, logTap := logger()
	defer withLogger.Info("ends")

	servers, err := createNfsBrokerServer(withLogger, *listenAddress)

	if err!= nil {
		panic("failed to load services metadata......aborting")
	}
	if dbgAddr := cf_debug_server.DebugAddress(flag.CommandLine);dbgAddr != "" {
		servers = append(grouper.Members{
			{"debug-server", cf_debug_server.Runner(dbgAddr, logTap)},
		}, servers...)
	}
	process := ifrit.Invoke(processRunnerFor(servers))
	withLogger.Info("started")
	untilTerminated(withLogger, process)
}

func createNfsBrokerServer(logger lager.Logger, listenAddress string) (grouper.Members, error) {
	nfsClient := nfsbrokerlocal.NewNfsClient(*nfsHost, *remoteMount, *nfsVer, *defaultMountPath)
	existingServiceInstances, err := loadServiceInstances()
	if err != nil {
		return nil, err
	}
	existingServiceBindings, err := loadServiceBindings()
	if err != nil {
		return nil, err
	}
	controller := nfsbrokerlocal.NewController(nfsClient, *configPath, existingServiceInstances, existingServiceBindings)
	handler, err := nfsbrokerhttp.NewHandler(logger, controller)
	exitOnFailure(logger, err)

	return grouper.Members{
		{"http-server", http_server.New(listenAddress, handler)},
	}, nil
}

func exitOnFailure(logger lager.Logger, err error) {
	if err != nil {
		logger.Error("Fatal err.. aborting", err)
		panic(err.Error())
	}
}

func untilTerminated(logger lager.Logger, process ifrit.Process) {
	err := <-process.Wait()
	exitOnFailure(logger, err)
}

func processRunnerFor(servers grouper.Members) ifrit.Runner {
	return sigmon.New(grouper.NewOrdered(os.Interrupt, servers))
}

func loadServiceBindings() (map[string]*model.ServiceBinding, error) {
	var bindingMap map[string]*model.ServiceBinding
	err := utils.ReadAndUnmarshal(&bindingMap, *configPath, "service_bindings.json")
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("WARNING: key map data file '%s' does not exist: \n", "service_bindings.json")
			bindingMap = make(map[string]*model.ServiceBinding)
		} else {
			return nil, errors.New(fmt.Sprintf("Could not load the service instances, message: %s", err.Error()))
		}
	}

	return bindingMap, nil
}

func loadServiceInstances() (map[string]*model.ServiceInstance, error){
	var serviceInstancesMap map[string]*model.ServiceInstance

	err := utils.ReadAndUnmarshal(&serviceInstancesMap, *configPath, "service_instances.json")
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("WARNING: service instance data file '%s' does not exist: \n", "service_instances.json")
			serviceInstancesMap = make(map[string]*model.ServiceInstance)
		} else {
			return nil, errors.New(fmt.Sprintf("Could not load the service instances, message: %s", err.Error()))
		}
	}

	return serviceInstancesMap, nil
}

func logger() (lager.Logger, *lager.ReconfigurableSink) {
	logger, reconfigurableSink := cf_lager.New("nfs-broker")
	return logger, reconfigurableSink
}

func parseCommandLine() {
	cf_lager.AddFlags(flag.CommandLine)
	cf_debug_server.AddFlags(flag.CommandLine)
	flag.Parse()
}