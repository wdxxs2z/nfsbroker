package nfsbroker

import (
	"github.com/pivotal-cf/brokerapi"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/goshims/ioutil"
	"sync"
	"path/filepath"
	"fmt"
	"encoding/json"
	"os"
	"reflect"
	"code.cloudfoundry.org/voldriver"
	"errors"
	"path"
)

const (
	DefaultContainerDir = "/var/vcap/data"
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
)

type lock interface {
	Lock()
	Unlock()
}

type serviceDetails struct {
	ServiceName string     `json:"ServiceName"`
	ServiceId   string     `json:"ServiceId"`
	PlanName    string     `json:"PlanName"`
	PlanId      string     `json:"PlanId"`
	PlanDesc    string     `json:"PlanDesc"`
	DisplayName string     `json:"displayName,omitempty"`
	ImageUrl    string     `json:"imageUrl,omitempty"`
}

type serviceMap struct {
	InstanceMap map[string]brokerapi.ProvisionDetails
	BindingMap map[string]brokerapi.BindDetails
}

type broker struct {
	logger          lager.Logger
	controller      Controller
	dataDir         string
	ioutil          ioutilshim.Ioutil
	mutex           lock
	sd              serviceDetails
	sm              serviceMap
}

func New(logger lager.Logger, controller Controller, serviceName,serviceId,planName,planId,planDesc,displayName,imageUrl,dataDir string,ioutil ioutilshim.Ioutil) *broker {
	selfBroker := broker{
		logger:      logger,
		controller:  controller,
		dataDir:     dataDir,
		ioutil:      ioutil,
		mutex:       &sync.Mutex{},
		sd:          serviceDetails{serviceName,serviceId,planName,planId,planDesc,displayName,imageUrl},
		sm:          serviceMap{
			InstanceMap: map[string]brokerapi.ProvisionDetails{},
			BindingMap : map[string]brokerapi.BindDetails{},
		},
	}
	selfBroker.restoreServiceMap()
	return &selfBroker
}

//https://github.com/pivotal-cf/brokerapi/blob/master/catalog.go
func (b *broker) Services() []brokerapi.Service {
	logger := b.logger.Session("services")
	logger.Info("start")
	defer logger.Info("end")

	return []brokerapi.Service{{
		ID:            b.sd.ServiceId,
		Name:          b.sd.ServiceName,
		Description:   fmt.Sprintf("%s service docs: https://github.com/wdxxs2z/%s-init", b.sd.ServiceName, b.sd.ServiceName),
		Bindable:      true,
		Tags:          []string{b.sd.ServiceName},
		PlanUpdatable: false,
		Plans:         []brokerapi.ServicePlan{{
			ID:          b.sd.PlanId,
			Name:        b.sd.PlanName,
			Description: b.sd.PlanDesc,
			Free:        new(bool), //not is true,is new(bool)
		}},
		Requires:      []brokerapi.RequiredPermission{PermissionVolumeMount},
	}}
}
//https://github.com/pivotal-cf/brokerapi/blob/0ea2a3913c148837e8615a1ef8bde757151934c3/api.go#L77
func (b *broker) Provision(instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, error) {
	logger := b.logger.Session("privision")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.serialize(b.sm)

	if b.instanceConflicts(details, instanceID) {
		logger.Error("instance-already-exists", brokerapi.ErrInstanceAlreadyExists)
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}

	//create service instances
	errResp := b.controller.Create(logger, voldriver.CreateRequest{
		Name:    instanceID,
		Opts:    map[string]interface{}{"volume_id": instanceID} ,
	})

	if errResp.Err != "" {
		err := errors.New(errResp.Err)
		logger.Error("provision-create-failed", err)
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	b.sm.InstanceMap[instanceID] = details
	return brokerapi.ProvisionedServiceSpec{}, nil
}

//https://github.com/pivotal-cf/brokerapi/blob/0ea2a3913c148837e8615a1ef8bde757151934c3/api.go#L194
func (b *broker) Deprovision(instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.DeprovisionServiceSpec, error){
	logger := b.logger.Session("deprovision")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.serialize(b.sm)

	if _,ok := b.sm.InstanceMap[instanceID];!ok {
		return brokerapi.DeprovisionServiceSpec{},brokerapi.ErrInstanceDoesNotExist
	}

	errResp := b.controller.Remove(logger, voldriver.RemoveRequest{
		Name:  instanceID,
	})

	if errResp.Err != "" {
		err := errors.New(errResp.Err)
		logger.Error("provision-remove-failed", err)
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	delete(b.sm.InstanceMap, instanceID)

	return brokerapi.DeprovisionServiceSpec{}, nil
}

//https://github.com/pivotal-cf/brokerapi/blob/0ea2a3913c148837e8615a1ef8bde757151934c3/api.go#L235
func (b *broker) Bind(instanceID string, bindId string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
	logger := b.logger.Session("binding")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.serialize(b.sm)

	if _,ok := b.sm.InstanceMap[instanceID]; !ok {
		return brokerapi.Binding{}, brokerapi.ErrInstanceDoesNotExist
	}

	if details.AppGUID == "" {
		return brokerapi.Binding{}, brokerapi.ErrAppGuidNotProvided
	}

	mode, err := evaluateMode(details.Parameters)
	if err != nil {
		return brokerapi.Binding{},err
	}

	if b.bindingConflicts(bindId, details) {
		return brokerapi.Binding{}, brokerapi.ErrBindingAlreadyExists
	}

	resp := b.controller.Bind(logger, instanceID)
	if resp.Err != "" {
		err := errors.New(resp.Err)
		logger.Error("binding-service-failed", err)
		return brokerapi.Binding{}, err
	}

	b.sm.BindingMap[bindId] = details

	return brokerapi.Binding{
		Credentials:      struct {}{},
		VolumeMounts:     []brokerapi.VolumeMount{{
			Driver:         fmt.Sprintf("%sdriver",b.sd.ServiceName),
			ContainerDir:   evaluateContainerDir(details.Parameters, instanceID),
			Mode:           mode,
			DeviceType:     "shared",
			Device:         resp.SharedDevice,
		}},
	}, nil
}

//https://github.com/pivotal-cf/brokerapi/blob/0ea2a3913c148837e8615a1ef8bde757151934c3/api.go#L284
func (b *broker) Unbind(instanceID ,bindingID string, details brokerapi.UnbindDetails) error {
	logger := b.logger.Session("binding")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.serialize(b.sm)

	if _,ok := b.sm.InstanceMap[instanceID]; !ok {
		return brokerapi.ErrInstanceDoesNotExist
	}

	if _,ok := b.sm.BindingMap[bindingID]; !ok {
		return brokerapi.ErrBindingDoesNotExist
	}

	delete(b.sm.BindingMap, bindingID)
	return nil
}

func (b *broker) Update(instanceID string, details brokerapi.UpdateDetails, asyncAllowd bool) (brokerapi.UpdateServiceSpec, error) {
	panic("not implemented")
}

func (b *broker) LastOperation(instanceID,operationData string) (brokerapi.LastOperation, error) {
	panic("not implemented")
}

func evaluateContainerDir(parameters map[string]interface{}, volID string) string {
	if containerDir, ok := parameters["mount"];ok  && containerDir != "" {
		return containerDir.(string)
	}
	return path.Join(DefaultContainerDir, volID)
}

func (b *broker) bindingConflicts(bindingID string, details brokerapi.BindDetails) bool {
	if existed, ok := b.sm.BindingMap[bindingID]; ok {
		if !reflect.DeepEqual(details, existed){
			return true
		}
	}
	return false
}

func evaluateMode(parameters map[string]interface{}) (string, error) {
	if ro, ok := parameters["readonly"]; ok {
		switch ro := ro.(type) {
		case bool:
			return readOnlyToMode(ro),nil
		default:
			return "", brokerapi.ErrRawParamsInvalid
		}
	}
	return "rw", nil
}

func readOnlyToMode(ro bool) string {
	if ro {
		return "r"
	}
	return "rw"
}

func (b *broker) instanceConflicts(details brokerapi.ProvisionDetails, instanceID string) bool {
	if existing, ok := b.sm.InstanceMap[instanceID]; ok {
		if !reflect.DeepEqual(details,existing) {
			return true
		}
	}
	return false
}

func (b *broker) serialize(serviceMode interface{}) {
	logger := b.logger.Session("serialize")
	logger.Info("start")
	defer logger.Info("end")

	serviceFile := filepath.Join(b.dataDir, fmt.Sprintf("%s-services.json", b.sd.ServiceName))
	serviceData, err := json.Marshal(serviceMode)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-marshall-service-file: %s", serviceFile), err)
		return
	}
	err = b.ioutil.WriteFile(serviceFile, serviceData, os.ModePerm)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-write-service-file: %s", serviceFile), err)
		return
	}
	logger.Info("service-file-saved", lager.Data{"service-file" : serviceFile})
}

func (b *broker) restoreServiceMap() {
	logger := b.logger.Session("restore-services")
	logger.Info("start")
	defer logger.Info("end")

	detailsFile := filepath.Join(b.dataDir, fmt.Sprintf("%s-services.json", b.sd.ServiceName))
	serviceData, err := b.ioutil.ReadFile(detailsFile)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-read-state-file: %s", detailsFile), err)
		return
	}
	serviceMap := serviceMap{}
	err = json.Unmarshal(serviceData, &serviceMap)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-unmarshall-state from state-file: %s", detailsFile), err)
		return
	}
	logger.Info("state-restored", lager.Data{"details-file":detailsFile})
	b.sm = serviceMap
}
