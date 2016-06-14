package nfsbrokerlocal

import (
	"fmt"
	"path"
	"reflect"

	"github.com/wdxxs2z/nfsbroker/model"
	"github.com/wdxxs2z/nfsbroker/utils"
	"github.com/pivotal-golang/lager"
)

const (
	DEFAULT_POLLING_INTERVAL_SECONDS = 10
	DEFAULT_CONTAINER_PATH           = "/var/vcap/data/"
)

type Controller interface {
	GetCatalog(logger lager.Logger) (model.Catalog, error)
	CreateServiceInstance(logger lager.Logger, serverInstanceId string, instance model.ServiceInstance) (model.CreateServiceInstanceResponse, error)
	ServiceInstanceExists(logger lager.Logger, serviceInstanceId string) bool
	ServiceInstancePropertiesMatch(logger lager.Logger, serviceInstanceId string, instance model.ServiceInstance) bool
	DeleteServiceInstance(logger lager.Logger, serviceInstanceId string) error
	BindServiceInstance(logger lager.Logger, serverInstanceId string, bindingId string, bindingInfo model.ServiceBinding) (model.CreateServiceBindingResponse, error)
	ServiceBindingExists(logger lager.Logger, serviceInstanceId string, bindingId string) bool
	ServiceBindingPropertiesMatch(logger lager.Logger, serviceInstanceId string, bindingId string, binding model.ServiceBinding) bool
	GetBinding(logger lager.Logger, serviceInstanceId, bindingId string) (model.ServiceBinding, error)
	UnbindServiceInstance(logger lager.Logger, serviceInstanceId string, bindingId string) error
}

type nfsController struct {
	nfsClient  Client
	instanceMap map[string]*model.ServiceInstance
	bindingMap  map[string]*model.ServiceBinding
	configPath  string
}

func NewController(nfsClient Client, configPath string, instanceMap map[string]*model.ServiceInstance, bindingMap map[string]*model.ServiceBinding) Controller {
	return &nfsController{
		nfsClient:                nfsClient,
		configPath:               configPath,
		instanceMap:              instanceMap,
		bindingMap:               bindingMap,
	}
}

func (n *nfsController) GetCatalog(logger lager.Logger) (model.Catalog, error) {
	logger.Session("get-catalog")
	logger.Info("start")
	defer logger.Info("end")

	plan := model.ServicePlan{
		Name:                     "free",
		Id:                       "free-plan-guid",
		Description:              "free nfs filesystem",
		Metadata:                 nil,
		Free:                     true,
	}

	service := model.Service{
		Name:                     "nfs",
		Id:                       "nfs-service-guid",
		Description:              "Provider the NFS FS volume service,including volume creation and volume mounts",
		Bindable:                 true,
		PlanUpdateable:           false,
		Tags:                     []string{"nfs"},
		Requires:                 []string{"volume_mount"},

		Metadata:                 nil,
		Plans:                    []model.ServicePlan{plan},
		DashboardClient:          nil,
	}

	catalog := model.Catalog{
		Services:                 []model.Service{service},
	}

	return catalog, nil
}

func (n *nfsController) CreateServiceInstance(logger lager.Logger, serverInstanceId string, instance model.ServiceInstance) (model.CreateServiceInstanceResponse, error) {
	logger.Session("create-service-instance")
	logger.Info("start")
	defer logger.Info("end")

	mounted := n.nfsClient.IsFilesystemMounted(logger)
	if !mounted {
		_, err := n.nfsClient.MountFileSystem(logger, "/")
		if err != nil {
			return model.CreateServiceInstanceResponse{}, err
		}
	}

	mountpoint, err := n.nfsClient.CreateShare(logger, serverInstanceId)
	if err != nil {
		return model.CreateServiceInstanceResponse{}, err
	}

	instance.DashboardUrl  = "http://dashboard_url"
	instance.Id            = serverInstanceId
	instance.LastOperation = &model.LastOperation{
		State:                        "in progress",
		Description:                  "creating nfs ervice instance......",
		AsyncPollIntervalSeconds:     DEFAULT_POLLING_INTERVAL_SECONDS,
	}

	n.instanceMap[serverInstanceId] = &instance
	err = utils.MarshalAndRecord(n.instanceMap, n.configPath, "service_instances.json")
	if err != nil {
		return model.CreateServiceInstanceResponse{}, err
	}

	logger.Info("mountpoing-created", lager.Data{mountpoint: mountpoint})
	response := model.CreateServiceInstanceResponse{
		DashboardUrl:                  instance.DashboardUrl,
		LastOperation:                 instance.LastOperation,
	}
	return response, nil
}

func (n *nfsController) ServiceInstanceExists(logger lager.Logger, serviceInstanceId string) bool {
	logger.Session("service-instance-exists")
	logger.Info("start")
	defer logger.Info("end")

	_, exists := n.instanceMap[serviceInstanceId]
	return exists
}

func (n *nfsController) ServiceInstancePropertiesMatch(logger lager.Logger, serviceInstanceId string, instance model.ServiceInstance) bool {
	logger.Session("service-instance-properties-match")
	logger.Info("start")
	defer logger.Info("end")

	existingServiceInstance, exists := n.instanceMap[serviceInstanceId]
	if exists == false {
		return false
	}
	if existingServiceInstance.PlanId != instance.PlanId {
		return false
	}
	if existingServiceInstance.SpaceGuid != instance.SpaceGuid {
		return false
	}
	if existingServiceInstance.OrganizationGuid != instance.OrganizationGuid {
		return false
	}
	areParaEqual := reflect.DeepEqual(existingServiceInstance.Parameters, instance.Parameters)
	return areParaEqual
}

func (n *nfsController) DeleteServiceInstance(logger lager.Logger, serviceInstanceId string) error {
	logger.Session("delete-service-instance")
	logger.Info("start")
	defer logger.Info("end")

	err := n.nfsClient.DeleteShare(logger, serviceInstanceId)
	if err != nil {
		logger.Error("Error delete share",err)
		return err
	}

	delete(n.instanceMap, serviceInstanceId)
	err = utils.MarshalAndRecord(n.instanceMap, n.configPath, "service_instances.json")
	if err != nil {
		return nil
	}
	return nil
}

func (n *nfsController) BindServiceInstance(logger lager.Logger, serverInstanceId string, bindingId string, bindingInfo model.ServiceBinding) (model.CreateServiceBindingResponse, error) {
	logger.Session("bing-service-instance")
	logger.Info("start")
	defer logger.Info("end")
	n.bindingMap[bindingId] = &bindingInfo
	sharePath, err := n.nfsClient.GetPathForShare(logger, serverInstanceId)
	if err != nil {
		return model.CreateServiceBindingResponse{}, err
	}
	containerMountPath := determineContainerMountPath(bindingInfo.Parameters, serverInstanceId)
	remoteInfo, version ,err := n.nfsClient.GetConfigDetails(logger)
	if err != nil {
		return model.CreateServiceBindingResponse{} ,err
	}
	nfsConfig := model.NfsConfig{
		RemoteInfo:               remoteInfo,
		Version:                  version,
		RemoteMountPoint:         sharePath,
	}
	privateDetails := model.VolumeMountPrivateDetails{
		Driver:                   "nfs",
		GroupId:                  serverInstanceId,
		Config:                   nfsConfig,
	}

	volumeMount := model.VolumeMount{
		ContainerPath:            containerMountPath,
		Mode:                     "rw",
		Private:                  privateDetails,
	}

	volumeMounts := []model.VolumeMount{volumeMount}
	creds := model.Credentials{URI: ""}
	createBindingResponse := model.CreateServiceBindingResponse{Credentials: creds, VolumeMounts: volumeMounts}
	utils.MarshalAndRecord(n.bindingMap, n.configPath, "service_bindings.json")
	if err != nil {
		return model.CreateServiceBindingResponse{}, err
	}
	return createBindingResponse, nil
}

func (n *nfsController) ServiceBindingExists(logger lager.Logger, serviceInstanceId string, bindingId string) bool {
	logger = logger.Session("service-binding-exists")
	logger.Info("start")
	defer logger.Info("end")
	_, exists := n.bindingMap[bindingId]
	return exists
}

func (n *nfsController) ServiceBindingPropertiesMatch(logger lager.Logger, serviceInstanceId string, bindingId string, binding model.ServiceBinding) bool {
	logger = logger.Session("service-binding-properties-match")
	logger.Info("start")
	defer logger.Info("end")
	existingBinding, exists := n.bindingMap[bindingId]
	if exists == false {
		return false
	}
	if existingBinding.AppId != binding.AppId {
		return false
	}
	if existingBinding.ServicePlanId != binding.ServicePlanId {
		return false
	}
	if existingBinding.ServiceId != binding.ServiceId {
		return false
	}
	if existingBinding.ServiceInstanceId != binding.ServiceInstanceId {
		return false
	}
	if existingBinding.Id != binding.Id {
		return false
	}
	return true
}

func (n *nfsController) UnbindServiceInstance(logger lager.Logger, serviceInstanceId string, bindingId string) error {
	logger = logger.Session("unbind")
	logger.Info("start")
	defer logger.Info("end")
	delete(n.bindingMap, bindingId)
	err := utils.MarshalAndRecord(n.bindingMap, n.configPath, "service_bindings.json")
	if err != nil {
		logger.Error("error-unbind", err)
		return err
	}
	return nil
}

func (n *nfsController) GetBinding(logger lager.Logger, instanceId, bindingId string) (model.ServiceBinding, error) {
	logger = logger.Session("get-binding")
	logger.Info("start")
	defer logger.Info("end")
	binding, exists := n.bindingMap[bindingId]
	if exists == true {
		return *binding, nil
	}
	return model.ServiceBinding{}, fmt.Errorf("binding not found")
}

func determineContainerMountPath(parameters map[string]interface{}, volId string) string {
	if containerPath, ok := parameters["container_path"]; ok {
		return containerPath.(string)
	}
	if containerPath, ok := parameters["path"]; ok {
		return containerPath.(string)
	}
	return path.Join(DEFAULT_CONTAINER_PATH, volId)
}
