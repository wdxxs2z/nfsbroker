package nfsbroker

import (
	"code.cloudfoundry.org/voldriver"
	"github.com/pivotal-cf/brokerapi"
	"code.cloudfoundry.org/lager"
	"strings"
)

type BindResponse struct {
	voldriver.ErrorResponse
	SharedDevice brokerapi.SharedDevice
}

type Controller interface {
	voldriver.Provisioner
	Bind(logger lager.Logger, instanceID string) BindResponse
}

type controller struct {
	nfsClient Client
}

func NewController(nfsClient Client) Controller{
	return &controller{nfsClient:nfsClient}
}

func (c *controller) Create(logger lager.Logger, createRequest voldriver.CreateRequest) voldriver.ErrorResponse {
	logger = logger.Session("provision")
	logger.Info("start")
	defer logger.Info("end")

	mounted := c.nfsClient.IsFilesystemMounted(logger)
	if !mounted {
		_, err := c.nfsClient.MountFileSystem(logger, "/")
		if err != nil {
			return voldriver.ErrorResponse{Err: err.Error()}
		}
	}
	mountpoint, err := c.nfsClient.CreateShare(logger, createRequest.Name)
	if err != nil {
		return voldriver.ErrorResponse{Err: err.Error()}
	}
	logger.Info("mountpoint-created", lager.Data{mountpoint: mountpoint})
	return voldriver.ErrorResponse{}
}

func (c *controller) Remove(logger lager.Logger, removeRequest voldriver.RemoveRequest) voldriver.ErrorResponse{
	logger = logger.Session("remove")
	logger.Info("start")
	defer logger.Info("end")

	err := c.nfsClient.DeleteShare(logger, removeRequest.Name)
	if err != nil {
		logger.Error("Error deleting share", err)
		return voldriver.ErrorResponse{Err:err.Error()}
	}
	return voldriver.ErrorResponse{}
}

func (c *controller) Bind(logger lager.Logger, instanceID string) BindResponse{
	logger = logger.Session("bind-service-instance")
	logger.Info("start")
	defer logger.Info("end")
	response := BindResponse{}

	remoteSharePath, localPath , err := c.nfsClient.GetPathForShare(logger, instanceID)
	if err != nil {
		logger.Error("failed-getting-paths-for-share",err)
		response.Err = err.Error()
		return response
	}
	remoteInfo, version ,err := c.nfsClient.GetConfigDetails(logger)
	if err != nil {
		logger.Error("failed-to-determine-container-mountpath", err)
		response.Err = err.Error()
		return response
	}
	return BindResponse{
		SharedDevice: brokerapi.SharedDevice{
			VolumeId: instanceID,
			MountConfig: map[string]interface{}{
				"remote_info"       : strings.Split(remoteInfo,":")[0],
				"version"           : version,
				"remote_mountpoint" : remoteSharePath,
				"local_mountpoint"  : localPath,
			},
		},
	}
}
