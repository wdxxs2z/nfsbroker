package nfsbroker

import (
	"os"
	"path/filepath"
	"fmt"
	"strings"
	"os/exec"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/goshims/execshim"
	"code.cloudfoundry.org/goshims/os"
	ioutilshim "code.cloudfoundry.org/goshims/ioutil"

	"../utils"
)

const (
	DefaultNfsV3 string = "port=2049,nolock,proto=tcp"
	CellBasePath string = "/var/vcap/data/volumes"
)

type Client interface {
	IsFilesystemMounted(lager.Logger) bool
	MountFileSystem(lager.Logger, string) (string, error)
	CreateShare(lager.Logger, string) (string, error)
	DeleteShare(lager.Logger, string) error
	GetPathForShare(lager.Logger, string) (string, string, error)
	GetConfigDetails(lager.Logger) (string, int, error)
}

type nfsClient struct{
	remoteInfo          string
	remoteMount         string
	version             int
	useFileUtil         ioutilshim.Ioutil
	os                  osshim.Os
	baseLocalMountPoint string
	mounted             bool
	invoker             Invoker
}

func NewNfsClientWithInfokerAndFileUtil(remoteInfo string, remoteMount string,version int, useInvoker Invoker, localMountPoint string, os osshim.Os, useFileUtil ioutilshim.Ioutil) Client{
	return &nfsClient{
		remoteInfo:          remoteInfo,
		remoteMount:         remoteMount,
		version:             version,
		invoker:             useInvoker,
		useFileUtil:         useFileUtil,
		os         :         os,
		mounted:             false,
		baseLocalMountPoint: localMountPoint,
	}
}

func NewNfsClient(remoteInfo string, remoteMount string, version int,localMountPoint string) Client{
	return &nfsClient{
		remoteInfo:          remoteInfo,
		remoteMount:         remoteMount,
		version:             version,
		useFileUtil:         &ioutilshim.IoutilShim{},
		os         :         &osshim.OsShim{},
		mounted:             false,
		baseLocalMountPoint: localMountPoint,
		invoker:             NewRealInvoker(),
	}
}

func (n *nfsClient) IsFilesystemMounted(logger lager.Logger) bool {
	logger = logger.Session("is-filesystem-mounted")
	logger.Info("start...")
	defer logger.Info("end...")
	return n.mounted
}

func (n *nfsClient) MountFileSystem(logger lager.Logger, remoteMountPoint string) (string,error) {
	logger = logger.Session("mount-filesystem")
	logger.Info("start")
	defer logger.Info("end")

	err := n.os.MkdirAll(n.baseLocalMountPoint, os.ModePerm)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to create local director '%s', mount filesystem failed", n.baseLocalMountPoint), err)
		return "",fmt.Errorf("failed to create local director '%s', mount filesystem failed", n.baseLocalMountPoint)
	}

	//Judgement the director mounted. The code and logical must be modify, but now shitty to use it.
	cmd := exec.Command("mountpoint", n.baseLocalMountPoint)
	out, err := cmd.Output()
	if err != nil {
		logger.Error(fmt.Sprintf("warning,failed to command mountpoint '%s'", n.baseLocalMountPoint), err)
		//return "",fmt.Errorf("can't mount the '%s', failed verify the filesystem is mounted", n.baseLocalMountPoint)
	}
	if strings.EqualFold(strings.Replace(string(out), "\n", "", -1), n.baseLocalMountPoint + " is a mountpoint") {
		n.mounted = true
		return n.baseLocalMountPoint, nil
	}

	var cmdArgs []string
	switch n.version {
	case 3:
		cmdArgs = []string{"-o", DefaultNfsV3 , n.remoteInfo + ":" + remoteMountPoint, n.baseLocalMountPoint}
	default:
		cmdArgs = []string{"-t","nfs4" , n.remoteInfo + ":" + remoteMountPoint,n.baseLocalMountPoint}
	}

	err = n.invokeNFS(logger, cmdArgs)
	if err != nil {
		logger.Error("nfs-error", err)
		return "", err
	}
	n.mounted = true
	return n.baseLocalMountPoint, nil
}

func (n *nfsClient) CreateShare(logger lager.Logger, shareName string) (string, error) {
	logger = logger.Session("create-share")
	logger.Info("start")
	defer logger.Info("end")
	logger.Info("share-name", lager.Data{shareName: shareName})
	sharePath := filepath.Join(n.baseLocalMountPoint, shareName)
	err := n.os.MkdirAll(sharePath, os.ModePerm)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to create share '%s'", sharePath), err)
		return "", fmt.Errorf("failed to create share '%s'", sharePath)
	}
	return sharePath, nil
}

func (n *nfsClient) DeleteShare(logger lager.Logger, shareName string) error {
	logger = logger.Session("delete-share")
	logger.Info("start")
	defer logger.Info("end")

	sharePath := filepath.Join(n.baseLocalMountPoint, shareName)
	err := n.os.Remove(sharePath)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to delete share '%s'", sharePath), err)
		return fmt.Errorf("failed to delete share '%s'", sharePath)
	}
	return nil
}

func (n *nfsClient) GetPathForShare(logger lager.Logger, shareName string) (string, string ,error) {
	logger = logger.Session("get-path-for-share")
	logger.Info("start")
	defer logger.Info("end")

	logger.Info("share-name", lager.Data{shareName: shareName})

	shareLocalPath := filepath.Join(n.baseLocalMountPoint, shareName)
	exists := utils.Exists(shareLocalPath, n.os)
	if exists == false {
		return "","", fmt.Errorf("share not found, internal error")
	}

	shareAbsPath := filepath.Join(n.remoteMount, shareName)
	cellPath := filepath.Join(CellBasePath, shareName)
	return shareAbsPath, cellPath ,nil
}

func (n *nfsClient) GetConfigDetails(lager.Logger) (string, int, error) {
	if n.remoteInfo == "" || n.version == 0 {
		return "", 0, fmt.Errorf("Error retreiving Nfs config details")
	}
	return n.remoteInfo, n.version, nil
}

func (n *nfsClient) invokeNFS(logger lager.Logger, args []string) error {
	cmd := "mount"
	logger.Info("invoke-nfs", lager.Data{"cmd": cmd, "args": args})
	defer logger.Debug("done-invoking-nfs")
	return n.invoker.Invoke(logger, cmd, args)
}

type Invoker interface {
	Invoke(logger lager.Logger, executable string, args []string) error
}

type realInvoker struct {
	useExec execshim.Exec
}

func NewRealInvoker() Invoker {
	return NewRealInvokerWithExec(&execshim.ExecShim{})
}

func NewRealInvokerWithExec(useExec execshim.Exec) Invoker {
	return &realInvoker{useExec}
}

func (r *realInvoker) Invoke(logger lager.Logger, executable string, cmdArgs []string) error {
	cmdHandle := r.useExec.Command(executable, cmdArgs...)

	_, err := cmdHandle.StdoutPipe()
	if err != nil {
		logger.Error("unable-to-get-stdout", err)
		return err
	}

	if err = cmdHandle.Start(); err != nil {
		logger.Error("starting command", err)
		return err
	}

	if err = cmdHandle.Wait(); err != nil {
		logger.Error("command-exited", err)
		return err
	}

	// could validate stdout, but defer until actually need it
	return nil
}
