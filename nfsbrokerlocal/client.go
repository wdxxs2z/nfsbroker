package nfsbrokerlocal

import (
	"os"
	"path/filepath"
	"github.com/wdxxs2z/nfsbroker/utils"
	"io/ioutil"

	"github.com/pivotal-golang/lager"
	"github.com/cloudfoundry/gunk/os_wrap/exec_wrap"
	"fmt"
	"strings"
	"os/exec"
)

const (
	DefaultNfsV3 = "port=2049,nolock,proto=tcp"
)

type Client interface {
	IsFilesystemMounted(lager.Logger) bool
	MountFileSystem(lager.Logger, string) (string, error)
	CreateShare(lager.Logger, string) (string, error)
	DeleteShare(lager.Logger, string) error
	GetPathForShare(lager.Logger, string) (string, error)
	GetConfigDetails(lager.Logger) (string, int, error)
}

type nfsClient struct{
	remoteInfo          string
	remoteMount         string
	version             int
	useFileUtil         FileUtil
	baseLocalMountPoint string
	mounted             bool
	invoker             Invoker
}

func NewNfsClientWithInfokerAndFileUtil(remoteInfo string, remoteMount string,version int, useInvoker Invoker, useFileUtil FileUtil, localMountPoint string) Client{
	return &nfsClient{
		remoteInfo:          remoteInfo,
		remoteMount:         remoteMount,
		version:             version,
		invoker:             useInvoker,
		useFileUtil:         useFileUtil,
		mounted:             false,
		baseLocalMountPoint: localMountPoint,
	}
}

func NewNfsClient(remoteInfo string, remoteMount string, version int,localMountPoint string) Client{
	return &nfsClient{
		remoteInfo:          remoteInfo,
		remoteMount:         remoteMount,
		version:             version,
		mounted:             false,
		baseLocalMountPoint: localMountPoint,
		invoker:             NewRealInvoker(),
		useFileUtil:         NewRealFileUtil(),
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

	err := n.useFileUtil.MkdirAll(n.baseLocalMountPoint, os.ModePerm)
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
	err := n.useFileUtil.MkdirAll(sharePath, os.ModePerm)
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
	err := n.useFileUtil.Remove(sharePath)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to delete share '%s'", sharePath), err)
		return fmt.Errorf("failed to delete share '%s'", sharePath)
	}
	return nil
}

func (n *nfsClient) GetPathForShare(logger lager.Logger, shareName string) (string, error) {
	logger = logger.Session("get-path-for-share")
	logger.Info("start")
	defer logger.Info("end")
	logger.Info("share-name", lager.Data{shareName: shareName})
	shareAbsPath := filepath.Join(n.baseLocalMountPoint, shareName)
	exists := n.useFileUtil.Exists(shareAbsPath)
	if exists == false {
		return "", fmt.Errorf("share not found, internal error")
	}
	return shareAbsPath, nil
}

func (n *nfsClient) GetConfigDetails(lager.Logger) (string, int, error) {
	if n.remoteInfo == "" || n.version == 0 {
		return "", 0, fmt.Errorf("Error retreiving Nfs config details")
	}
	return n.remoteInfo, n.version, nil
}

func (n *nfsClient) invokeNFS(logger lager.Logger, args []string) error {
	cmd := "mount"
	return n.invoker.Invoke(logger, cmd, args)
}

type FileUtil interface {
	MkdirAll(filepath string, perm os.FileMode) error
	WriteFile(filename string, data []byte, perm os.FileMode) error
	ReadFile(filepath string) ([]byte, error)
	Remove(filepath string) error
	Exists(filepath string) bool
}

type realFileUtil struct {}

func NewRealFileUtil() FileUtil{
	return &realFileUtil{}
}

func (f *realFileUtil) MkdirAll(filepath string, perm os.FileMode) error{
	return os.MkdirAll(filepath, perm)
}

func (f *realFileUtil) WriteFile(filename string, data []byte, perm os.FileMode) error {
	return ioutil.WriteFile(filename, data, perm)
}

func (f *realFileUtil) ReadFile(filename string) ([]byte, error) {
	return utils.ReadFile(filename)
}

func (f *realFileUtil) Remove(filepath string) error {
	return os.RemoveAll(filepath)
}

func (f *realFileUtil) Exists(filepath string) bool {
	if _, err := os.Stat(filepath) ; os.IsNotExist(err){
		return false
	}
	return true
}

type Invoker interface {
	Invoke(logger lager.Logger, executable string, args []string) error
}

type realInvoker struct {
	useExec exec_wrap.Exec
}

func NewRealInvoker() Invoker {
	return NewRealInvokerWithExec(exec_wrap.NewExec())
}

func NewRealInvokerWithExec(useExec exec_wrap.Exec) Invoker {
	return &realInvoker{useExec}
}

func (r *realInvoker) Invoke(logger lager.Logger, executable string, cmdArgs []string) error {
	cmdHandle := r.useExec.Command(executable, cmdArgs...)

	_, err := cmdHandle.StdoutPipe()
	if err != nil {
		logger.Error("unable to get stdout", err)
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