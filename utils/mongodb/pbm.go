package mongodb

import (
	"errors"

	jsoniter "github.com/json-iterator/go"
	"k8s.io/klog/v2"

	pbmexec "deeproute.ai/snapscheduler/utils/exec"
)

type BackupInfo struct {
	Backups StorageStat `json:"backups"`
	Cluster Cluster     `json:"cluster"`
	Pitr    PitrStat    `json:"pitr"`
	Running *CurrOp     `json:"running,omitempty"`
}

// Backups
type StorageStat struct {
	Type     string         `json:"type"`
	Path     string         `json:"path"`
	Region   string         `json:"region,omitempty"`
	Snapshot []SnapshotStat `json:"snapshot"`
	PITR     *pitrRanges    `json:"pitrChunks,omitempty"`
}
type SnapshotStat struct {
	Name       string   `json:"name"`
	Namespaces []string `json:"nss,omitempty"`
	Size       int64    `json:"size,omitempty"`
	Status     string   `json:"status"`
	Err        error    `json:"-"`
	ErrString  string   `json:"error,omitempty"`
	RestoreTS  int64    `json:"restoreTo"`
	PBMVersion string   `json:"pbmVersion"`
	Type       string   `json:"type"`
	SrcBackup  string   `json:"src"`
	StoreName  string   `json:"storage,omitempty"`
}
type pitrRanges struct {
	Ranges []PitrRange `json:"pitrChunks,omitempty"`
	Size   int64       `json:"size"`
}
type PitrRange struct {
	Err            error    `json:"error,omitempty"`
	Range          Timeline `json:"range"`
	NoBaseSnapshot bool     `json:"noBaseSnapshot,omitempty"`
}
type Timeline struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
	Size  int64  `json:"-"`
}

// Cluster
type Cluster []Rs
type Rs struct {
	Name  string `json:"rs"`
	Nodes []Node `json:"nodes"`
}
type Node struct {
	Host     string   `json:"host"`
	Ver      string   `json:"agent"`
	Role     string   `json:"role"`
	PrioPITR string   `json:"prio_pitr"`
	PrioBcp  string   `json:"prio_backup"`
	OK       bool     `json:"ok"`
	Errs     []string `json:"errors,omitempty"`
}

// PITR
type PitrStat struct {
	InConf       bool     `json:"conf"`
	Running      bool     `json:"run"`
	RunningNodes []string `json:"nodes"`
	Err          string   `json:"error,omitempty"`
}

// Running operation
type CurrOp struct {
	Type    string `json:"type,omitempty"`
	OPID    string `json:"opID,omitempty"`
	Name    string `json:"name,omitempty"`
	StartTS int64  `json:"startTS,omitempty"`
	Status  string `json:"status,omitempty"`
}

func PbmRunStatus(executor pbmexec.Executor, opts ...CommandOption) (*BackupInfo, error) {
	baseArgs := []string{"status"}
	args := buildArgs(baseArgs, opts...)
	output, err := NewPbmCommand(executor, args).run()
	if err != nil {
		return nil, err
	}
	backupInfo := &BackupInfo{}
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(output, backupInfo)
	if err != nil {
		return nil, err
	}
	return backupInfo, nil
}

type ExternBcpOut struct {
	Name  string          `json:"name"`
	Nodes []ExternBcpNode `json:"storage"`
}

type ExternBcpNode struct {
	Name  string   `json:"name"`
	Files []string `json:"files"`
}

func PbmRunExternalBackup(executor pbmexec.Executor, opts ...CommandOption) (*ExternBcpOut, error) {
	baseArgs := []string{"backup", "-t", "external", "--wait"}
	args := buildArgs(baseArgs, opts...)
	output, err := NewPbmCommand(executor, args).run()
	if err != nil {
		klog.Errorf("Failed to run pbm external backup %s: %s", output, err)
		return nil, err
	}
	externBcpOut := &ExternBcpOut{}
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(output, externBcpOut)
	if err != nil {
		return nil, err
	}
	return externBcpOut, nil
}

type FinishlBackupOutMsg struct {
	Msg string `json:"msg"`
}

func PbmFinishExternalBackup(executor pbmexec.Executor, backupName string, opts ...CommandOption) (*FinishlBackupOutMsg, error) {
	baseArgs := []string{"backup-finish"}
	if backupName != "" {
		baseArgs = append(baseArgs, backupName)
	}
	args := buildArgs(baseArgs, opts...)
	output, err := NewPbmCommand(executor, args).run()
	if err != nil {
		return nil, err
	}
	outMsg := &FinishlBackupOutMsg{}
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(output, outMsg)
	if err != nil {
		return nil, err
	}
	return outMsg, nil
}

// BackupStatus 表示 MongoDB 备份的整体状态
type BackupStatus struct {
	Name       string    `json:"name"`
	Opid       string    `json:"opid"`
	Type       string    `json:"type"`
	PBMBersion string    `json:"pbm_version"`
	Status     string    `json:"status"`
	Replsets   []Replset `json:"replsets"`
}

// Replset 表示 MongoDB 副本集的状态
type Replset struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Node   string `json:"node"`
}

func PbmDescribeBackup(executor pbmexec.Executor, backupName string, opts ...CommandOption) (*BackupStatus, error) {
	baseArgs := []string{"describe-backup"}
	if backupName == "" {
		return nil, errors.New("backup name is required")
	}
	baseArgs = append(baseArgs, backupName)

	args := buildArgs(baseArgs, opts...)
	output, err := NewPbmCommand(executor, args).run()
	if err != nil {
		return nil, err
	}
	backupStatus := &BackupStatus{}
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(output, backupStatus)
	if err != nil {
		return nil, err
	}
	return backupStatus, nil
}

func PbmDeleteBackup(executor pbmexec.Executor, backupName string, opts ...CommandOption) error {
	baseArgs := []string{"delete-backup", "--yes"}
	if backupName == "" {
		return errors.New("backup name is required")
	}
	baseArgs = append(baseArgs, backupName)

	args := buildArgs(baseArgs, opts...)
	_, err := NewPbmCommand(executor, args).run()
	if err != nil {
		return err
	}
	return nil
}

// PbmCancelBackup cancels a running backup operation by its name. Backup status becomes "canceled".
func PbmCancelBackup(executor pbmexec.Executor, backupName string, opts ...CommandOption) error {
	baseArgs := []string{"cancel-backup"}
	if backupName == "" {
		return errors.New("backup name is required")
	}
	baseArgs = append(baseArgs, backupName)

	args := buildArgs(baseArgs, opts...)
	_, err := NewPbmCommand(executor, args).run()
	if err != nil {
		return err
	}
	return nil
}
