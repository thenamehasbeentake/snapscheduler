package hooks

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/percona/percona-server-mongodb-operator/pkg/naming"

	v1 "deeproute.ai/snapscheduler/api/v1"
	"deeproute.ai/snapscheduler/utils/exec"
	"deeproute.ai/snapscheduler/utils/mongodb"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

const (
	MsgOngoingExternalBackup = "there is an ongoing external backup, skipping new backup to avoid conflicts"
	MsgClusterUnhealthy      = "mongodb cluster is not healthy, skipping backup"

	BackupNameLabelKey        = "snapscheduler.backube/backup-name"
	BackupVolumeCountLabelKey = "snapscheduler.backube/backup-volume-count"
	ServiceLabelKey           = "snapscheduler.backube/service-name"
)

type MongodbHook struct {
	service v1.ServiceType
}

var retryConfig = wait.Backoff{
	Duration: 3 * time.Second,  // 最小执行间隔3秒
	Factor:   1.5,              // 退避因子，每次重试间隔增加1.5倍
	Jitter:   0.1,              // 随机抖动因子，避免惊群效应
	Steps:    10,               // 最大重试次数
	Cap:      30 * time.Second, // 最大重试间隔
}

func (m *MongodbHook) Name() v1.ServiceType {
	return m.service
}

func (m *MongodbHook) PreSnapshot(ctx *SnapshotContext) error {
	executor := ctx.Executor

	// Check pbm status for ongoing backups and cluster health
	backupInfo, err := mongodb.PbmRunStatus(executor, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
	if err != nil {
		klog.Warningf("failed to get pbm status before snapshot: %v", err)
		return err
	}
	if backupInfo.Backups.Snapshot != nil && backupInfo.Backups.Snapshot[0].Type == "external" {
		lastSnap := backupInfo.Backups.Snapshot[0]
		if !m.isBackupFinished(lastSnap.Status) {
			return fmt.Errorf("%s: %+v", MsgOngoingExternalBackup, lastSnap)
		}

		delta, err := m.backupTillNow(lastSnap.Name)
		if err != nil {
			return err
		}
		if delta < 5*time.Minute {
			return fmt.Errorf("too short till last backup %s: %s", lastSnap.Name, delta)
		}
	}
	if backupInfo.Running != nil && backupInfo.Running.Type == "backup" && !m.isBackupFinished(backupInfo.Running.Status) {
		return fmt.Errorf("%s: %+v", MsgOngoingExternalBackup, backupInfo.Running)
	}

	var cluserHostNameOrderMap = make(map[string]string)
	clusterOk := true
	for _, cluster := range backupInfo.Cluster {
		for nodeIndex, node := range cluster.Nodes {
			cluserHostNameOrderMap[node.Host] = cluster.Name + "-" + fmt.Sprintf("%d", nodeIndex)
			if !node.OK {
				clusterOk = false
				break
			}
		}
	}
	if !clusterOk {
		return fmt.Errorf("%s: %+v", MsgClusterUnhealthy, backupInfo.Cluster)
	}

	if backupInfo.Pitr.Err != "" {
		klog.Errorf("mongodb pitr status has error: %s", backupInfo.Pitr.Err)
	}

	// start external backup
	extBcpOut, err := mongodb.PbmRunExternalBackup(executor, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
	if err != nil {
		externalBackup, err := m.dealwithFailedBackup(ctx)
		if err != nil {
			klog.Errorf("failed to run mongodb external backup: %s", err)
			return err
		}
		klog.Infof("Recovered from failed backup, retrying external backup for backup %s", externalBackup)

		describe, err := mongodb.PbmDescribeBackup(executor, externalBackup, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
		if err != nil {
			klog.Errorf("failed to describe backup %s: %s", externalBackup, err)
			return err
		}
		// reconstruct extBcpOut from describe output
		extBcpOut = &mongodb.ExternBcpOut{
			Name:  describe.Name,
			Nodes: []mongodb.ExternBcpNode{},
		}
		for _, rs := range describe.Replsets {
			extBcpOut.Nodes = append(extBcpOut.Nodes, mongodb.ExternBcpNode{
				Name: rs.Node,
			})
		}
	}

	klog.Infof("successful mongodb external backup: %+v", extBcpOut)
	if len(extBcpOut.Nodes) == 0 {
		return errors.New("no backup nodes found in external backup output")
	}

	for _, storage := range extBcpOut.Nodes {
		if hostNameOrder, ok := cluserHostNameOrderMap[storage.Name]; ok {
			// TODO: tune matching logic if needed
			for _, pvc := range ctx.PvcList.Items {
				if pvc.Name != "" && strings.HasSuffix(pvc.Name, hostNameOrder) {
					if replset, ok := pvc.Labels[naming.LabelKubernetesReplset]; !ok || !strings.HasPrefix(hostNameOrder, replset) {
						// for replicaset, need to match the pvc with replset name
						klog.V(5).Infof("pvc %s replset label %s does not match the host %s", pvc.Name, replset, storage.Name)
						continue
					}
					if pvc.Labels[naming.LabelKubernetesInstance] != ctx.Schedule.Spec.SnapshotTemplate.Service.Instance {
						// not the same mongodb instance
						klog.V(5).Infof("pvc %s instance label %s does not match the service instance %s", pvc.Name, pvc.Labels[naming.LabelKubernetesInstance], ctx.Schedule.Spec.SnapshotTemplate.Service.Instance)
						continue
					}
					ctx.MatchedPVCs.Insert(pvc.Name)
					klog.Infof("add pvc %s into matched list", pvc.Name)
				}
			}
		}
	}

	if ctx.SnapshotLabels == nil {
		ctx.SnapshotLabels = make(map[string]string)
	}
	// format backup name to be label friendly, (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')
	labelName := strings.ReplaceAll(extBcpOut.Name, ":", "_")

	ctx.SnapshotLabels[BackupNameLabelKey] = labelName
	ctx.SnapshotLabels[ServiceLabelKey] = string(m.service)
	ctx.SnapshotLabels[BackupVolumeCountLabelKey] = fmt.Sprintf("%d", len(ctx.MatchedPVCs))

	// wait for backup to reach copyReady status
	err = m.waitBackupStatus(executor, extBcpOut.Name, ctx.Schedule.Spec.SnapshotTemplate.Service.URI, mongodb.StatusCopyReady)
	if err != nil {
		klog.Errorf("mongodb backup %s did not reach copyReady status after finish: %s ", extBcpOut.Name, err)
		return err
	}
	klog.Infof("mongodb pre-snapshot hook completed successfully: matched pvc %v", ctx.MatchedPVCs)
	return nil
}

func (m *MongodbHook) dealwithFailedBackup(ctx *SnapshotContext) (string, error) {
	var (
		uncompleteBackups = make([]string, 0)
		executor          = ctx.Executor
		err               error
	)
	newBackupInfo, err := mongodb.PbmRunStatus(executor, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
	if err != nil {
		klog.Errorf("failed to get pbm status: %s", err)
		return "", err
	}
	for _, bcp := range newBackupInfo.Backups.Snapshot {
		if bcp.Type == "external" && !m.isBackupFinished(bcp.Status) {
			uncompleteBackups = append(uncompleteBackups, bcp.Name)
		}
	}
	if len(uncompleteBackups) != 1 {
		return "", fmt.Errorf("expected one uncompleted backup, found %d: %v", len(uncompleteBackups), uncompleteBackups)
	}

	uncompleteBackup := uncompleteBackups[0]

	if uncompleteBackup != "" {
		var delta time.Duration
		delta, err = m.backupTillNow(uncompleteBackup)
		if err != nil {
			return uncompleteBackup, err
		}
		if delta > 10*time.Minute {
			klog.Errorf("uncompleted backup %s is too old (%s), not retrying", uncompleteBackup, delta)
			return uncompleteBackup, fmt.Errorf("uncompleted backup %s is too old (%s), not retrying", uncompleteBackup, delta)
		}

		// Try to wait for it to reach copyReady status
		// Failed with reason `get backup metadata` but backup is actually running or completed
		err = m.waitBackupStatus(executor, uncompleteBackup, ctx.Schedule.Spec.SnapshotTemplate.Service.URI, mongodb.StatusCopyReady)

		if err != nil {
			// Failed again, cancel the backup
			err = mongodb.PbmCancelBackup(executor, uncompleteBackup, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
			if err != nil {
				klog.Errorf("failed to cancel uncompleted backup %s: %s", uncompleteBackup, err)
				return uncompleteBackup, err
			}
			err = m.waitBackupStatus(executor, uncompleteBackup, ctx.Schedule.Spec.SnapshotTemplate.Service.URI, mongodb.StatusCancelled)
			if err != nil {
				klog.Errorf("failed to wait for cancel uncompleted backup status %s: %s", uncompleteBackup, err)
				return uncompleteBackup, err
			}
			err = mongodb.PbmDeleteBackup(executor, uncompleteBackup, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
			if err != nil {
				klog.Errorf("failed to delete uncompleted backup %s: %s", uncompleteBackup, err)
				return uncompleteBackup, err
			}
			return "", fmt.Errorf("uncompleted backup %s cancelled and deleted", uncompleteBackup)
		}
	}
	return uncompleteBackup, nil
}

func (m *MongodbHook) isBackupFinished(status string) bool {
	return !(status == mongodb.StatusCopyReady || status == mongodb.StatusStarting || status == mongodb.StatusRunning || status == mongodb.StatusCopyDone)
}

func (m *MongodbHook) backupTillNow(backupname string) (time.Duration, error) {
	latestTimestamp, err := time.Parse(time.RFC3339, backupname)
	if err != nil {
		klog.Errorf("failed to parse backup name %s as timestamp: %s", backupname, err)
		return 0, err
	}
	return time.Since(latestTimestamp), nil
}

func (m *MongodbHook) PostSnapshot(ctx *SnapshotContext) error {
	executor := ctx.Executor
	backupName, ok := ctx.SnapshotLabels[BackupNameLabelKey]
	if !ok {
		return errors.New("mongodb backup name label not found in snapshot context")
	}
	backupName = strings.ReplaceAll(backupName, "_", ":")

	backupInfo, err := mongodb.PbmRunStatus(executor, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
	if err != nil {
		klog.Errorf("failed to get pbm status after snapshot: %s", err)
		return err
	}

	if backupInfo.Running == nil || backupInfo.Running.Name != backupName || backupInfo.Running.Type != "backup" || backupInfo.Running.Status != mongodb.StatusCopyReady {
		return fmt.Errorf("mongodb backup %s not in expected state to finish: %+v", backupName, backupInfo.Running)
	}

	finishOut, err := mongodb.PbmFinishExternalBackup(executor, backupName, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
	if err != nil {
		klog.Errorf("failed to finish mongodb external backup %s, msg: %s: %s", backupName, finishOut.Msg, err)
		return err
	}
	klog.Infof("mongodb external backup finished: %+v", finishOut)
	err = m.waitBackupStatus(executor, backupName, ctx.Schedule.Spec.SnapshotTemplate.Service.URI, mongodb.StatusDone)
	if err != nil {
		klog.Errorf("mongodb backup %s did not reach done status after finish: %s", backupName, err)
		return err
	}
	klog.Infof("mongodb post-snapshot hook completed successfully")
	return nil
}

func (m *MongodbHook) waitBackupStatus(executor exec.Executor, backupName, uri string, status string) error {
	var desOut *mongodb.BackupStatus
	var err error
	var retryTimes = 0
	retryErr := retry.OnError(retryConfig, func(err error) bool {
		return true
	}, func() error {
		klog.Infof("checking mongodb backup status for backupName %s, retries: %d", backupName, retryTimes)
		retryTimes++
		desOut, err = mongodb.PbmDescribeBackup(executor, backupName, mongodb.WithMongodbURI(uri))
		if err != nil {
			klog.Errorf("failed to describe mongodb backup: %v", err)
			return err
		}
		if desOut.Status == status {
			return nil
		}
		if m.isBackupFinished(desOut.Status) {
			err = fmt.Errorf("mongodb backup %s ended with status %s", backupName, desOut.Status)
			return nil
		}
		errMsg := fmt.Sprintf("mongodb backup %s not done yet, current status: %s", backupName, desOut.Status)
		return errors.New(errMsg)
	})
	if retryErr != nil {
		klog.Errorf("mongodb backup did not reach done status after retries: %s", retryErr)
		return retryErr
	}

	if desOut.Status != status {
		errMsg := fmt.Sprintf("mongodb backup %s completed with status %s, replsets status: %#v", backupName, desOut.Status, desOut.Replsets)
		klog.Errorf("mongodb backup error: %s", errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (m *MongodbHook) OnExpire(ctx *SnapshotContext) error {
	executor := ctx.Executor
	var err error
	for _, backupName := range ctx.DeleteBackupNames {
		err = mongodb.PbmDeleteBackup(executor, backupName, mongodb.WithMongodbURI(ctx.Schedule.Spec.SnapshotTemplate.Service.URI))
		if err != nil {
			klog.Errorf("failed to delete mongodb backup %s: %s", backupName, err)
			break
		}
		klog.Infof("mongodb backup %s deleted successfully", backupName)
	}
	if err != nil {
		return err
	}
	klog.Infof("mongodb expire hook completed successfully")
	return err
}

func init() {
	hook := &MongodbHook{service: v1.ServiceMongodb}
	GlobalHooksRegistry.Register(hook)
}
