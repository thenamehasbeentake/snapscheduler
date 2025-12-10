package hooks

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "deeproute.ai/snapscheduler/api/v1"
	"deeproute.ai/snapscheduler/utils/exec"
)

// SnapshotContext 在钩子中传递的上下文信息
type SnapshotContext struct {
	Context  context.Context
	Schedule *v1.SnapshotSchedule
	Executor exec.Executor
	PvcList  *corev1.PersistentVolumeClaimList
	Logger   logr.Logger
	// 额外字段按需扩展

	// PreSnapshot
	MatchedPVCs    sets.Set[string]
	SnapshotLabels map[string]string

	// OnExpire
	DeleteBackupNames []string
}

// Hook 是单个钩子的接口（实现可以只关心部分阶段）
type Hook interface {
	Name() v1.ServiceType
	PreSnapshot(ctx *SnapshotContext) error
	PostSnapshot(ctx *SnapshotContext) error
	OnExpire(ctx *SnapshotContext) error
}

var GlobalHooksRegistry = NewRegistry()
