/*
Copyright (C) 2019  The snapscheduler authors

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snapschedulerv1 "deeproute.ai/snapscheduler/api/v1"
	"deeproute.ai/snapscheduler/internal/hooks"
	"deeproute.ai/snapscheduler/utils/exec"
)

// expireByCount deletes the oldest snapshots until the number of snapshots for
// a given PVC (created by the supplied schedule) is no more than the
// schedule's maxCount. This function is the entry point for count-based
// expiration of snapshots.
func expireByCount(ctx context.Context, schedule *snapschedulerv1.SnapshotSchedule,
	logger logr.Logger, c client.Client) error {
	if schedule.Spec.Retention.MaxCount == nil {
		// No count-based retention configured
		return nil
	}

	snapList, err := snapshotsFromSchedule(ctx, schedule, logger, c)
	if err != nil {
		logger.Error(err, "unable to retrieve list of snapshots")
		return err
	}

	grouped := groupSnapsByPVC(snapList)
	for _, backupSnapMp := range grouped {
		backupList := sortSnapsByTime(backupSnapMp)
		if len(backupList) > int(*schedule.Spec.Retention.MaxCount) {
			backupList = backupList[:len(backupList)-int(*schedule.Spec.Retention.MaxCount)]
			list := []snapv1.VolumeSnapshot{}
			for _, l := range backupList {
				list = append(list, l...)
			}
			err := deleteSnapshots(ctx, list, logger, c, schedule)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// expireByTime deletes snapshots that are older than the retention time in the
// specified schedule. It only affects snapshots that were created by the provided schedule.
// This function is the entry point for the time-based expiration of snapshots
func expireByTime(ctx context.Context, schedule *snapschedulerv1.SnapshotSchedule,
	now time.Time, logger logr.Logger, c client.Client) error {
	expiration, err := getExpirationTime(schedule, now, logger)
	if err != nil {
		logger.Error(err, "unable to determine snapshot expiration time")
		return err
	}
	if expiration == nil {
		// No time-based retention configured
		return nil
	}

	snapList, err := snapshotsFromSchedule(ctx, schedule, logger, c)
	if err != nil {
		logger.Error(err, "unable to retrieve list of snapshots")
		return err
	}

	expiredSnaps := filterExpiredSnaps(snapList, *expiration)

	logger.Info("deleting expired snapshots", "expiration", expiration.Format(time.RFC3339),
		"total", len(snapList), "expired", len(expiredSnaps))
	err = deleteSnapshots(ctx, expiredSnaps, logger, c, schedule)
	return err
}

func deleteSnapshots(ctx context.Context, snapshots []snapv1.VolumeSnapshot,
	logger logr.Logger, c client.Client, schedule *snapschedulerv1.SnapshotSchedule) error {
	backupSnapshotsMap := make(map[string][]snapv1.VolumeSnapshot)
	backupSnapCountMap := make(map[string]int)
	for i := range snapshots {
		snap := snapshots[i]
		backupName, ok := snap.Labels[hooks.BackupNameLabelKey]
		if !ok {
			logger.Info("skip deleting snapshot without backup name label", "snapshotName", snap.Name)
			continue
		}
		// format back to original backup name
		backupName = strings.ReplaceAll(backupName, "_", ":")

		backupSnapshotsMap[backupName] = append(backupSnapshotsMap[backupName], snap)
		if _, ok := backupSnapCountMap[backupName]; !ok {
			if backupVolCounts, ok := snap.Labels[hooks.BackupVolumeCountLabelKey]; ok {
				count, err := strconv.Atoi(backupVolCounts)
				if err != nil {
					logger.Error(err, "error parsing backup volume count label", "labelValue", backupVolCounts)
					count = 0
				}
				backupSnapCountMap[backupName] = count
			}
		}
	}
	for backup, snaps := range backupSnapshotsMap {
		if len(snaps) != backupSnapCountMap[backup] {
			logger.Info("skip deleting snapshots for incomplete backup", "backupName", backup, "snapshotCount", len(snaps), "expectedVolumeCount", backupSnapCountMap[backup])
			continue
		}
		for _, snap := range snaps {
			logger.Info("deleted snapshot", "backupName", backup, "snapshotName", snap.Name)
			if err := c.Delete(ctx, &snap, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				if client.IgnoreNotFound(err) == nil {
					logger.Info("snapshot already deleted", "snapshotName", snap.Name)
					continue
				}
				logger.Error(err, "error deleting snapshot", "name", snap.Name)
				return err
			}
		}

		// run OnExpire hook
		if schedule == nil {
			continue
		}
		hook := hooks.GlobalHooksRegistry.GetHook(schedule.Spec.SnapshotTemplate.Service.Type)
		if hook == nil {
			return fmt.Errorf("no hook registered for service type %s", schedule.Spec.SnapshotTemplate.Service.Type)
		}
		scCtx := hooks.SnapshotContext{
			Context:           ctx,
			Schedule:          schedule,
			Executor:          &exec.CommandExecutor{},
			Logger:            logger,
			DeleteBackupNames: []string{backup},
		}
		err := hook.OnExpire(&scCtx)
		if err != nil {
			logger.Error(err, "failed to run OnExpire hook for backup", "backupName", backup)
			return err
		}
		logger.Info("deleted snapshot", "backupName", backup, "count", len(snaps))
	}
	logger.Info("completed deletion of expired snapshots")
	return nil
}

// getExpirationTime returns the cutoff Time for snapshots created with the
// referenced schedule. Any snapshot created prior to the returned time should
// be considered expired.
func getExpirationTime(schedule *snapschedulerv1.SnapshotSchedule,
	now time.Time, logger logr.Logger) (*time.Time, error) {
	if schedule.Spec.Retention.Expires == "" {
		// No time-based retention configured
		return nil, nil
	}

	lifetime, err := time.ParseDuration(schedule.Spec.Retention.Expires)
	if err != nil {
		logger.Error(err, "unable to parse spec.retention.expires")
		return nil, err
	}

	if lifetime < 0 {
		err := errors.New("duration must be greater than 0")
		logger.Error(err, "invalid value for spec.retention.expires")
		return nil, err
	}

	expiration := now.Add(-lifetime).UTC()
	return &expiration, nil
}

// filterExpiredSnaps returns the set of expired snapshots from the provided list.
func filterExpiredSnaps(snaps []snapv1.VolumeSnapshot,
	expiration time.Time) []snapv1.VolumeSnapshot {
	outList := make([]snapv1.VolumeSnapshot, 0)
	for _, snap := range snaps {
		if snap.CreationTimestamp.Time.Before(expiration) {
			outList = append(outList, snap)
		}
	}
	return outList
}

// snapshotsFromSchedule returns a list of snapshots that were created by the
// supplied schedule
func snapshotsFromSchedule(ctx context.Context, schedule *snapschedulerv1.SnapshotSchedule,
	logger logr.Logger, c client.Client) ([]snapv1.VolumeSnapshot, error) {
	labelSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			ScheduleKey: schedule.Name,
		},
	}
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		logger.Error(err, "unable to create label selector for snapshot expiration")
		return nil, err
	}

	listOpts := []client.ListOption{
		client.InNamespace(schedule.Namespace),
		client.MatchingLabelsSelector{
			Selector: selector,
		},
	}
	var snapList snapv1.VolumeSnapshotList
	err = c.List(ctx, &snapList, listOpts...)
	if err != nil {
		logger.Error(err, "unable to retrieve list of snapshots")
		return nil, err
	}

	return snapList.Items, nil
}

// groupSnapsByPVC takes a list of snapshots and groups them by the PVC they
// were created from
func groupSnapsByPVC(snaps []snapv1.VolumeSnapshot) map[string]map[string][]snapv1.VolumeSnapshot {
	// service instance -> backupName -> []snapshots
	groupedSnaps := make(map[string]map[string][]snapv1.VolumeSnapshot)
	for _, snap := range snaps {
		if snap.Spec.Source.PersistentVolumeClaimName == nil {
			continue
		}

		backupName, backupOK := snap.Labels[hooks.BackupNameLabelKey]
		serviceName, instanceOK := snap.Labels[hooks.ServiceLabelKey]
		if backupOK && instanceOK {
			backupName = strings.ReplaceAll(backupName, "_", ":")
		} else {
			// raw volumes
			continue
		}

		if groupedSnaps[serviceName] == nil {
			groupedSnaps[serviceName] = make(map[string][]snapv1.VolumeSnapshot)
		}
		if groupedSnaps[serviceName][backupName] == nil {
			groupedSnaps[serviceName][backupName] = []snapv1.VolumeSnapshot{}
		}
		groupedSnaps[serviceName][backupName] = append(groupedSnaps[serviceName][backupName], snap)
	}

	return groupedSnaps
}

// sortSnapsByTime sorts the snapshots in order of ascending CreationTimestamp
func sortSnapsByTime(snaps map[string][]snapv1.VolumeSnapshot) [][]snapv1.VolumeSnapshot {
	var snapGroups [][]snapv1.VolumeSnapshot
	for _, snapList := range snaps {
		if len(snapList) > 0 {
			snapGroups = append(snapGroups, snapList)
		}
	}
	sort.Slice(snapGroups, func(i, j int) bool {
		return snapGroups[i][0].CreationTimestamp.Before(&snapGroups[j][0].CreationTimestamp)
	})
	return snapGroups
}
