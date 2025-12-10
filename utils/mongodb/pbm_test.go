package mongodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"k8s.io/klog/v2"

	exectest "deeproute.ai/snapscheduler/utils/exec/test"
)

func TestPbmCommandsSuite(t *testing.T) {
	suite.Run(t, new(PbmCommandsSuite))
}

type PbmCommandsSuite struct {
	suite.Suite
}

func (s *PbmCommandsSuite) TestPbmRunStatus() {
	output := `
{
  "backups": {
    "type": "S3",
    "path": "http://10.3.11.84:80/test-backup/test-rs-mdb-pbm-shard",
    "region": "us-east-1",
    "snapshot": [
      {
        "name": "2025-12-05T07:07:32Z",
        "status": "copyReady",
        "restoreTo": 1764918461,
        "pbmVersion": "2.9.1",
        "type": "external",
        "src": ""
      },
      {
        "name": "2025-12-01T03:29:15Z",
        "status": StatusDone,
        "restoreTo": 1764559758,
        "pbmVersion": "2.9.1",
        "type": "external",
        "src": ""
      }
    ],
    "pitrChunks": {
      "pitrChunks": [
        {
          "range": {
            "start": 1764558961,
            "end": 1764561114
          },
          "noBaseSnapshot": true
        }
      ],
      "size": 24468996
    }
  },
  "cluster": [
    {
      "rs": "cfg",
      "nodes": [
        {
          "host": "dev-rs-mdb-cfg-0.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
          "agent": "v2.9.1",
          "role": "P",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        },
        {
          "host": "dev-rs-mdb-cfg-1.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
          "agent": "v2.9.1",
          "role": "S",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        },
        {
          "host": "dev-rs-mdb-cfg-2.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
          "agent": "v2.9.1",
          "role": "S",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        }
      ]
    },
    {
      "rs": "rs1",
      "nodes": [
        {
          "host": "10.3.11.250:27017",
          "agent": "v2.9.1",
          "role": "P",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        },
        {
          "host": "10.3.11.251:27017",
          "agent": "v2.9.1",
          "role": "S",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        },
        {
          "host": "10.3.11.253:27017",
          "agent": "v2.9.1",
          "role": "S",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        }
      ]
    },
    {
      "rs": "rs0",
      "nodes": [
        {
          "host": "10.3.11.249:27017",
          "agent": "v2.9.1",
          "role": "P",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        },
        {
          "host": "10.3.11.252:27017",
          "agent": "v2.9.1",
          "role": "S",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        },
        {
          "host": "10.3.11.254:27017",
          "agent": "v2.9.1",
          "role": "S",
          "prio_pitr": "",
          "prio_backup": "",
          "ok": true
        }
      ]
    }
  ],
  "pitr": {
    "conf": true,
    "run": false,
    "nodes": null,
    "error": "2025-12-08T02:47:01.000+0000 E [rs0/10.3.11.254:27017] [pitr] init: catchup: oplog has insufficient range, some records since the last saved ts {1764561116 1} are missing. Run ` + "`pbm backup`" + ` to create a valid starting point for the PITR; 2025-12-08T02:47:00.000+0000 E [rs1/10.3.11.253:27017] [pitr] init: catchup: oplog has insufficient range, some records since the last saved ts {1764561114 1} are missing. Run ` + "`pbm backup`" + ` to create a valid starting point for the PITR; 2025-12-08T02:47:38.000+0000 E [cfg/dev-rs-mdb-cfg-1.dev-rs-mdb-cfg.default.svc.cluster.local:27017] [pitr] init: catchup: oplog has insufficient range, some records since the last saved ts {1764603848 1} are missing. Run ` + "`pbm backup`" + ` to create a valid starting point for the PITR"
  },
  "running": {
    "type": "backup",
    "opID": "693284b442429528828c0fe7",
    "name": "2025-12-05T07:07:32Z",
    "startTS": 1764918453,
    "status": "copyReady"
  }
}
`

	executor := &exectest.MockExecutor{
		MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
			klog.Infof("run command %s %v", command, args)
			return output, nil
		},
	}
	backupInfo, err := PbmRunStatus(executor)
	s.NoError(err)
	expectPbmStatus := &BackupInfo{
		Backups: StorageStat{
			Type:   "S3",
			Path:   "http://10.3.11.84:80/test-backup/test-rs-mdb-pbm-shard",
			Region: "us-east-1",
			Snapshot: []SnapshotStat{
				{
					Name:       "2025-12-05T07:07:32Z",
					Status:     "copyReady",
					RestoreTS:  1764918461,
					PBMVersion: "2.9.1",
					Type:       "external",
				},
				{
					Name:       "2025-12-01T03:29:15Z",
					Status:     StatusDone,
					RestoreTS:  1764559758,
					PBMVersion: "2.9.1",
					Type:       "external",
				},
			},
			PITR: &pitrRanges{
				Ranges: []PitrRange{
					{
						Range: Timeline{
							Start: 1764558961,
							End:   1764561114,
						},
						NoBaseSnapshot: true,
					},
				},
				Size: 24468996,
			},
		},
		Cluster: Cluster{
			{
				Name: "cfg",
				Nodes: []Node{
					{
						Host: "dev-rs-mdb-cfg-0.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
						Ver:  "v2.9.1",
						Role: "P",
						OK:   true,
					},
					{
						Host: "dev-rs-mdb-cfg-1.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
						Ver:  "v2.9.1",
						Role: "S",
						OK:   true,
					},
					{
						Host: "dev-rs-mdb-cfg-2.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
						Ver:  "v2.9.1",
						Role: "S",
						OK:   true,
					},
				},
			},
			{
				Name: "rs1",
				Nodes: []Node{
					{
						Host: "10.3.11.250:27017",
						Ver:  "v2.9.1",
						Role: "P",
						OK:   true,
					},
					{
						Host: "10.3.11.251:27017",
						Ver:  "v2.9.1",
						Role: "S",
						OK:   true,
					},
					{
						Host: "10.3.11.253:27017",
						Ver:  "v2.9.1",
						Role: "S",
						OK:   true,
					},
				},
			},
			{
				Name: "rs0",
				Nodes: []Node{
					{
						Host: "10.3.11.249:27017",
						Ver:  "v2.9.1",
						Role: "P",
						OK:   true,
					},
					{
						Host: "10.3.11.252:27017",
						Ver:  "v2.9.1",
						Role: "S",
						OK:   true,
					},
					{
						Host: "10.3.11.254:27017",
						Ver:  "v2.9.1",
						Role: "S",
						OK:   true,
					},
				},
			},
		},
		Pitr: PitrStat{
			InConf:       true,
			Running:      false,
			RunningNodes: nil, // JSON 中的 null
			Err:          "2025-12-08T02:47:01.000+0000 E [rs0/10.3.11.254:27017] [pitr] init: catchup: oplog has insufficient range, some records since the last saved ts {1764561116 1} are missing. Run `pbm backup` to create a valid starting point for the PITR; 2025-12-08T02:47:00.000+0000 E [rs1/10.3.11.253:27017] [pitr] init: catchup: oplog has insufficient range, some records since the last saved ts {1764561114 1} are missing. Run `pbm backup` to create a valid starting point for the PITR; 2025-12-08T02:47:38.000+0000 E [cfg/dev-rs-mdb-cfg-1.dev-rs-mdb-cfg.default.svc.cluster.local:27017] [pitr] init: catchup: oplog has insufficient range, some records since the last saved ts {1764603848 1} are missing. Run `pbm backup` to create a valid starting point for the PITR",
		},
		Running: &CurrOp{
			Type:    "backup",
			OPID:    "693284b442429528828c0fe7",
			Name:    "2025-12-05T07:07:32Z",
			StartTS: 1764918453,
			Status:  "copyReady",
		},
	}
	s.EqualValues(expectPbmStatus, backupInfo)
}

func (s *PbmCommandsSuite) TestPbmRunExternalBackup() {
	output := `
{
  "name": "2025-12-09T04:03:09Z",
  "storage": [
    {
      "name": "10.3.11.251:27017",
      "files": null
    },
    {
      "name": "10.3.11.254:27017",
      "files": null
    },
    {
      "name": "dev-rs-mdb-cfg-1.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
      "files": null
    }
  ]
}
`

	executor := &exectest.MockExecutor{
		MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
			klog.Infof("run command %s %v", command, args)
			return output, nil
		},
	}
	externBcpOut, err := PbmRunExternalBackup(executor)
	s.NoError(err)
	expectedExternBcpOut := &ExternBcpOut{
		Name: "2025-12-09T04:03:09Z",
		Nodes: []ExternBcpNode{
			{
				Name: "10.3.11.251:27017",
			},
			{
				Name: "10.3.11.254:27017",
			},
			{
				Name: "dev-rs-mdb-cfg-1.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
			},
		},
	}

	s.EqualValues(externBcpOut, expectedExternBcpOut)
}

func (s *PbmCommandsSuite) TestPbmFinishExternalBackup() {
	output := `
{
  "msg": "Command sent. Check ` + "`pbm describe-backup 2025-12-09T04:03:09Z`" + ` for the result."
}
`

	executor := &exectest.MockExecutor{
		MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
			klog.Infof("run command %s %v", command, args)
			return output, nil
		},
	}
	externBcpOut, err := PbmFinishExternalBackup(executor, "2025-12-09T04:03:09Z")
	s.NoError(err)
	expectMsg := &FinishlBackupOutMsg{
		Msg: "Command sent. Check `pbm describe-backup 2025-12-09T04:03:09Z` for the result.",
	}
	s.EqualValues(expectMsg, externBcpOut)
}

func (s *PbmCommandsSuite) TestbmDescribeBackup() {
	output := `
{
  "name": "2025-12-09T04:03:09Z",
  "opid": "69379f7d7eca9def9c4a124c",
  "type": "external",
  "last_write_ts": 1765252991,
  "last_transition_ts": 1765253409,
  "last_write_time": "2025-12-09T04:03:11Z",
  "last_transition_time": "2025-12-09T04:10:09Z",
  "mongodb_version": "6.0.24-19",
  "fcv": "6.0",
  "pbm_version": "2.9.1",
  "status": StatusDone,
  "size": 0,
  "size_h": "0 B",
  "replsets": [
    {
      "name": "rs1",
      "status": StatusDone,
      "node": "10.3.11.251:27017",
      "last_write_ts": 1765252983,
      "last_transition_ts": 1765253408,
      "last_write_time": "2025-12-09T04:03:03Z",
      "last_transition_time": "2025-12-09T04:10:08Z",
      "security": {
        "enableEncryption": false,
        "encryptionKeyFile": "/etc/mongodb-encryption/encryption-key",
        "relaxPermChecks": true
      }
    },
    {
      "name": "rs0",
      "status": StatusDone,
      "node": "10.3.11.254:27017",
      "last_write_ts": 1765252988,
      "last_transition_ts": 1765253407,
      "last_write_time": "2025-12-09T04:03:08Z",
      "last_transition_time": "2025-12-09T04:10:07Z",
      "security": {
        "enableEncryption": false,
        "encryptionKeyFile": "/etc/mongodb-encryption/encryption-key",
        "relaxPermChecks": true
      }
    },
    {
      "name": "cfg",
      "status": StatusDone,
      "node": "dev-rs-mdb-cfg-1.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
      "last_write_ts": 1765252991,
      "last_transition_ts": 1765253408,
      "last_write_time": "2025-12-09T04:03:11Z",
      "last_transition_time": "2025-12-09T04:10:08Z",
      "configsvr": true,
      "security": {
        "enableEncryption": false,
        "encryptionKeyFile": "/etc/mongodb-encryption/encryption-key",
        "relaxPermChecks": true
      }
    }
  ]
}
`

	executor := &exectest.MockExecutor{
		MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
			klog.Infof("run command %s %v", command, args)
			return output, nil
		},
	}
	status, err := PbmDescribeBackup(executor, "2025-12-09T04:03:09Z")
	s.NoError(err)
	expectStatus := &BackupStatus{
		Name:       "2025-12-09T04:03:09Z",
		Opid:       "69379f7d7eca9def9c4a124c",
		Type:       "external",
		PBMBersion: "2.9.1",
		Status:     StatusDone,
		Replsets: []Replset{
			{
				Name:   "rs1",
				Status: StatusDone,
				Node:   "10.3.11.251:27017",
			},
			{
				Name:   "rs0",
				Status: StatusDone,
				Node:   "10.3.11.254:27017",
			},
			{
				Name:   "cfg",
				Status: StatusDone,
				Node:   "dev-rs-mdb-cfg-1.dev-rs-mdb-cfg.default.svc.cluster.local:27017",
			},
		},
	}
	s.EqualValues(expectStatus, status)
}
