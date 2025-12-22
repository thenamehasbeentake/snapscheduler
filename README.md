# SnapScheduler for db
部署方式与原文档相同，crds 参考 config/samples/snapscheduler_mongodb.yaml

新增 snapshotTemplate.service 字段，原项目会根据 SnapshotSchedule 所在 ns ， 扫描所有匹配的 pvc。
新功能会根据 snapshotTemplate.service 相关字段，扫描匹配服务的 pvc，并在打快照前后执行与服务相关的逻辑。
- claimSelector: 填写 matchLabels 用于匹配服务
- snapshotTemplate
  - service
    - type: 支持的服务类型
    - uri: 连接服务的 uri
    - instance: 具体服务实例

# SnapScheduler

[![Build
Status](https://deeproute.ai/snapscheduler/workflows/Tests/badge.svg)](https://deeproute.ai/snapscheduler/actions?query=branch%3Amaster+workflow%3ATests+)
[![Go Report
Card](https://goreportcard.com/badge/deeproute.ai/snapscheduler)](https://goreportcard.com/report/deeproute.ai/snapscheduler)
[![codecov](https://codecov.io/gh/backube/snapscheduler/branch/master/graph/badge.svg)](https://codecov.io/gh/backube/snapscheduler)

SnapScheduler provides scheduled snapshots for Kubernetes CSI-based volumes.

## Quickstart

Install:

```console
$ helm repo add backube https://backube.github.io/helm-charts/
"backube" has been added to your repositories

$ kubectl create namespace backube-snapscheduler
namespace/backube-snapscheduler created

$ helm install -n backube-snapscheduler snapscheduler backube/snapscheduler
NAME: snapscheduler
LAST DEPLOYED: Mon Jul  6 15:16:41 2020
NAMESPACE: backube-snapscheduler
STATUS: deployed
...
```

Keep 6 hourly snapshots of all PVCs in `mynamespace`:

```console
$ kubectl -n mynamespace apply -f - <<EOF
apiVersion: snapscheduler.backube/v1
kind: SnapshotSchedule
metadata:
  name: hourly
spec:
  retention:
    maxCount: 6
  schedule: "0 * * * *"
  claimSelector: 
    matchLabels:
      app.kubernetes.io/instance: dev-rs-mdb
      app.kubernetes.io/name: percona-server-mongodb
  
  disabled: false
  snapshotTemplate:
    service:
      type: percona-server-mongodb
      uri: "mongodb://backup:vccexxcdDZ7SMMuKr@10.3.11.253:27017"
      instance: "dev-rs-mdb"
    snapshotClassName: "csi-rbdplugin-snapclass"
    labels:
      createdBy: snapscheduler
EOF

snapshotschedule.snapscheduler.backube/hourly created
```

In this example, there is 1 PVC in the namespace, named `data`:

```console
$ kubectl -n mynamespace get pvc
NAME   STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS      AGE
data   Bound    pvc-c2e044ab-1b24-496a-9569-85f009892ccf   1Gi        RWO            csi-hostpath-sc   9s
```

At the top of each hour, a snapshot of that volume will be automatically
created:

```console
$ kubectl -n mynamespace get volumesnapshots
NAME                       AGE
data-hourly-202007061600   82m
data-hourly-202007061700   22m
```

## More information

Interested in giving it a try? [Check out the
docs.](https://backube.github.io/snapscheduler/)

The operator can be installed from:

- [Artifact
  Hub](https://artifacthub.io/packages/helm/backube-helm-charts/snapscheduler)
- [OperatorHub.io](https://operatorhub.io/operator/snapscheduler)

Other helpful links:

- [SnapScheduler Changelog](CHANGELOG.md)
- [Contributing guidelines](https://github.com/backube/.github/blob/master/CONTRIBUTING.md)
- [Organization code of conduct](https://github.com/backube/.github/blob/master/CODE_OF_CONDUCT.md)

## Licensing

This project is licensed under the [GNU AGPL 3.0 License](LICENSE) with the following
exceptions:

- The files within the `api/*` directories are additionally licensed under
  Apache License 2.0. This is to permit SnapScheduler's CustomResource types to
  be used by a wider range of software.
- Documentation is made available under the [Creative Commons
  Attribution-ShareAlike 4.0 International license (CC BY-SA
  4.0)](https://creativecommons.org/licenses/by-sa/4.0/)
