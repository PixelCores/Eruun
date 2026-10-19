# Job 组件示例

该目录提供 `job`、`cloudjob` 组件的示例请求体，以及用于集群验证的脚本。

## 文件

- `instant-job.json`：job 即时执行示例（Job）。
- `scheduled-job-start-time.json`：job startTime 一次性延迟示例。
- `cloudjob-skeleton.json`：cloudjob 基础骨架示例。
- `cloudjob-tenant-bootstrap.json`：租户云资源引导链路示例（文件系统 -> 挂载点 -> StorageClass）。

## 快速验证

```bash
curl -X POST http://localhost:8000/api/v1/settings \
  -H "Content-Type: application/json" \
  -d @examples/system-setting/create-aliyun-cloud-setting.json

curl -X POST http://localhost:8000/api/v1/applications/try \
  -H "Content-Type: application/json" \
  -d @examples/job-components/cloudjob-tenant-bootstrap.json
```

说明：

- 先把 `examples/system-setting/create-aliyun-cloud-setting.json` 里的密钥和网络拓扑占位值改成真实值。
- aliyun `cloudjob` 的 `regionId`、`zoneId`、`vpcId`、`vswId` 统一来自 `system_setting.type=aliyunCloud`，不能再写进 `cloud.params`。
- `create-nas-filesystem` 需要同时提供 `storageType` 和 `protocolType`。
- `create-tenant-storage-class` 默认会按 `server=<mountTargetDomain>:<serverPath>` 创建 NAS CSI StorageClass。

## 集群验证脚本

```bash
export API_ADDR=http://localhost:8000/api/v1
export KUBECTL_NAMESPACE=default
bash examples/job-components/verify.sh instant
bash examples/job-components/verify.sh delay
```

## 备注

- 一次性延迟执行使用 `job` 组件并设置 `properties.startTime`；周期执行使用 Workflow 调度或可复用 Job 定义。
- `startTime` 为 Unix 秒时间戳，使用前请更新 JSON 中的值。
