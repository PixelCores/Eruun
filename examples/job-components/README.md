# Job 组件示例

该目录提供 `job`、`scheduledjob` 组件的示例请求体，以及用于集群验证的脚本。

## 文件

- `instant-job.json`：job 即时执行示例（Job）。
- `scheduled-job-cron.json`：scheduledjob cron 示例（CronJob）。
- `scheduled-job-start-time.json`：job startTime 一次性延迟示例。

## 快速验证

```bash
curl --fail -X POST "$SERVER/api/v1/applications/try" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d @examples/job-components/instant-job.json
```

## 集群验证脚本

```bash
export API_ADDR=http://localhost:8000/api/v1
export KUBECTL_NAMESPACE=default
bash examples/job-components/verify.sh instant
bash examples/job-components/verify.sh cron
bash examples/job-components/verify.sh delay
```

## 备注

- Cron 格式支持 5 段，或 6 段且秒字段为 0（例如 `0 0 * * * *` 会规范化为 Kubernetes CronJob 使用的 5 段格式）。
- `scheduledjob` 必须提供 `properties.schedule`；一次性延迟执行请使用 `job` 组件并设置 `properties.startTime`。
- `startTime` 为 Unix 秒时间戳，使用前请更新 JSON 中的值。
