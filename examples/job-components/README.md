# Job 组件示例

该目录提供 `job` 组件的示例请求体，以及用于集群验证的脚本。

## 文件

- `instant-job.json`：job 即时执行示例（Job）。
- `scheduled-job-start-time.json`：job startTime 一次性延迟示例。

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
