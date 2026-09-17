# 原生 Harbor 任务示例

> 状态：Current。用于验证空间 Job 的原生 Harbor 执行与完整输出保存。

题目要求生成一个确定内容的文件及其制品副本。`tests/test.sh` 直接检查文件字节并生成 Harbor 原生 `reward.txt`。`solution/solve.sh` 是供 `oracle` 使用的参考答案，oracle 用于验证题目与平台接入，不代表某个 AI 模型的能力分数。

先在本机准备镜像并在 `task.toml` 中替换 `docker_image`，再打包。镜像地址必须与本机构建并推送的地址一致：

```sh
docker build -t example.com/your-team/eruun-greeting-task:1.0.0 examples/agent-evaluation/harbor-task/environment
docker push example.com/your-team/eruun-greeting-task:1.0.0
tar -C examples/agent-evaluation -czf /tmp/harbor-greeting.tar.gz harbor-task
```

上面的域名是占位值，使用自己的可访问仓库替换。上传 `/tmp/harbor-greeting.tar.gz` 后，提交 `eval`，框架选择 `harbor` / `0.22.0`，Agent 选择 `oracle`，无需模型凭据。完整输出应包含原生任务与 trial 结果、oracle 日志、verifier 日志、reward，以及 greeting 制品。

示例镜像兼容平台固定的 UID `1000` 和禁止特权策略。若选择 `terminus-2`，提供模型和已授权 Secret 即可由 Harbor 驱动该任务；若选择 `codex` 或 `claude-code`，先扩展本地 Dockerfile，固定安装对应 CLI 及系统依赖，并保留所有工作目录对 UID `1000` 可写。

运行容器中的参考解与 verifier 可以单独验证镜像准备情况：

```sh
docker run --rm --user 1000:1000 --cap-drop ALL --security-opt no-new-privileges \
  -v "$PWD/examples/agent-evaluation/harbor-task/solution:/solution:ro" \
  -v "$PWD/examples/agent-evaluation/harbor-task/tests:/tests:ro" \
  example.com/your-team/eruun-greeting-task:1.0.0 \
  sh -c 'sh /solution/solve.sh && sh /tests/test.sh && cat /logs/verifier/reward.txt'
```

预期输出为 `1`。此命令验证参考解和 verifier；真实集群中的 Harbor 调度、网络、模型访问及 API 归档仍需通过空间 Job 流程验收。
