# targetOnlyStrategy

`targetOnlyStrategy` 用来处理这类差异：`targetAppID` 的完整应用里存在某个组件，但 `sourceAppId` 的完整应用里不存在这个组件。

JSON 示例文件保持为可直接请求的纯 JSON，所以策略说明放在这里。

## preserve

```json
{
  "targetOnlyStrategy": "preserve"
}
```

默认策略。目标独有组件会出现在响应的 `extraComponents` 中，`action` 为 `preserve`。该策略不会删除目标独有组件，也不会因为这些组件清理 Kubernetes 资源；如果同时存在其他可执行差异，执行模式仍会更新版本号和相关组件。

适合目标环境有临时组件、环境专属组件、辅助组件，或者只想预览差异的场景。

## block

```json
{
  "targetOnlyStrategy": "block"
}
```

目标独有组件会出现在响应的 `extraComponents` 中，`action` 为 `block`。`dryRun=true` 时只返回差异；`dryRun=false` 时如果存在目标独有组件，接口会失败并且不会执行更新。

适合生产环境需要人工确认删除风险的场景。

## remove

```json
{
  "targetOnlyStrategy": "remove"
}
```

目标独有组件会出现在响应的 `extraComponents` 中，`action` 为 `remove`。`dryRun=false` 时会生成 remove 操作，复用现有版本更新删除路径，删除组件记录并清理对应资源。

适合目标应用需要严格同步为源应用完整状态的场景。建议先用 `dryRun=true` 查看 `extraComponents`，确认无误后再执行。
