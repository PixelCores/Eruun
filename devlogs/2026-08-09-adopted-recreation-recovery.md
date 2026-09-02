# Adopted 资源重建的可恢复持久化协议

## 背景与需求

Adopted exclusive 资源缺失时，worker 会先在 Kubernetes 创建替代对象，再用事务更新 adoption snapshot、workload UID 或 Secret 密文。旧流程在数据库提交失败后尝试删除新对象；如果父 context 已取消、Delete 失败或进程退出，新对象会保留为不同 UID，而数据库仍绑定旧 UID。此后的重试只能看到同名所有权冲突，无法判断它是本次未完成的创建还是外部对象。

同一轮评审还发现，Deployment/StatefulSet 的失败清理在父 context 于 ownership 查询期间取消时会提前 fail-closed，导致 native workflow 已记录的创建资源无法进入原有清理路径。

## 影响范围

- API：无路由、DTO 或请求字段变化。
- Domain：adoption snapshot 支持 v1 读取，并以 v2 保存可选的资源级 recreation claim。
- DB：复用 `applications.adoption_snapshot`，无 schema migration、无新表或新实体。
- K8s：重建候选对象带 server-owned recreation token annotation；该 annotation 不参与 snapshot manifest 或 digest。
- Workflow：Deployment、StatefulSet、Service、ConfigMap、Ingress、Secret、ServiceAccount、Role、RoleBinding、PodDisruptionBudget 与 NetworkPolicy 共享恢复协议。

## 技术选型与取舍

选择把 pending claim 放在对应的 `ResourceSnapshot` 中，而不是增加 claim 表或独立 application 字段。资源身份、重建 manifest 与未完成意图因此由同一个 application-row CAS 合并，避免跨实体事务和孤儿记录，也让不理解 v2 的旧 worker 通过既有版本校验 fail-closed。

Claim 只保存随机 token；旧 UID 继续以 `Source.UID` 为事实来源。协议不设置 TTL 或超时接管，因为仅凭时间无法证明同名对象可以安全取得所有权。Kubernetes 对象必须携带相同 token，且不能处于删除中，才允许完成数据库绑定。

## 实现摘要

1. 在任何 Kubernetes Create 前，从最新 application snapshot 读取目标资源并用 `update_time` CAS 写入 claim；并发冲突会有界重载和合并。
2. 候选对象写入 `eruun.io/adopted-recreation-token`。token 属于协议 metadata，生成 manifest 和 spec digest 时会被剥离。
3. 创建成功后使用脱离父取消且有界的 context 完成事务：dependency 更新 snapshot；workload 同事务更新 component source UID；Secret 同事务重绑定密文。
4. 事务提交清除 claim。提交失败或结果无法确认时保留 live 对象和 claim，不直接 Delete：确认读取与 Delete 之间无法形成原子边界，回滚可能误删已由并发 worker 绑定的同 UID 对象。下一轮重试通过同 token 对象继续完成提交。
5. 重启或并发 worker 只接受同 token 的 live 对象并继续提交；ownership 绑定完成后仍按当前任务的 desired state 执行正常调和，不能把较早执行者留下的对象直接视为本次成功。无 token、不同 token、错误 UID 或 terminating 对象均不绑定也不删除。
6. Deployment/StatefulSet 的 cleanup ownership 查询始终使用保留 values 的独立有界 context，后续 tracker 清理仍使用原 context。

## 测试与验收

覆盖 snapshot v1/v2 校验、legacy 空资源 namespace 回退、token metadata 规范化、claim 先于 Create、claim CAS 失败不创建、持久化失败保留对象与 claim、同 token 重启恢复、并发 worker 已完成绑定时幂等成功、`AlreadyExists` 后读取错误传播、stale replacement 按当前 desired state 继续调和、panic 后 lease 释放、错误 token fail-closed、StatefulSet retention snapshot、Secret 密文事务，以及父 context 在 cleanup ownership 查询中途取消的 Deployment/StatefulSet 回归场景。


## 风险与后续

Snapshot v2 是内部持久化格式；新建 snapshot 直接写 v2，既有无 claim 的 v1 数据仍可读取，并在首次需要重建时升级为 v2。滚动升级期间，旧 worker 会拒绝 v2 而不会绕过 claim 写入。Pending claim 不自动过期；长期无法收敛表示同名对象冲突、对象仍在删除或持久层不可用，需要先恢复真实外部状态，而不是强制接管。
