# 全仓过度设计审计（2026-09-29，第三轮）

> 状态：Historical / Audit。基线为 `main@02503bdbc6ca5045542fa2f1aac71dc91c56a84c`，代码证据固定到该提交。O13–O15 及两项候选已在 [PR #118](https://github.com/PixelCores/Eruun/pull/118) 分支实施，处置和本地验收见下文；不据此声明已合入 main 或完成线上验收。前两轮整改已经进入本轮审计基线。

## 结论与范围

审计基线中确认 **3 项可处理问题**。优先修正 Ingress 默认规则的两套实现：合法配置已经能让 API 查询摘要与实际资源渲染给出不同端口。其次删除无生产调用的结果发送旧入口，收窄不使用却持续传递的依赖参数。另保留两项低优先级候选，分别是状态同步调度层和遗留策略常量；不为增加发现数量而把所有复杂机制列成待删代码。

从 `docs/README.md` 路由到启动装配、HTTP/gRPC、领域服务与校验、Workflow/Job/Traits、Harbor、基础设施、配置和部署，沿真实调用、数据来源和现有测试取证。本轮不是逐行穷尽审计。文件长度、接口数量、生成代码和第三方依赖体积均不单独构成问题；Draft / Proposal 不作为当前实现契约。

| 编号 | 已确认的问题 | 优先级与实际影响 | 最小处理边界 |
| --- | --- | --- | --- |
| O13 | 查询层重复计算 Ingress 默认值，端口已发生漂移 | P2：支持的配置会显示错误后端端口 | 共享无副作用的默认规则，assembler 保留 DTO 投影 |
| O14 | 结果发送的旧包装与 payload 构造只由测试调用 | P3：额外维护一组入口、默认值和专用测试 | 删除无生产入口的路径，迁移仍有意义的协议断言 |
| O15 | 4 个构造/执行函数声明并传递未读取的依赖 | P3：调用者和测试替身承担无效参数 | 仅移除这些形参及传递，保留外围授权和运行依赖 |

P2 表示可复现的功能不一致，P3 表示维护债务；没有据此声称已发生线上事故或测得性能收益。

## 后续实施处置

以下为本 PR 分支的实施状态；后文 O13–O15 和候选保留审计时的证据与建议，不再作为当前分支的未完成清单。

| 项目 | 已实施的收敛 | 保留与验收 |
| --- | --- | --- |
| O13 | assembler 与 Trait 渲染复用 `domain/spec` 的后端默认值及相同的 rewrite/pathType 规则 | 批量摘要与渲染使用同一输入比较；第二 Service 端口及显式默认服务的 properties 回退先证明旧实现失败，修复后通过。保留查询容错、部署歧义拒绝及输入模型不变 |
| O14 | 删除 `EnqueueResultJob`、`dispatchJobResult`、`newJobResultPayload`，测试改走真实 `enqueueResultJob` | 保留消息字段编码、错误身份和 Delay 构造/解码断言；outbox、CAS、ACK、执行身份与恢复路径未改 |
| O15 | `BuildTask` 仅收任务与 namespace；3 个 Pod 执行/归档 helper 移除 client 形参，调用者与替身同步收窄 | 外围服务的 context、Store、Config、Pod 查询 KubeClient 和执行 rest.Config 保留；既有行为测试及 integration tag 调用编译通过 |
| 状态同步候选 | 复用 client-go 按 key 去重的 workqueue 和 2 个固定 worker，移除额外 active、提交超时、signal/retry goroutine；删除失去唯一生产消费者的 `utils/async` | 保留 latest payload、epoch、generation fence、同 key 串行、并发上限、reset 等待与 Close 丢弃待办；覆盖饱和、重置、关闭及 panic 后继续处理 |
| 旧策略常量 | 删除 `DefaultNotRun`、`ForceRun`、`SkipRun` | `NormalizeJobRunPolicy` 实现未变，空值、`recreate`、`skip_if_completed` 和未知值行为保持 |

修复后，报告中原始探针的 `summary_backend` 与 `rendered_backend` 都为 `api-v2:9090`。查询摘要的错误值得到修正，HTTP/gRPC 路由及 JSON 字段不变。

**源码接入。** 仓内调用已更新；仓外 Go 消费者未验证。直接使用 `jobs.BuildTask` 的代码应改为 `BuildTask(task, namespace)`，Pod helper 调用应删除 client 实参、继续传递 context 与 rest.Config。旧结果包装、策略常量和 `utils/async` 已移除，不保留兼容转发层。可靠结果发送继续通过既有 outbox 流程，不应在仓外补回绕过持久化的发送路径。

## O13｜Ingress 默认规则由查询层和渲染层分别维护

**触发与证据。** 同一组件定义 `api-v1:8080` 和 `api-v2:9090` 两个 Service，Ingress 明确指定 `api-v2`，省略端口。两个 Service 都带合法 selector，组件 properties 包含两个端口和匹配 labels。Try 接受该输入，写入 Trait 校验也通过。

查询摘要经过 [`buildComponentIngresses`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/interfaces/api/assembler/v1/component_service.go#L110-L128)，先选组件级默认端口；[`buildIngressTraitDetails`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/interfaces/api/assembler/v1/component_service.go#L131-L168) 虽保留 route 指定的服务名，缺省端口仍使用第一个 Service 的 8080。实际渲染的 [`applyIngressDefaults`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/workflow/traits/ingress.go#L63-L75) 调用 [`resolveIngressBackendPort`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/workflow/traits/ingress.go#L119-L145)，按服务名找到 9090。HTTP 与 gRPC 都调用批量 assembler，因此这是生产查询路径的规则分歧。

本地探针直接调用现有 Try、写入 Trait 校验、批量 assembler 和 `ApplyTraits`，结果为：

```text
try_valid=true try_errors=[]
write_traits_error=<nil>
summary_backend=api-v2:8080
rendered_backend=api-v2:9090
```

**影响与简化。** API 使用者会看到一个不对应渲染结果的端口，排障与后续规则修改都要对照两套实现。收敛这组默认规则，由 assembler 投影到 DTO；从复制后的 spec 计算，避免查询修改保存模型。先修复并覆盖按服务名选端口的分歧，再核对名称、namespace、host、pathType/rewrite 的共同规则。无需增加注册表、策略接口或新的渲染框架，也不能让查询直接调用整个 `ApplyTraits` 流程。

**必须保留。** 显式端口优先、多 Service 缺省名称时的歧义拒绝、properties/80 回退、资源身份与命名空间、host 顺序、pathType/rewrite 注解优先级、TLS 和 nil/空集合语义。后续测试应将同一输入同时送入 batch assembler 与 Trait 渲染，覆盖第二个 Service、显式端口、单 Service 默认、properties 回退及歧义输入。修改的是错误摘要值，不改变公共字段或路由。

### 可重复的本地证据

在上述基线的仓库根目录，将下列代码保存为 `/tmp/ingress-audit.go`，运行 `go run /tmp/ingress-audit.go`。它只做内存校验、DTO 转换和对象渲染，不创建数据库记录或集群资源；通过写入 Trait 校验不等于完成整个 HTTP 写入事务。

<details>
<summary>探针源码</summary>

```go
package main

import (
	"context"
	"fmt"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	application "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/validation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
)

func main() {
	traitSpec := spec.Traits{
		Service: []spec.ServiceTraitSpec{
			{Name: "api-v1", Type: "internal", Selector: map[string]string{"tier": "api"}, Ports: []spec.ServicePortTraitSpec{{Port: 8080}}},
			{Name: "api-v2", Type: "internal", Selector: map[string]string{"tier": "api"}, Ports: []spec.ServicePortTraitSpec{{Port: 9090}}},
		},
		Ingress: []spec.IngressTraitsSpec{{Name: "api-ingress", Routes: []spec.IngressRoutes{{Path: "/", Backend: spec.IngressRoute{ServiceName: "api-v2"}}}}},
	}
	properties := spec.Properties{Ports: []spec.Ports{{Port: 8080}, {Port: 9090}}, Labels: map[string]string{"tier": "api"}}
	req := apis.CreateApplicationsRequest{Name: "demo", Namespace: "default", Components: []apis.CreateComponentRequest{{Name: "backend", Image: "nginx:1", ComponentType: config.ServerJob, Properties: properties, Traits: traitSpec}}}
	check := validation.NewValidationService(nil, nil, nil, nil).TryApplication(context.Background(), req)
	fmt.Printf("try_valid=%t try_errors=%+v\n", check.Valid, check.Errors)
	fmt.Printf("write_traits_error=%v\n", application.ValidateComponentTraitsForWrite(config.ServerJob, traitSpec, "components[0].traits"))
	raw, err := model.NewJSONStructByStruct(traitSpec)
	must(err)
	rawProperties, err := model.NewJSONStructByStruct(properties)
	must(err)
	component := &model.ApplicationComponent{Name: "backend", Namespace: "default", AppID: "demo", Image: "nginx:1", ComponentType: config.ServerJob, Properties: rawProperties, Traits: raw}
	dtos, err := assembler.ConvertComponentModelsToDTO([]*model.ApplicationComponent{component})
	must(err)
	route := dtos[0].Ingresses[0].Routes[0]
	fmt.Printf("summary_backend=%s:%d\n", route.ServiceName, route.ServicePort)
	workload := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "backend", Image: "nginx:1"}}}}}}
	objects, err := traits.ApplyTraits(component, workload)
	must(err)
	for _, obj := range objects {
		if ingress, ok := obj.(*networkingv1.Ingress); ok {
			backend := ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service
			fmt.Printf("rendered_backend=%s:%d\n", backend.Name, backend.Port.Number)
		}
	}
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
```

</details>

## O14｜结果发送仍保留没有生产入口的旧路径

**当前需求与证据。** 实际结果发送已经由 outbox 驱动：[`dispatchPendingOutbox`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/event/workflow/job/job_result_outbox.go#L250-L312) 领取状态，使用 [`jobResultPayloadFromOutbox`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/event/workflow/job/job_result_outbox.go#L341-L359) 构造消息，再调用私有 `enqueueResultJob`。入队前后的状态比较更新和 message ID 持久化承担可靠性要求。

但 [`EnqueueResultJob`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/event/workflow/job/job_result.go#L69-L71) 包装、[`dispatchJobResult`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/event/workflow/job/job_result.go#L465-L471) 和 [`newJobResultPayload`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/event/workflow/job/job_result.go#L886-L919) 仍保留。全仓符号检索中，除这条链内部调用外，它们仅由 `delay_result_test.go` 调用；旧构造函数还维护 namespace、serviceName、timeout 和执行身份的组装。这里指生产文件中仅被测试保活的旧入口，不是建议删除普通测试辅助函数。

**代价与简化。** 维护者需要分辨 outbox 与旧入口哪个实际发送消息，协议变化也可能继续修改无消费者的构造逻辑。移除两个私有死入口；导出的 `EnqueueResultJob` 在核对仓外 Go 使用后一起收敛。把仍有价值的编码、无效 payload、队列不可用和入队失败断言迁移到实际 `enqueueResultJob` / outbox 路径。

**必须保留。** `newJobResultPayloadFromDelay` 仍有生产调用，不能同删；保留 Delay 解码断言、outbox、CAS、ACK、恢复、generation/token 和资源归属检查。仓内无消费者不能证明仓外源码集成不存在；本轮未验证仓外 Go 调用。后续验收应包含领取、入队失败回退、消息身份持久化和旧 dispatching 恢复测试。

## O15｜函数签名表达了并不存在的依赖

**当前需求与证据。** 下列 4 个函数的指定形参在函数体中完全未读取，且不是需要遵循固定签名的接口实现：

| 函数 | 无效形参 | 实际工作与传递成本 |
| --- | --- | --- |
| [`jobs.BuildTask`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/jobs/builder.go#L80-L110) | `ctx`、`store`、`cfg` | 根据 WorkflowQueue 声明和 namespace 构建任务；两个生产调用者及现有测试仍传入这三个值 |
| [`ExecPodShellScript`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/utils/kube/pod_exec.go#L97-L119) | `client` | 实际通过 `restConfig` 创建执行通道；测试额外传入 fake client |
| [`StreamPodShellScript`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/utils/kube/pod_exec.go#L122-L172) | `client` | 流式执行依赖 context 和 `restConfig`；应用层及替身继续传递 client |
| [`ArchivePodPathAsZip`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/utils/kube/pod_exec.go#L175-L199) | `client` | tar 探测、归档与 fallback 都使用 `restConfig`；Job 层函数类型也重复携带 client |

`BuildTask` 的两个生产入口位于 [`jobs/service.go`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/jobs/service.go#L221) 和 [`event/workflow/job_builder.go`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/event/workflow/job_builder.go#L53)。Pod 操作的传递位于 [`component_pod_ops.go`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/domain/service/application/component_pod_ops.go#L66)；归档 Job 的替换函数类型位于 [`job_log_archive_upload.go`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/event/workflow/job/job_log_archive_upload.go#L52-L55)。

**代价与简化。** 函数签名使读者误以为声明构造需要数据库/运行配置，或 Pod 执行 helper 使用传入的 Kubernetes client；调用和替身也被迫维持无效依赖。直接删除相应形参及所有传递点，不新建上下文结构体、依赖容器或接口来包装它们。`BuildTask` 中评测 token 生成仍使用随机数，不能因为移除这些参数就把它称为确定性的纯函数。

**必须保留。** 外围服务仍需 context、数据库、配置和用于 Pod 查询的 KubeClient；尤其 `BuildEvaluationTask` 的真实依赖、组件授权/锁、容器选择、取消传播、输出上限、归档路径校验及 tar fallback 都保留。签名变化涉及导出 Go 函数，仓外迁移边界尚未验证；不需要修改 HTTP/gRPC 协议。后续运行现有构建、exec/stream/archive、应用层及归档 Job 测试，并编译全仓核对所有调用点。

## 两项低优先级候选

### 状态同步重复维护待处理任务的调度状态

[`ResourceReadyWaiter`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/infrastructure/informer/waiter.go#L129-L161) 固定创建 2 个 worker 和容量 256 的 executor。它又通过 [`statusSyncLanes`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/infrastructure/informer/waiter.go#L857-L881) 管理每个组件的 latest、epoch、active；提交超时后，由 [defer、signal、扫描和 retry goroutine](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/infrastructure/informer/waiter.go#L948-L1032) 重新向 executor 投递。`utils/async` 的唯一仓内生产消费者就是这个 waiter。

这里的成本是同一待处理 key 需要协调 lane map、executor channel 和重试信号三处状态。256 仅限制 executor 队列，更多 key 仍保存在 lane map；**没有证据证明已经发生内存、吞吐或安全故障**。已有饱和与关闭测试说明这些边界不能随意取消。

可评估使用项目已有 client-go 的 keyed workqueue 配合 2 个固定 worker，保留 latest payload map、epoch 和 generation fence，仅替换提交超时和重试转运层。回调不返回错误，无需增加 rate-limit 或业务重试框架。该替换尚未实现或证明等价，优先级低于 O13–O15；如果原契约需要补回同样复杂的机制，应放弃替换。

验收必须保持同组件串行与最新值合并、不同组件并发上限、reset 等待执行中回调、旧 epoch 拒绝、Close 丢弃排队任务且不死锁、panic 隔离。不能只以代码更短判定成功。

### 没有消费者且不被规范化接受的旧策略常量

[`DefaultNotRun`、`ForceRun`、`SkipRun`](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/pkg/apiserver/workflow/config/job_policy.go#L7-L28) 在仓内只有定义；同文件的 `NormalizeJobRunPolicy` 对它们的字符串返回 `known=false`。可清理这些导出常量并核对仓外调用，不应为了保留它们扩大支持值。`DefaultRun` 对应空值，仍被规范化接受，不能与前三者混为一谈。此项收益小，适合附带清理，不独立重构策略模型。

## 保留的必要复杂性

Workflow 租约、generation/token fencing、outbox、数据库恢复和空间授权都有当前需求。Harbor 的 claim、检查点、UID 比较及结果确认屏障用于执行身份与恢复；command Job 和 evaluation Job 的输入及依赖也不同，不能只因为都有构建代码就合并路径。HTTP/gRPC 的双入口和 ProtoJSON 转换有公开协议约束。

Go API 与 Python Runner 的归档校验处在不同信任边界，不能当成普通重复代码删除。`ArchiveUploader` 在 Current 文档中有明确源码扩展与未配置时快速失败的约定，本轮不把整个扩展接口判为冗余。基础设施的测试替身、不同角色的 observer 及 DB 事务能力边界也不因只有少量实现而自动归类为过度设计。

## 前两轮处置

| 历史范围 | 进入本轮基线的处置 | 证据 |
| --- | --- | --- |
| O01–O07 | Redis 缓存/锁收敛、按角色装配、延迟通知去除完整 Job、独立 Workflow handler、Worker 接口与 JSONStruct 清理 | [首轮固定报告](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/docs/overdesign-audit-2026-09-29.md)；对应 PR #95–#101 |
| O08–O12 | Try 复用叶子校验、DSN 唯一选库、CloudJob 检查点/快照收敛、移除 errhandler | [已合并 PR #117](https://github.com/PixelCores/Eruun/pull/117)；[实施与验收固定版本](https://github.com/PixelCores/Eruun/blob/02503bdbc6ca5045542fa2f1aac71dc91c56a84c/docs/overdesign-audit-2026-09-29.md) |
| 上轮局部候选 | Aliyun 空 action 构造、Workflow 入队参数转发、单用途泛型转换和聚合状态依赖已收敛 | 同属 PR #117 |

以上具体旧项不再作为未完成待办；不表示相关模块已不存在其他债务。旧数据库参数迁移仍参见 [本地依赖](local-docker-dependencies.md) 和 [Helm 部署](helm-deployment.md)。本文复用原审计文件，详细历史通过固定提交保留。

## 审计基线验证

通过符号/导入检索和函数体核对建立 O14、O15 的消费者及依赖证据；O13 使用上述真实函数探针复现。以下既有测试在本轮基线通过，证明当前契约与测试现状，**不是尚未实施的简化方案已经验收**：

```sh
# Ingress 摘要、默认规则与歧义拒绝
go test ./pkg/apiserver/interfaces/api/assembler/v1 ./pkg/apiserver/workflow/traits ./pkg/apiserver/domain/service/validation -run 'TestConvertComponentModelToDTO.*Ingress|TestApplyIngressDefaults|TestValidationService_TryApplication_RejectsMissingIngressServiceNameWithMultipleServices' -count=1

# 结果编码、旧入口及真实 outbox 路径
go test ./pkg/apiserver/event/workflow/job -run 'TestEnqueueResultJobAndDispatch|TestDispatchJobResultReturnsQueueErrorWhenUnavailable|TestResultPayloadBuildersAndDecode|TestResultOutboxDispatcherClaimsPendingBeforeEnqueue|TestResultOutboxDispatcherPersistsQueuedStateAfterSuccessfulEnqueue|TestResultOutboxDispatcherReturnsToPendingWhenEnqueueFails|TestJobResultPayloadFromOutboxRequiresMandatoryFields' -count=1

# 无效依赖所涉的现有行为（32 个顶层测试，无跳过）
go test ./pkg/apiserver/utils/kube -race -run '^Test(ExecPodShellScript|StreamPodShellScript|ArchivePodPathAsZip|IsArchivePathLookupError|SanitizeArchiveEntryName|CopyTarHardLinkToZip|BuildPodExecURL)' -count=1 -v
go test ./pkg/apiserver/domain/service/application -race -run '^Test(ListComponentPodsUsesSourceUIDWithoutManagedLabels|ExportComponentFilesZip|ExecComponentShellScript|StreamComponentShellScript)' -count=1 -v
go test ./pkg/apiserver/event/workflow/job -race -run '^TestLogArchiveUploadJobCtl' -count=1 -v
go test ./pkg/apiserver/jobs -race -run '^(TestCommandAndEvaluationRenderIntoTheSameWorkspaceNamespace|TestEvaluationCredentialEnvsSurviveRunnerPlatformEnvs)$' -count=1 -v

# 状态同步并发、代次重置、饱和与关闭
go test ./pkg/apiserver/infrastructure/informer -race -run 'TestStatusSync|TestResetPodSnapshots|TestCloseDropsQueuedStatusSyncCallbacks|TestCloseUnblocksDeferredStatusSyncSubmit|TestResourceReadyWaiterCloseIsIdempotent' -count=1
```

审计初稿 `921220e` 只修改本文和索引，未改生产代码或测试；当时另检查固定证据链接的文件/行号、Markdown 本地链接、`git diff --check` 和敏感内容。审计阶段没有重复上轮的全仓验收，后续实施验收独立记录如下。未连接真实 MySQL/Redis/Kafka/Kubernetes/ACS，未运行云 SDK 验收或性能基准，也未验证仓外 Go 消费者。

## 后续实施验收

修复基线为 `921220e`。O13 新增回归先在旧生产实现上复现端口错误；O14/O15 迁移已有行为断言，不为被删除的入口另建兼容层。状态同步测试以固定 2 worker 的实际行为验证，移除只绑定旧 executor/signal 实现的操作。

已通过：

```sh
go test -race -cover -p 2 ./...
go vet ./...
go build -trimpath -o /tmp/eruun-round3-server ./cmd/main.go
go test -tags integration ./pkg/apiserver/jobs -run '^$'
scripts/check-sensitive-content.sh
git diff --check
```

另完成触及 Go 文件格式检查、原始 Ingress 探针复跑及限定改动范围的独立回归审查。integration tag 命令只验证编译，不代表执行真实数据库集成测试。部署文件、安装器和 Helm 契约没有修改，沿用基线的对应验收；未开展真实集群、数据库、云 SDK、仓外 Go 集成或性能验证。
