# Eruun 独立开源产品基线

> 状态：Historical / Audit。本文记录当时的开源基线；2026-09-03 后续清理已移除发布就绪清单、许可证检查机制及独立的社区行为准则和贡献指南文档，保留其他构建、测试与安全检查。当前 CI 范围以 [CI 工作流](../.github/workflows/ci.yml)和[依赖审查工作流](../.github/workflows/dependency-review.yml)为准。

## 背景与需求

Eruun 作为独立产品线进入独立 Git 仓库，定位为 “A distributed runtime for agents, models, and AI workloads”。迁入内容来自权利人指定的干净源代码快照，仅包含当时受 Git 跟踪的文件；目标仓库保留自己的远端和历史。

本次建立的是全新安装基线，不承担旧环境变量、资源名、镜像名、数据库表名或安装拓扑的兼容与迁移。服务端现有 `/api/v1` 路由、DTO/JSON 字段和启动 flags 行为保持不变，客户端 CLI 不属于 Eruun 产品范围。

## 影响范围

- API：继续使用 `/api/v1`，不引入品牌迁移别名。
- Domain/Workflow：保留已实现的 Kubernetes 应用与工作流能力。
- DB：表前缀统一为 `eruun_`，只面向全新数据库。
- Cache/Config：服务端环境变量统一使用 `ERUUN_*`。
- K8s：namespace、资源、镜像、Helm、元数据和 workflow group 统一使用 Eruun 契约。
- Distribution：只交付服务端二进制和容器镜像，不交付客户端 CLI。
- Skills：仓库内与全局代码 Skill 统一路由到 Eruun API Server、工作流和部署维护。

## 技术选型与取舍

### 独立历史，而不是复制源历史

目标仓库保留自己的 `.git` 和远端，只迁入快照文件。这样可以明确产品边界，并避免把不属于公开发行范围的历史、忽略文件和构建产物带入公开仓库。

### 破坏性品牌边界，而不是兼容层

所有 module、符号、配置、镜像、资源和元数据直接使用 Eruun 名称，不保留双读、别名或迁移逻辑。代价是既有安装不能原地升级；收益是公开契约单一、可审计，不会长期维护隐式兼容面。

### 能力状态显式化

文档只将代码和测试已经覆盖的 Kubernetes 工作流能力标为 Current 或 Implemented；agents、models 及其他尚未实现的产品能力继续标为 Draft。产品定位不等于当前实现承诺。

### 凭据与许可证门禁

部署清单不携带固定凭据。quickstart 在本地生成随机密码，通过权限为 `0600` 的临时文件创建 Kubernetes Secret，并在退出时清理且不打印值。Helm 对空密码和未替换占位符直接失败。

Go 依赖清单由固定版本的许可证扫描器生成；unknown、restricted 和 forbidden 分类会阻断检查。不能自动识别的依赖必须进入显式人工审核覆盖表。该清单是技术尽调材料，不是法律意见。

## 实现摘要

- 服务端 module、二进制、版本、镜像和部署基线统一为 Eruun `0.1.0`。
- 删除客户端 CLI 源码、构建目标、指南和专属配置。
- 重写中英文入口、文档索引、部署示例和开源治理材料。
- 增加 CI、依赖审查、敏感信息扫描、许可证清单和贡献者治理文件。
- 增加仓库与全局 `eruun-code` Skill，并同步跨项目路由 Skill。

## 测试与验收

- `go fmt ./...`
- `go vet ./...`
- `go test ./... -race -cover`
- 服务端本机构建与 Docker build
- 部署清单 Go 测试与 quickstart 脚本测试
- 固定版本 Helm lint 与 template 契约测试
- 敏感内容、旧名称、私有引用和客户端 CLI 残留扫描
- 固定版本许可证扫描及人工覆盖表校验
- 仓库和全局 Skill `quick_validate.py`

## 风险与后续

- 公开前必须由权利人确认代码、历史贡献、文档和素材具有公开再许可权。机械重命名和 MIT LICENSE 不能替代该确认。
- 仓库管理员仍需在远端启用 secret scanning、push protection 和所需分支保护。
- 镜像推送、远端提交、Release 和仓库设置不属于本次本地基线工作。
