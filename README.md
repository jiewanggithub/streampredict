# StreamPredict

> 一个可交互、可观测、可扩缩容、支持模型版本管理与安全回滚的实时机器学习推理系统。

StreamPredict 不只是一个预测 API。它的目标是实现从用户触发 Demo、请求进入系统、Kafka 异步处理、Redis 在线特征与缓存、ONNX Runtime 模型推理服务，到 Prometheus 指标展示、Kubernetes 自动扩缩容以及模型回滚的完整闭环。

项目最终应当能够作为 GitHub Portfolio、面试演示和 MLOps / ML Infrastructure 系统设计案例使用。

## 项目状态

| 标记 | 含义 |
| --- | --- |
| `未开始` | 已规划，但尚未进入开发 |
| `正在实现` | 已开始开发，功能或测试尚未完成 |
| `已实现` | 已完成代码、测试和最基本的使用文档 |

> 更新规则：模块只有在核心功能可运行并通过对应验收条件后，才能标记为 `已实现`。

### 当前进度总览

| ID | Module | 当前状态 | 目标 |
| --- | --- | --- | --- |
| M0 | 项目基础与开发环境 | `已实现` | 建立可复现的本地开发环境、目录与工程规范 |
| M1 | React / Next.js Demo Dashboard | `已实现` | 给用户一个可操作、可观察系统变化的 Demo 页面 |
| M2 | FastAPI API Gateway | `已实现` | 提供预测、Demo 控制、健康检查和指标接口 |
| M3 | Kafka Event Pipeline | `已实现` | 实现事件生产、缓冲、消费与消费积压观测 |
| M4 | Redis Feature & Cache Layer | `正在实现` | 提供在线特征读取和预测结果缓存 |
| M5 | ONNX Runtime Model Serving | `已实现` | 托管版本化模型并执行真实推理 |
| M6 | MLflow + S3 Model Lifecycle | `已实现` | 管理模型版本、制品、Champion/Challenger 与回滚 |
| M7 | Prometheus Observability | `已实现` | 统一采集 API、Kafka、Redis、模型和集群指标 |
| M8 | Demo Orchestrator & Load Generator | `已实现` | 安全地触发流量尖峰并展示系统反馈闭环 |
| M9 | Kubernetes Deployment & HPA | `已实现` | 部署各服务并基于 CPU、RPS 和 Kafka lag 扩缩容 |
| M10 | Testing, CI/CD & Security | `未开始` | 建立自动化测试、质量门禁、镜像发布与安全基线 |

## Demo 最终体验

用户打开 StreamPredict Dashboard 后，可以完成以下操作：

1. 输入一条样例数据并获得实时预测结果。
2. 点击 **Run Demo** 或 **Start Traffic Spike**。
3. 观察请求速率与 Kafka lag 上升。
4. 观察 Consumer replicas 从 `2 -> 4 -> 8` 自动扩容。
5. 观察消费吞吐提升以及 Kafka lag 逐步回落。
6. 查看 p50 / p95 / p99 latency、success rate、cache hit rate 和当前 model version。
7. 模拟新模型健康检查失败，并看到系统从 `v13` 回滚到 `v12`。
8. Demo 结束后自动停止流量，系统逐步恢复到正常副本数。

Demo 使用合成数据，并且必须具备最大持续时间、速率上限和手动停止机制，避免产生无限流量或失控资源消耗。

## 系统架构

当前架构图：[`docs/architecture/streampredict-architecture-demo.pdf`](docs/architecture/streampredict-architecture-demo.pdf)（图中的 TorchServe 已由自研 ONNX Runtime 推理服务替代，见 M5）

```text
User
  |
  v
React / Next.js Dashboard
  |
  v
FastAPI API Gateway
  |-------------------------------> Redis cache / online features
  |                                      |
  |                                      v
  |---------------------------------> Model Serving (ONNX Runtime, KServe v2)
  |
  v
Kafka Producer -> Kafka Topic -> Consumer Group -> Redis -> Model Serving

MLflow Registry -> S3 Artifacts -> Deployment Controller -> Model Serving

Prometheus <- FastAPI / Kafka / Redis / Model Serving / Kubernetes
     |
     +-> Dashboard metrics
     +-> HPA scaling signals
     +-> model health gates and rollback
```

## Modules

### M0 - 项目基础与开发环境 `已实现`

负责所有模块共享的工程基础。

需要实现：

- [x] 初始化 Git 仓库与 `main` 分支。
- [x] 创建 Python 3.11 Conda 环境与 `environment.yml`。
- [x] 建立 `apps/`、`services/`、`infra/` 和 `docs/` 基础目录。
- [x] 添加架构图。
- [x] 确定 Python、Node.js 和容器依赖的版本锁定方案。
- [x] 增加统一配置管理和 `.env.example`。
- [x] 增加代码格式化、lint、类型检查和 pre-commit hooks。
- [x] 增加根目录任务入口 `Makefile`。

验收条件：新开发者能够根据 README 创建开发环境、安装 Git hooks，并通过全部基础质量检查。最小应用服务将在 Phase 1 中实现。

### M1 - React / Next.js Demo Dashboard `已实现`

面向最终用户的交互式演示页面。

需要实现：

- [x] 系统总览：首页显示服务健康状态与当前模型版本。
- [x] 在线预测：填写样例输入并展示预测结果、耗时与 cache hit 状态。
- [x] Demo 控制：提供 **Run Demo**、**Start Traffic Spike** 和 **Stop Demo**。
- [x] 实时指标：展示 RPS、p50/p95/p99 latency、success rate。
- [x] Kafka 面板：展示 incoming events、consumer throughput 和 consumer lag。
- [x] Redis 面板：展示 cache hit rate、miss rate 和 lookup latency。
- [x] Model 面板：展示 Champion / Challenger、版本、错误率和回滚事件（另含预测分布、发布控制与发布历史）。
- [x] Infrastructure 面板：展示 Pod 数量、CPU、内存和 HPA scaling events（Kubernetes 中读取 Deployment、HPA、metrics-server 与 SuccessfulRescale 事件；Compose 中显示 Consumer 副本与分区分配）。
- [x] 使用 SSE 或 WebSocket 接收实时更新；轮询可作为第一版实现。
- [x] 提供加载、空数据、断线、错误和 Demo 完成状态。

验收条件：用户不需要命令行，即可触发一次受控 Demo 并理解系统发生了什么。

### M2 - FastAPI API Gateway `已实现`

系统统一入口，负责同步预测、Demo 控制和前端所需的聚合数据。

计划接口：

| Method | Endpoint | 用途 | 状态 |
| --- | --- | --- | --- |
| `GET` | `/health` | 进程健康检查 | `已实现` |
| `GET` | `/ready` | Redis、Kafka、模型推理服务依赖就绪检查 | `已实现` |
| `POST` | `/api/v1/predict` | 同步预测 | `已实现` |
| `POST` | `/api/v1/events` | 接收并发布异步预测事件 | `已实现` |
| `POST` | `/api/v1/demo/traffic-spike` | 启动受控流量尖峰 | `已实现` |
| `POST` | `/api/v1/demo/stop` | 停止当前 Demo | `已实现` |
| `GET` | `/api/v1/demo/status` | 查询 Demo 状态 | `已实现` |
| `GET` | `/api/v1/metrics/overview` | 返回前端聚合指标 | `已实现` |
| `GET` | `/api/v1/metrics/stream` | 通过 SSE 推送指标 | `已实现` |
| `GET` | `/metrics` | 暴露 Prometheus 格式指标 | `已实现` |

需要实现：

- [x] Pydantic 请求与响应 Schema。
- [x] Request ID、结构化日志和统一错误格式。
- [x] 超时、重试、并发限制和优雅关闭。
- [x] CORS、输入校验和 Demo 控制接口保护。
- [x] OpenAPI 文档和接口测试。

验收条件：API 能处理同步预测、异步事件和 Demo 控制，并暴露可采集指标。

说明：异步事件的失败重试由 Consumer 负责（M3）；同步路径对推理服务设置超时并快速失败（503），由调用方重试。

### M3 - Kafka Event Pipeline `已实现`

负责高吞吐异步事件处理，并把流量尖峰与在线推理解耦。

需要实现：

- [x] 定义 `prediction-events`、`prediction-results` 和 dead-letter topic。
- [x] FastAPI Producer 发布带 schema version、request ID 和时间戳的事件。
- [x] Consumer Group 批量拉取、处理和提交 offset。
- [x] 失败重试、幂等处理和 dead-letter queue。
- [x] Consumer lag、吞吐、失败率和处理耗时指标。
- [x] 配置 partition、retention 和 consumer concurrency。
- [x] 本地 Docker Compose Kafka 环境。

验收条件：在突发流量下不丢事件，Consumer 可以水平扩展，失败事件可定位和重放。

验证记录：本地 Compose 中 100 RPS 的 spike 共发布 1,088 个事件，产生 1,088 个结果；2 个 Consumer 各分得 6 个 partition；`make test-kafka` 在真实 broker 上验证结果恰好一次、重复投递去重、dead-letter 与重放。运维说明见 [`docs/runbooks/kafka-event-pipeline.md`](docs/runbooks/kafka-event-pipeline.md)。

### M4 - Redis Feature & Cache Layer `正在实现`

负责低延迟在线特征读取、预测缓存和短期 Demo 状态。

需要实现：

- [ ] 设计 feature key、prediction cache key 和 TTL 规则。
- [x] 实现 cache-aside 读取流程。
- [x] 防止缓存击穿、雪崩和无界 key 增长。
- [ ] 保存 Demo session 状态，但不将 Redis 作为永久事实来源。
- [x] 暴露 hit rate、miss rate、连接数和 lookup latency。
- [x] 增加 Redis 不可用时的降级策略。

验收条件：重复请求能命中缓存；Redis 故障不会造成 API 无限等待或不可解释的错误。

### M5 - ONNX Runtime Model Serving `已实现`

负责加载模型、执行推理并暴露稳定的内部推理接口。平台与模型解耦：任何能导出为 ONNX 的模型（PyTorch、TensorFlow、scikit-learn 等）都可以按同一模型仓库规范接入。

选型说明：原计划的 TorchServe 已于 2025 年 8 月归档停止维护；Triton 功能最全，但镜像约 20 GB，其核心优势（GPU / TensorRT、多框架混跑）本项目用不到。因此自研轻量推理服务（镜像约 0.4 GB），接口采用 Open Inference Protocol（KServe v2），模型仓库目录与 Triton / KServe 一致，需要时可无缝切换。

需要实现：

- [x] 训练或准备一个轻量级示例模型：[`ml/training`](ml/training) 用合成数据训练 PyTorch 模型并导出 ONNX，v1（AUC 0.72）与 v2（AUC 0.78）两个版本。
- [x] 预处理、推理和后处理：特征变换与标准化打包进 ONNX 计算图；风险阈值等业务逻辑留在 API。
- [x] 模型制品与仓库规范：`<model>/config.json` + `<model>/<version>/model.onnx` + 训练元数据。
- [x] 支持 model version、batching、线程数和超时配置；支持不重启热加载 / 卸载版本。
- [x] 暴露 inference latency、queue time、batch size、error rate 和版本就绪指标。
- [x] 增加 warm-up、健康检查和 readiness probe。

验收条件：同一模型制品可以在本地和 Kubernetes 中稳定运行，并返回可验证的预测结果。

验证记录：本地 Compose 中同步预测 p50 约 2 ms、p95 约 15 ms（含 Redis 缓存命中）；100 RPS 的 Kafka spike 共 1,084 个事件全部经推理服务处理，0 错误；并发请求被动态 batching 合并（157 次推理合并为 104 个批次）。Kubernetes 中以 2 个副本运行并通过 HPA（CPU）扩缩，发布时控制器逐个通知副本加载新版本（M9 验证）。

### M6 - MLflow + S3 Model Lifecycle `已实现`

负责模型追踪、注册、制品存储、发布和回滚。发布权威是 [`services/model-controller`](services/model-controller)：它读取 MLflow Registry，管理推理服务的模型仓库（共享卷）并执行门禁。

需要实现：

- [x] MLflow Tracking Server 与数据库后端（Postgres）。
- [x] S3 兼容对象存储保存模型制品：SeaweedFS（MinIO 社区版已归档停止发布镜像），MLflow 以代理模式读写制品。
- [x] 记录参数、指标、数据版本和代码版本：训练 run 记录超参数、AUC 等指标、`synthetic(seed, samples)` 数据版本与 Git commit。
- [x] 实现 Champion / Challenger 流程：`champion` alias 指向生产版本；候选版本经门禁后晋升，旧 champion 保留为热备。
- [x] 发布前进行模型签名与兼容性检查：输入输出签名与特征顺序必须与线上契约一致，离线 AUC 不得低于阈值或明显退化。
- [x] 部署记录与当前生产版本可追踪：每次发布是 `streampredict-deployments` 实验中的一个 run（结果、原因、门禁指标），版本带 `status` 标签。
- [x] 健康门禁失败时回滚至上一个稳定版本：切流后用生产形态的参考输入验证新版本，输出分布相对其训练时分布的 PSI、高风险率、延迟或错误率超标即自动切回。

验收条件：任意线上模型可以追溯到其训练记录和制品，并能完成一次可观察的版本升级与回滚。

验证记录：本地 Compose 中 v1 → v2 通过门禁晋升（PSI 0.003）；v2 → v3 时 v3 离线 AUC 与 v2 相同（0.778），但它在训练时把金额当作“分”，上线后高风险率从预期的 6.2% 降到 0%（PSI 1.25），约 10 秒内自动回滚到 v2，Dashboard 与 MLflow 均可见全过程。运维说明见 [`docs/runbooks/model-releases.md`](docs/runbooks/model-releases.md)。

### M7 - Prometheus Observability `已实现`

负责统一采集并呈现系统行为。

核心指标：

- [x] FastAPI：RPS、status code、p50/p95/p99 latency、in-flight requests。
- [x] Kafka：producer rate、consumer throughput、consumer lag、retry count。
- [x] Redis：hit rate、miss rate、lookup latency、connection usage。
- [x] Model Serving：inference latency、batch size、queue time、error rate。
- [x] Kubernetes：Pod count、CPU、memory、restart count、HPA events。
- [x] Model：current version、prediction distribution、rollback count。
- [x] Demo：session state、target RPS、elapsed time、generated events。

需要实现：

- [x] Prometheus scrape 配置和 service discovery。
- [x] 指标命名规范与 label 基数限制。
- [x] 基础告警：高错误率、高延迟、Kafka lag、模型健康失败。
- [ ] 可选 Grafana 工程监控面板（未做：Dashboard 与 Prometheus UI 已覆盖演示需要）。

验收条件：一次 Demo 的主要变化都能从指标中解释，并能在前端或 Grafana 中复现。

验证记录：Prometheus 自动发现并采集每个副本（K8s 中 12 个 target 全部健康），记录规则提供全集群 p50/p95/p99，Dashboard 显示的延迟改为全集群口径；带流量发布缺陷模型 v3 后 `ModelReleaseRolledBack` 告警触发并显示在 Dashboard 横幅；网关按 Prometheus RPS（每 Pod 40）从 2 扩到 3。告警规则由 `make prometheus-test`（promtool 单元测试）验证。指标目录与约定见 [`docs/observability.md`](docs/observability.md)。

### M8 - Demo Orchestrator & Load Generator `已实现`

负责把复杂系统行为封装成用户可触发的安全演示。

需要实现：

- [x] 定义 Demo 状态机：`idle -> starting -> running -> cooling_down -> completed/failed`。
- [x] 支持固定模式和 traffic spike 模式。
- [x] 限制最大 RPS、最大时长和同一时间的 session 数量。
- [x] 支持手动停止、超时停止和异常清理。
- [x] 使用合成数据，不上传或保留用户敏感数据。
- [x] 将 Demo 进度和关键事件推送到前端。
- [x] Demo 完成后生成摘要：峰值 RPS、最大 lag、扩容次数、恢复时间（从 lag 峰值回落到 ≤ 10 的秒数）。

验收条件：连续运行多次 Demo 不会残留任务、无限发消息或持续占用资源。

实现说明：单副本部署（Compose）中编排器运行在网关进程内；Kubernetes 中作为独立的单副本服务运行（与网关同镜像，`streampredict_api.demo_service`），网关各副本把 Demo 接口代理给它，保证集群内只有一个 Session 状态；合成流量经网关 Service 发出，与真实用户流量走相同路径。

### M9 - Kubernetes Deployment & HPA `已实现`

负责生产式部署、服务发现、弹性伸缩和安全回滚。

需要实现：

- [x] FastAPI、Consumer、Model Serving 等组件的 Deployment 和 Service。
- [x] ConfigMap、Secret、resource requests / limits。
- [x] liveness、readiness 和 startup probes。
- [x] FastAPI 基于 CPU / RPS 的 HPA（KEDA：CPU 70% 或每 Pod 40 RPS，RPS 来自 Prometheus）。
- [x] Consumer 基于 Kafka lag 的扩缩容（KEDA，2–8 副本，每 15 秒最多翻倍，缩容先稳定 60 秒）。
- [x] PodDisruptionBudget 和滚动更新策略。
- [x] 本地集群方案，例如 `kind` 或 `minikube`。
- [x] 模型发布失败自动回滚：发布控制器的部署后门禁失败即切回旧版本；Deployment 滚动发布超时由 `deploy.sh` 自动 `rollout undo`。

验收条件：流量尖峰能触发扩容，负载下降后能安全缩容，发布失败不会长时间影响预测服务。

验证记录：本地 kind 集群（Docker 内存 7.7 GB，节点占用约 3.7 GB）中，100 RPS 的 Kafka spike 使 lag 升至约 900，KEDA 将 Consumer 从 2 扩到 4 再到 8，消费吞吐从约 50/s 升到约 180/s，lag 回落到 0；负载结束后按策略缩回 2。带流量发布有缺陷的 v3 时，部署后门禁约 10 秒内回滚到 v2，3,240 个请求 0 错误。部署与排障见 [`docs/runbooks/kubernetes.md`](docs/runbooks/kubernetes.md)。

### M10 - Testing, CI/CD & Security `未开始`

负责保证项目可以持续迭代，而不是只能运行一次的 Demo。

需要实现：

- [ ] 单元测试：Schema、缓存、事件处理和业务逻辑。
- [ ] 集成测试：FastAPI + Redis + Kafka + Model Serving。
- [ ] 端到端测试：从 Dashboard 触发 Demo 并验证反馈闭环。
- [ ] 负载测试：吞吐、延迟、lag 和扩缩容恢复时间。
- [ ] GitHub Actions：lint、type check、test、build。
- [ ] Docker 镜像构建、版本标签和漏洞扫描。
- [ ] Secret 不入库，日志不记录敏感数据。
- [ ] API 限流、依赖超时和最小权限配置。

验收条件：Pull Request 能自动执行质量检查，主分支始终保持可构建和可运行。

## 事件数据约定

异步预测事件至少包含以下字段：

```json
{
  "schema_version": "1.0",
  "event_id": "uuid",
  "request_id": "uuid",
  "created_at": "ISO-8601 timestamp",
  "model_name": "streampredict-demo",
  "model_version": "v13",
  "features": {},
  "metadata": {
    "source": "dashboard-demo",
    "demo_session_id": "uuid"
  }
}
```

后续修改事件格式时必须增加 `schema_version`，并保证 Consumer 可以处理允许范围内的旧版本。

## 非功能性要求

以下数值是项目目标，最终需要通过负载测试校准，而不是提前宣称已经达到：

| 指标 | 初始目标 |
| --- | --- |
| 同步预测 p95 latency | `< 150 ms`，不包含首次模型冷启动 |
| API success rate | `>= 99.5%`，在定义的测试负载内 |
| 缓存读取 p95 | `< 10 ms` |
| Demo 最大持续时间 | `5 min` |
| Demo 自动停止 | 必须 |
| 事件幂等处理 | 必须 |
| 模型版本可追踪与回滚 | 必须 |
| 关键组件健康检查 | 必须 |
| 指标与结构化日志 | 必须 |

## 目标仓库结构

```text
StreamPredict/
├── apps/
│   └── dashboard/              # Next.js 用户 Demo
├── services/
│   ├── api/                    # FastAPI Gateway（含 Demo 编排器 demo_service）
│   ├── consumer/               # Kafka Consumer workers
│   ├── model-controller/       # 发布控制器：门禁、晋升与自动回滚
│   └── model-serving/          # ONNX Runtime 推理服务与模型仓库
├── ml/
│   ├── training/               # 示例训练流程（PyTorch -> ONNX）
│   ├── artifacts/              # 已提交的示例模型版本（用于初始化 Registry）
│   └── registry/               # MLflow 集成
├── infra/
│   ├── docker/                 # 本地 Docker Compose
│   ├── kubernetes/             # Kubernetes manifests / Helm
│   ├── prometheus/             # 指标采集和告警
│   └── grafana/                # 可选工程监控面板
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── e2e/
│   └── load/
├── docs/
│   ├── architecture/
│   ├── api/
│   └── runbooks/
├── .github/workflows/          # CI/CD
├── environment.yml
└── README.md
```

## 实施阶段

### Phase 1 - Local Vertical Slice `已实现`

实现 Dashboard -> FastAPI -> Redis -> mock inference 的最小闭环，同时建立测试和 Docker Compose。

已通过 `make up` 验证 Redis、API 与 Dashboard 全栈启动。

### Phase 2 - Streaming Pipeline `已实现`

接入 Kafka Producer / Consumer，加入真实模型推理和端到端事件追踪。

已在本地完成：Kafka 事件管道（M3）、ONNX Runtime 推理服务与示例模型（M5），事件携带 `event_id` / `request_id` 贯穿到结果 topic。

### Phase 3 - Model Lifecycle & Observability `已实现`

接入 MLflow、S3-compatible artifact storage、Prometheus 和完整指标面板。

已完成：MLflow + SeaweedFS 模型生命周期与门禁回滚（M6）、Prometheus 采集、告警与全集群指标（M7）。

### Phase 4 - Kubernetes & Autoscaling Demo `已实现`

部署到本地或云端 Kubernetes，完成基于 Kafka lag 的扩缩容、健康门禁与回滚演示。

### Phase 5 - Portfolio Hardening `未开始`

补齐 CI/CD、安全、负载测试、运行手册、截图、演示视频和公开 Demo 部署。

## 本地开发

完整说明见 [`docs/development.md`](docs/development.md)。

```bash
conda env create -f environment.yml
conda activate streampredict
make hooks
make check
```

如果环境已经创建，可使用：

```bash
conda env update -f environment.yml --prune
```

启动本地全栈（Redis + Kafka + MLflow + 模型推理服务 + 发布控制器 + FastAPI + Consumer + Dashboard）：

```bash
make up            # Docker Compose：Dashboard http://localhost:3000，API http://localhost:8000/docs，MLflow http://localhost:5001，Prometheus http://localhost:9090
make test-kafka    # 在运行中的 Kafka 上执行事件管道测试
```

在本地 Kubernetes（kind）中运行，含 KEDA 与 HPA 自动扩缩容（先 `make down` 释放端口）：

```bash
make k8s-up        # 创建集群、安装 metrics-server 与 KEDA、构建并部署全部服务
make k8s-status    # 查看 Pod、HPA、ScaledObject 与扩缩容事件
make k8s-down      # 删除集群
make down

# 或不使用 Docker 分别启动（API 在 Redis 不可用时降级为 cache bypass）
make api-dev
make dashboard-install && make dashboard-dev
```

## 完成定义

StreamPredict 项目达到第一版完成状态时，应满足：

- 用户可以通过公开或受控访问的 Dashboard 完成一次真实 Demo。
- 同步与异步预测链路都可运行、可测试、可观测。
- Redis、Kafka、模型推理服务、MLflow、对象存储和 Prometheus 均有明确职责。
- Kubernetes 能根据负载或 lag 扩缩容。
- Dashboard 能展示 RPS、lag、replicas、latency、success rate、cache hit rate 和 model version。
- 模型发布失败时可以回滚，并在 Dashboard 中看到回滚结果。
- 所有主要模块都有测试、启动说明和故障排查文档。

## License

`未开始` - 在公开发布前确定许可证。
