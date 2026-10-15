# Consul Connect 代理证书轮换可观测演练模式 - 产品需求文档

## Overview
- **Summary**: 在 `agent/leafcert` 证书管理器中新增"演练模式"（dry-run）：允许操作员在**不替换活动证书**的前提下，完整走查代理叶子证书轮换的关键环节——缓存命中检查、根 CA 读取与重叠窗口校验、真实签发、证书链验证，并产出结构化、阶段化、可观测的演练报告；失败时给出阶段化原因，并可从上次成功阶段继续（断点续跑）。
- **Purpose**: 根 CA 滚动更新与叶子证书轮换是 Connect 代理断连的高发操作。变更前缺乏"先演练、后执行"的安全手段：操作员无法提前验证 CSR 签发链路、新证书能否在当前根集合下验证通过、旧根是否仍处于受信重叠窗口。演练模式把一次轮换拆成可观测、可续跑的阶段，降低变更风险。
- **Target Users**: Consul 运维人员、平台 SRE、排障工程师；以及以编程方式做上线前巡检的自动化工具。

## Goals
- 提供 `Manager.DryRunRotate` 入口，按固定阶段顺序执行：缓存命中检查 → 根 CA / 重叠窗口检查 → 真实签发（生成私钥+CSR 并调用 SignCert）→ 新证书链验证。
- 演练过程**绝不**修改缓存中的活动证书（`certData.value/index/state` 不变），不影响正常 `Get`/`Notify` 路径；演练产生的候选证书只用于报告与续跑工件。
- 演练报告 `RotationReport` 包含每阶段状态（成功/失败/跳过）、耗时、阶段化原因码、缓存命中情况、候选证书信息、根轮换重叠窗口信息。
- 根 CA 滚动更新时，显式校验并报告"新旧根共存的重叠窗口"：窗口大小由配置显式给出；窗口内旧根必须仍在受信根集合中，否则以阶段失败暴露。
- 失败返回带阶段与原因码的错误；进程内保留每 key 的演练检查点，支持以 `Resume` 方式从上次成功阶段之后继续：已完成阶段跳过，失败阶段重新执行。
- 演练全过程有指标与结构化日志（按阶段、按结果）。
- 覆盖重启、证书过期、多代理并发三类测试，并覆盖断点续跑、重叠窗口正常/违规/过期、各阶段失败原因。

## Non-Goals
- 不新增/修改 HTTP API、CLI 命令或 UI 页面（本次仅提供 leafcert 包内的程序化能力与观测信号）。
- 不改变真实轮换路径（`Get`/`attemptLeafRefresh`/`generateNewLeaf`）的现有时序、退避与限流行为（仅做等价抽取重构）。
- 不做检查点的磁盘持久化：断点续跑作用域为单个 Manager 进程生命周期；Manager 重启后演练从零开始，且活动证书行为不受影响（由测试固化）。
- 不改动服务端根 CA 裁剪/保留策略，只在客户端读取并校验既有根集合。
- 不引入新的第三方依赖。

## Background & Context
- 叶子证书管理集中在 `agent/leafcert`：`leafcert.go`（Manager/缓存/TTL/指标）、`generate.go`（`attemptLeafRefresh`、`generateNewLeaf`、根变更抖动）、`cert.go`（`certData`/`fetchState`）、`roots.go`（共享 rootWatcher）、`structs.go`（`ConnectCALeafRequest`）。
- 现有轮换已在 `generateNewLeaf` 内完成：读根 → 构造 SPIFFE URI/DNS/IP SAN → 生成 EC 私钥与 CSR → `CertSigner.SignCert` → `connect.ParseCert` → 记录 authorityKeyID；但与缓存写入强耦合，无法"只签发不安装"。
- 根集合 `structs.IndexedCARoots` 已保留被轮换出的旧根（`CARoot.Active=false`、`RotatedOutAt`），这天然构成重叠窗口，但客户端目前既不显式校验该窗口，也不对外报告。
- 该 IBM 分支已为轮换增加可观测基础（`fetchState.lastRenewalFailureReason`、`cert_renewal_failure`/`cert_expiry` 指标、分级日志），演练模式沿用相同风格。
- 测试基建在同包 `leafcert_test_helpers.go`：`NewTestManager`、`TestSigner`（可轮换 CA、注入签发错误、签发过期证书、保留旧根）、`testRootsReader`、`requireLeafValidUnderCA`、`testAsyncGet`。

## Functional Requirements
- **FR-1**: Manager 提供演练入口，对指定 `ConnectCALeafRequest` 依次执行四个阶段：缓存命中检查、根与重叠窗口检查、签发、证书链验证；返回 `RotationReport`。
- **FR-2**: 演练不替换活动证书：演练前后同一 key 的活动证书（序列号、ModifyIndex、fetchedAt、fetchState）保持不变；演练后普通 `Get` 仍命中原证书；不触碰 TTL 堆与正常刷新 singleflight。
- **FR-3**: 缓存检查阶段报告：是否存在活动证书、是否命中（有可用值）、序列号、index、authorityKeyID、有效期、是否已过期。
- **FR-4**: 根检查阶段读取当前根集合；当活动根与活动证书签发根不一致时判定"根轮换进行中"，基于显式配置的重叠窗口计算窗口起止/剩余时间；校验活动证书仍能在当前根集合（含旧根）下验证通过；窗口内若旧根已从根集合消失则该阶段失败。
- **FR-5**: 签发阶段真实生成私钥+CSR 并调用 `CertSigner.SignCert`，得到候选证书；候选证书及其私钥仅保存在演练检查点/报告中，绝不写入 `certData.value`。
- **FR-6**: 证书链验证阶段解析候选证书，用当前全部受信根（及候选自带中间证书）做 x509 链验证，并确认候选证书链接到**当前活动根**；候选已过期同样判失败。
- **FR-7**: 任一阶段失败时，返回的 error 可程序化取出失败阶段标识与稳定原因码；报告保留已完成阶段的结果。
- **FR-8**: 支持断点续跑：失败后以 Resume 再次演练时，上次已成功的阶段标记为"跳过"并复用检查点工件（根快照/私钥与 CSR/候选证书），从失败阶段重新执行；不使用 Resume 时丢弃旧检查点从头执行；提供清理检查点的方法。
- **FR-9**: 同 key 并发演练通过 singleflight 合并为一次执行；不同 key（模拟多代理/多服务）彼此独立、可并行；演练与正常 `Get` 并发时互不污染、无数据竞争。
- **FR-10**: 重叠窗口时长为 `leafcert.Config` 中的显式配置项，缺省有默认值；另提供测试用时钟/窗口覆写手段。
- **FR-11**: 每个阶段的开始/成功/失败产生结构化日志，并发出带 stage/result/service 标签的计数器指标，以及演练整体结果计数器。

## Non-Functional Requirements
- **NFR-1**: 对正常轮换路径零行为变更：现有 `agent/leafcert` 全部测试在重构后保持通过。
- **NFR-2**: 并发安全：新增状态全部受锁保护；`go test -race ./agent/leafcert/...` 无告警。
- **NFR-3**: 检查点为进程内、有界、可清理：缓存条目被 TTL 驱逐时对应演练检查点一并释放；不复用已停止 Manager 的任何状态。
- **NFR-4**: 新代码沿用该包既有风格（IBM BUSL-1.1 版权头、hclog、go-metrics、testify），不引入新依赖。
- **NFR-5**: 测试保持确定性：复用 `TestOverrideCAChangeInitialDelay` 风格的测试覆写，不依赖真实墙钟长等待。

## Constraints
- **Technical**: Go 模块内实现；只能使用仓库已有依赖（singleflight、go-metrics、hclog、x509、testify）；重构 `generateNewLeaf` 时必须保持错误路径（rate limit 特殊处理、firstTime 语义）不变。
- **Business**: 不改变线上默认行为；演练为显式调用的旁路能力。
- **Dependencies**: `CertSigner`、`RootsReader` 接口维持不变；复用 `TestSigner` 测试基建并允许小幅扩展其观测能力（如签发计数）。

## Assumptions
- 演练由受信运维侧发起，签发产生的候选证书不会带来额外副作用（签发接口幂等读语义、不占用服务端状态）；即便产生额外 CSR，频率远低于正常轮换。
- "多代理并发"以同一 Manager 下多 service key 的并发演练来等价模拟（真实多代理为多进程，但共享的服务端合并/链路逻辑与单进程多 key 一致，且更易做竞态检测）。
- 根集合中的 `CARoot.RotatedOutAt` 可能为零值（测试/旧数据）；此时以本次进程内首次观测到轮换的时刻作为窗口起算点。
- 重叠窗口默认值选取 24 小时作为运维上"明确且保守"的最小共存预期，仅用于客户端演练校验与报告，不改变服务端行为。

## Acceptance Criteria

### AC-1: 演练完整执行且不替换活动证书
- **Type**: `rule`
- **Given**: 一个已通过正常 `Get` 持有活动叶子证书的 key
- **When**: 调用 `DryRunRotate`（非 Resume）
- **Then**: 四个阶段全部成功；签发接口被实际调用一次；演练后该 key 的活动证书序列号/ModifyIndex 与演练前完全相同，随后普通 `Get` 返回同一证书（缓存命中）
- **Pass Condition**: 测试断言四阶段成功、签发发生、活动证书字段不变、后续 Get 命中
- **Evidence**: `agent/leafcert/dryrun_test.go` 中 happy-path 测试通过输出

### AC-2: 缓存命中检查阶段如实反映缓存状态
- **Type**: `rule`
- **Given**: 冷 key（从未 Get）与热 key（已 Get）两种情形
- **When**: 分别执行演练
- **Then**: 冷 key 报告无活动证书/未命中；热 key 报告命中并带有序列号、index、authorityKeyID、有效期
- **Pass Condition**: 两情形报告字段断言全部成立
- **Evidence**: 对应测试通过输出

### AC-3: 根轮换重叠窗口——正常共存
- **Type**: `rule`
- **Given**: 活动证书由旧根签发，根集合同时包含旧根（Active=false）与新活动根
- **When**: 在重叠窗口内执行演练
- **Then**: 报告标记轮换进行中、旧根仍受信、窗口剩余时间 > 0；候选证书在新活动根下验证通过；活动证书仍能在含旧根的根集合下验证通过
- **Pass Condition**: 上述断言全部成立
- **Evidence**: 重叠窗口测试通过输出

### AC-4: 根轮换重叠窗口——窗口内旧根缺失被判失败
- **Type**: `rule`
- **Given**: 轮换刚发生（在窗口内）但根集合已不含旧根
- **When**: 执行演练
- **Then**: 根/重叠阶段失败，错误带稳定原因码（overlap 违规类），且失败阶段标识为根检查阶段
- **Pass Condition**: 错误的阶段==根检查阶段且原因码为 overlap 违规
- **Evidence**: 对应测试通过输出

### AC-5: 阶段化失败原因
- **Type**: `rule`
- **Given**: 可分别注入"无可用根/签发失败/候选证书过期（链验证失败）"三类故障
- **When**: 执行演练
- **Then**: 分别在根检查、签发、链验证阶段失败；错误可程序化读取阶段与原因码；报告包含已完成阶段结果
- **Pass Condition**: 三种场景的失败阶段与原因码逐一断言成立
- **Evidence**: 阶段失败测试通过输出

### AC-6: 断点续跑
- **Type**: `rule`
- **Given**: 演练在签发阶段失败（根检查已成功）
- **When**: 以 Resume 重新演练（故障解除）
- **Then**: 根检查阶段标记跳过并复用快照，签发阶段重新执行并成功，链验证成功；不使用 Resume 的重跑则所有阶段重新执行
- **Pass Condition**: 跳过/执行标记、签发调用次数、最终成功均符合预期
- **Evidence**: 续跑测试通过输出

### AC-7: Manager 重启安全
- **Type**: `rule`
- **Given**: Manager A 完成过正常 Get 与一次演练
- **When**: 停止 A，用同一 signer/roots 新建 Manager B
- **Then**: A 的活动证书从未被候选证书替换；B 不继承演练检查点（演练从零阶段开始）；B 的正常 Get 与演练均正常工作
- **Pass Condition**: 序列号对比、阶段跳过标记、B 上 Get/演练结果断言成立
- **Evidence**: 重启测试通过输出

### AC-8: 过期证书演练
- **Type**: `rule`
- **Given**: 缓存中活动证书已过期（注入签发过期证书）
- **When**: 执行演练
- **Then**: 缓存阶段如实报告活动证书已过期；签发与候选链验证成功；演练不替换、不"修复"活动证书；随后正常 Get 路径能完成真实换新
- **Pass Condition**: 过期标记、活动证书不变、真实 Get 后换新成立
- **Evidence**: 过期测试通过输出

### AC-9: 多代理并发与同 key 合并
- **Type**: `rule`
- **Given**: N 个不同 service key，每个 key 上并发发起 M 个演练，并与正常 Get 并发混合
- **When**: 全部演练返回
- **Then**: 每个 key 的实际签发仅发生 1 次（同 key 合并），不同 key 并行独立；全部报告成功；活动缓存未被污染；`-race` 无告警
- **Pass Condition**: 签发计数恰为 key 数，`-race` 干净，抽查活动证书不变
- **Evidence**: 并发测试通过输出与 race 检测结果

### AC-10: 可观测信号
- **Type**: `rule`
- **Given**: 成功与失败两类演练
- **When**: 演练执行
- **Then**: 产生按 stage/result 的结构化日志，并对每阶段结果与整体结果发出计数器指标（标签至少含 stage、result）
- **Pass Condition**: 代码路径中每个阶段出口必有日志与指标；测试通过指标 sink（若可行）或代码审查+日志捕获佐证
- **Evidence**: 代码与测试证据

### AC-11: 实现质量与既有行为保持
- **Type**: `rubric`
- **Dimension**: 代码与包设计一致性（阶段抽象清晰、与现有 Manager 风格统一、并发模型正确、重构无行为漂移）
- **Scale**: 1-5
- **Anchors**: 1 = 侵入正常轮换路径或引入竞态/资源泄漏；3 = 功能可用但抽象生硬或有明显重复；5 = 旁路干净、阶段模型清晰、复用得当、默认行为零漂移
- **Pass Threshold**: >= 4
- **Evidence**: 独立代码审查 + 现有 leafcert 全量测试结果
