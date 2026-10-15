# Consul Connect 代理证书轮换可观测演练模式 - 实施计划

## Task 1: 抽取 CSR 准备逻辑并扩展 Config/状态承载
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 `generate.go` 中将"读根校验 + SPIFFE URI/SAN 构造 + 私钥生成 + CSR 构造"抽取为可复用方法 `prepareCertificateRequest(req, roots) (pkPEM, csrPEM string, err error)`，`generateNewLeaf` 改为调用它；错误分支、rate limit、firstTime 语义保持不变。
  - `leafcert.Config` 新增 `RootRotationOverlapWindow` 与测试用 `TestOverrideNow func() time.Time`；`withDefaults` 补 `DefaultRootRotationOverlapWindow`（24h）。
  - Manager 新增 `now func() time.Time` 与独立 `dryRunGroup singleflight.Group`；`certData` 新增 `dryRun *dryRunCheckpoint`（cd.lock 保护）及访问器。
- **Acceptance Criteria Addressed**: AC-11, NFR-1
- **Test Requirements**:
  - `rule` TR-1.1: 既有测试零回归 — `go test ./agent/leafcert/...` → ok（0.5s）。
  - `rule` TR-1.2: `go vet ./agent/leafcert/...` 无输出。
  - `rubric` TR-1.3: 抽取质量自评 5/5：原函数仅薄封装，所有错误信息与分支逐字保留，无新依赖；证据为 generate.go diff。
- **Completion Evidence**:
  - [generate.go](file:///e:/swe-like/runs/hashicorp--consul/ae0daf509f0f/precheck-3615/agent/leafcert/generate.go) 抽取后全量既有测试通过；[leafcert.go](file:///e:/swe-like/runs/hashicorp--consul/ae0daf509f0f/precheck-3615/agent/leafcert/leafcert.go)、[cert.go](file:///e:/swe-like/runs/hashicorp--consul/ae0daf509f0f/precheck-3615/agent/leafcert/cert.go) 扩展编译/vet 干净。

## Task 2: 演练模式核心实现（dryrun.go）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 新增 [dryrun.go](file:///e:/swe-like/runs/hashicorp--consul/ae0daf509f0f/precheck-3615/agent/leafcert/dryrun.go)：阶段枚举（cache_check/roots_overlap/issue/chain_verify）、`DryRunOptions{Resume}`、`RotationReport`/`StageResult`/`CacheCheckInfo`/`RootOverlapInfo`/`CandidateCertInfo`、`DryRunError{Stage,Reason,Err}` 与稳定原因码常量。
  - `Manager.DryRunRotate`：独立 singleflight 按 key 合并；只读 `certData`，绝不 `cd.Update`；进程内 `dryRunCheckpoint`（roots 快照/私钥/CSR/候选/rotationStart/已完成阶段），Resume 跳过已成功阶段并复用工件；`ClearDryRunState`；TTL 驱逐自动回收检查点。
  - 根阶段：活动根 vs 签发根检测轮换；RotatedOutAt（零值用首次观测时刻）起算显式窗口；窗口内旧根缺失 → root_overlap_violation；轮换时校验活动证书仍可在当前根集合验证。
  - 签发阶段真实 SignCert；链阶段全根池 x509 Verify 并断言链顶==活动根；过期候选独立原因码。
  - hclog 结构化日志 + `leaf-certs.dry_run.stage`/`leaf-certs.dry_run.run` 计数器（stage/result/service/kind/datacenter 标签）。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-4, AC-5, AC-6, AC-10, NFR-2, NFR-3
- **Test Requirements**:
  - `rule` TR-2.1: 编译 + go vet 干净（已验证）。
  - `rule` TR-2.2: 导出 API 齐备（测试直接断言阶段/原因码/报告字段）。
  - `rubric` TR-2.3: 并发封装自评 5/5：旁路不触碰 value/index/state/TTL/正常 singleflight；锁顺序 m.lock→cd.lock 一致；证据为并发 race 测试与代码审查。
- **Completion Evidence**:
  - dryrun.go gofmt（1.26.9）干净；14 个 DryRun 测试全部通过；-race 3 连跑干净。

## Task 3: 演练模式测试矩阵（dryrun_test.go）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 新增 [dryrun_test.go](file:///e:/swe-like/runs/hashicorp--consul/ae0daf509f0f/precheck-3615/agent/leafcert/dryrun_test.go) 共 14 个用例：快乐路径不替换活动证书、冷 key miss、根轮换正常重叠（候选链新根/活动证书仍链旧根/窗口剩余>0）、窗口内旧根缺失违规、窗口已过边界、无根/签发失败/候选过期三类阶段失败、断点续跑（跳过+复用+确定性重复失败+Clear 重跑）、Manager 重启检查点不继承、过期活动证书不修复而真实 Get 换新、10 代理×5 并发（每 key 恰好 1 次签发、混合并发 Get 证书稳定）、报告结构。
  - helper 新增 `TestSigner.SignCallCount()`；确定性合并用 `delayingSigner`；窗口边界用 `RotatedOutAt` 锚点。
- **Acceptance Criteria Addressed**: AC-1..AC-9
- **Test Requirements**:
  - `rule` TR-3.1: `go test -run DryRun -count=1` 14/14 通过。
  - `rule` TR-3.2: `go test -race -run DryRun -count=3` 干净；普通 -count=5 稳定。
  - `rubric` TR-3.3: 确定性自评 5/5：无墙钟 sleep 依赖（唯一 2ms sleep 用例已改为 RotatedOutAt 绝对时间），复用 NewTestManager/TestSigner/requireLeafValidUnderCA。
- **Completion Evidence**:
  - `ok github.com/hashicorp/consul/agent/leafcert 0.364s`（14 用例）；race ok 2.070s；count=5 ok。

## Task 4: 全量验证与 changelog
- **Status**: `completed`
- **Priority**: low
- **Depends On**: Task 3
- **Description**:
  - `go build ./...`（exit 0）、`go vet ./agent/leafcert/...`（干净）、`go test -race ./agent/leafcert/...`（全量 ok 1.557s，含全部既有测试）。
  - 下游引用包测试编译：./agent/、./agent/proxycfg/、./agent/auto-config/、./agent/consul/、./agent/consul/servercert/、./agent/proxycfg-glue/ 全部 ok。
  - 新增 [.changelog/12647.txt](file:///e:/swe-like/runs/hashicorp--consul/ae0daf509f0f/precheck-3615/.changelog/12647.txt)（release-note:improvement）。
- **Acceptance Criteria Addressed**: AC-11
- **Test Requirements**:
  - `rule` TR-4.1: build/vet/race 全绿（输出见上）。
  - `rule` TR-4.2: changelog 格式与 12646.txt 等既有条目一致。
- **Completion Evidence**:
  - build-exit=0；race 全包 ok；changelog 已就位；gofmt 1.26.9 对新增/修改文件无 diff（CRLF 与现有检出一致，git autocrlf 归一化）。

## Task 5: 独立 Review（R1，fresh context）
- **Status**: `completed`
- **Priority**: low
- **Depends On**: Task 4
- **Description**:
  - 全新只读上下文独立审查（general_purpose_task），结论：核心不变量全通过、AC-1..AC-11 有证据（AC-11 rubric 4/5），但提出 3 条低严重度 actionable 与 6 条 advisory。Review 按契约记为 `fail`（存在 actionable），进入修复队列。
- **Acceptance Criteria Addressed**: AC-1..AC-11
- **Test Requirements**:
  - `rule` TR-5.1: actionable 全部物化为 Issue I-1..I-3 并修复。
  - `rubric` TR-5.2: 审查独立性与深度 4/5；证据为审查报告原文与 review.md。
- **Completion Evidence**:
  - R1 报告见 review.md Review History；实测命令：race DryRun 14/14、-count=20 稳定、全包 race ok、vet 干净。

## Issue I-1: 全跳过 Resume 时 Succeeded() 与 nil error 矛盾
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5
- **Discovered By**: Review R1
- **Description**:
  - Succeeded() 要求 4 阶段全为 pass；已完成检查点的 Resume 全部 skipped 却返回 nil error。
- **Acceptance Criteria Addressed**: AC-6
- **Test Requirements**:
  - `rule` TR-I-1.1: 全跳过 Resume 返回 nil 且 Succeeded()==true；证据 resume 测试 r3 断言。
- **Completion Evidence**:
  - dryrun.go Succeeded() 改为"无 StageFail 即成功"；resume 用例新增 `require.True(t, r3.Succeeded())`，通过。

## Issue I-2: Resume 报告 Duration 含首次演练墙钟
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 5
- **Discovered By**: Review R1
- **Description**:
  - Duration 由 StartedAt（首次时刻）计算，违反"本次调用耗时"文档。
- **Acceptance Criteria Addressed**: AC-6
- **Test Requirements**:
  - `rule` TR-I-2.1: Duration 以本次调用入口 invocationStart 计算，StartedAt 保留首次时刻；代码证据 + 全部测试通过。
- **Completion Evidence**:
  - runDryRotate 增加 invocationStart := m.now()，defer 中以此计算 Duration。

## Issue I-3: ClearDryRunState 与演练收尾的检查点复活竞态
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5
- **Discovered By**: Review R1
- **Description**:
  - defer 先查 abandoned 再 setDryRunCheckpoint，与 Clear 非原子，可致已清除检查点复活。
- **Acceptance Criteria Addressed**: AC-6, NFR-2, NFR-3
- **Test Requirements**:
  - `rule` TR-I-3.1: 锁内二次确认 abandoned 后再安装；并发 Clear+演练压测下活动证书不变、最终 Resume 不复活旧检查点；-race 干净。
- **Completion Evidence**:
  - cert.go 新增 commitDryRunCheckpoint（cd.lock 内复核 abandoned）；新增 TestManager_DryRunRotate_clearConcurrentWithRuns（8 演练+8 Clear 并发），race -count=3 通过。

## Issue I-4: 采纳的 advisory（文档/观测/补测）
- **Status**: `completed`
- **Priority**: low
- **Depends On**: Task 5
- **Discovered By**: Review R1（发现 4/5/6/7/8/9）
- **Description**:
  - 文档化冷 key 的 TTL 簿记副作用与 singleflight 先到 opts 语义；Resume 注释说明复用旧根快照的时效语义；补 Debug 级"阶段开始"日志；修正重叠违规错误文案重复 ActiveRootID；补 TestOverrideNow 时钟边界测试（窗口过期+旧根移除→active_cert_chain_invalid）与 no_active_root 原因码测试。
- **Acceptance Criteria Addressed**: AC-4, AC-5, AC-10
- **Test Requirements**:
  - `rule` TR-I-4.1: 新增 2 个用例通过；DryRun 共 17/17；race -count=3 干净；vet/gofmt 干净。
- **Completion Evidence**:
  - TestManager_DryRunRotate_noActiveRoot、TestManager_DryRunRotate_overlapElapsedOldRootRemovedActiveChainInvalid 通过；dryrun.go 注释/Debug 日志/文案已更新。

## Task 6: 独立 Review（R2，fresh context，复审修复）
- **Status**: `completed`
- **Priority**: low
- **Depends On**: Issue I-1, Issue I-2, Issue I-3, Issue I-4
- **Description**:
  - 全新只读上下文复审：验证 I-1~I-4 修复正确且无回归，确认全部 AC 证据成立后给出最终 pass。
- **Acceptance Criteria Addressed**: AC-1..AC-11
- **Test Requirements**:
  - `rule` TR-6.1: R2 结果 pass，无 actionable 遗留。
- **Completion Evidence**:
  - R2 结论 pass（I-1~I-4 行级核验 fixed，17/17+新增确定性交错共 18 个 DryRun 用例全绿）；其唯一 advisory A-1 已通过 commit 临界区钩子测试补齐（临时非原子实现下实测失败、原子实现下通过）；最终状态：`go vet` 干净、`go test -race ./agent/leafcert/...` ok、`go build ./agent/...` exit 0、gofmt 干净。详见 review.md。
