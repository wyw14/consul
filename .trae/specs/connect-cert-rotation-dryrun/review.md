# Consul Connect 代理证书轮换可观测演练模式 - 独立 Review

- [x] CP-R1: AC-1 完整演练且不替换活动证书（真实签发一次、后续 Get 同证书）
  - **Type**: `rule`
  - **Covers**: AC-1
  - **Evidence**: R1/R2 均核对 dryrun.go 无 `cd.Update`/certGroup 引用；TestManager_DryRunRotate_happyPath（签发计数 +1、序列号不变、后续 Get 不重签）。

- [x] CP-R2: AC-2 缓存命中检查冷热两态如实报告
  - **Type**: `rule`
  - **Covers**: AC-2
  - **Evidence**: happyPath + coldKeyReportsCacheMiss；dryRunCacheCheck 只读快照。

- [x] CP-R3: AC-3 根轮换重叠窗口正常共存
  - **Type**: `rule`
  - **Covers**: AC-3
  - **Evidence**: rootRotationOverlap：候选链新根（ChainsToActiveRoot/RootID=ca2）、活动证书 requireLeafValidUnderCA(ca1) 真实 x509.Verify、Remaining>0。

- [x] CP-R4: AC-4 窗口内旧根缺失即阶段失败
  - **Type**: `rule`
  - **Covers**: AC-4
  - **Evidence**: overlapViolationOldRootMissing（ReasonOverlapViolation、Stage=roots_overlap、Stages 长度 2）；窗口过期边界 overlapWindowElapsed。

- [x] CP-R5: AC-5 阶段化失败原因可程序化读取
  - **Type**: `rule`
  - **Covers**: AC-5
  - **Evidence**: rootsUnavailable/noActiveRoot/signingFailure/expiredCandidate 用例经 errors.As 断言 Stage+Reason；DryRunError.Unwrap；报告保留已完成阶段。

- [x] CP-R6: AC-6 断点续跑（跳过/复用/重跑/Clear）
  - **Type**: `rule`
  - **Covers**: AC-6
  - **Evidence**: resume（签发失败→Resume 成功且跳过计数正确、全跳过 Resume、非 Resume 全跑、Clear 后全新）；resumeChainFailureIsDeterministic（复用过期候选确定性再失败）。

- [x] CP-R7: AC-7 Manager 重启安全
  - **Type**: `rule`
  - **Covers**: AC-7
  - **Evidence**: restartAcrossManagers：A 活动证书未被候选替换；B 不继承检查点（Resumed=false、无 skipped）；B 的 Get/演练正常。

- [x] CP-R8: AC-8 过期活动证书演练不修复、真实 Get 换新
  - **Type**: `rule`
  - **Covers**: AC-8
  - **Evidence**: expiredActiveCert：CacheCheck.Expired=true、活动证书演练后仍过期、真实 Get 返回新有效证书并通过 ca 验证。

- [x] CP-R9: AC-9 多代理并发与同 key 合并
  - **Type**: `rule`
  - **Covers**: AC-9
  - **Evidence**: multipleProxiesConcurrent（10 key×5 并发 + 20 并发 Get，签发恰为 10，活动序列号稳定；delayingSigner+gate 保证确定性）；clearConcurrentWithRuns；`-race` 干净（DryRun -count=3、全包 -count=1）。

- [x] CP-R10: AC-10 可观测信号（日志+指标）
  - **Type**: `rule`
  - **Covers**: AC-10
  - **Evidence**: 每阶段 pass/fail/skipped 出口均经 recordDryRunStage（Info/Warn）+ stage 开始 Debug；`leaf-certs.dry_run.stage`/`.run` 计数器（stage/result/service/kind/datacenter 标签）；-v 实测日志可见。按 AC 备选条款以代码+日志捕获佐证（无 metric sink 单测）。

- [x] CP-R11: NFR-1/NFR-3 真实路径零漂移、检查点有界
  - **Type**: `rule`
  - **Covers**: AC-11
  - **Evidence**: git diff 证明 prepareCertificateRequest 逐字抽取（错误文案/rate limit/firstTime/metrics 保留）；全包既有测试零回归；checkpoint 为 certData 字段随 TTL 驱逐 GC。

- [x] CP-R12: 修复轮 I-1~I-3 有效且无回归（R2 复核）
  - **Type**: `rule`
  - **Covers**: AC-6, NFR-2
  - **Evidence**: Succeeded 放宽后误判风险排查（失败必先 append StageFail 且提前返回）；Duration 计量本次调用；commit 原子化并有确定性交错测试（非原子实现下该测试实测失败、原子实现下通过）。

- [x] CP-U1: 实现质量与包设计一致性
  - **Type**: `rubric`
  - **Covers**: AC-11
  - **Scale**: 1-5
  - **Anchors**: 1 = 侵入真实轮换或竞态；3 = 功能可用但抽象生硬；5 = 旁路干净、阶段模型清晰、复用得当、默认零漂移
  - **Pass Threshold**: >= 4
  - **Evidence**: R1 评 4/5（提出 I-1~I-3）；R2 复评修复质量与并发闭合论证充分，无 actionable，维持 4/5（达标）。

## Review History

### Review R1（fresh context，只读）
- **Result**: `fail`
- **Evidence**: 核心不变量、重构保真、AC-1~AC-10 证据全部成立（race DryRun 14/14、-count=20、全包 race、vet）；但存在 3 条 actionable：
  - I-1（high）：全 skipped 的 Resume 返回 nil error 但 Succeeded()==false，契约矛盾。
  - I-2（medium）：Resume 报告 Duration 含首次演练墙钟，违反"本次调用耗时"。
  - I-3（high）：ClearDryRunState 与收尾 commit 检查-设置非原子，极端交错下检查点可复活。
  - advisory：冷 key TTL 簿记需文档化、缺阶段开始日志、TestOverrideNow 无测试、Resume 旧根快照时效需注释、3 个原因码无测试、错误文案重复 ActiveRootID。
- **Route**: 物化为 tasks.md Issue I-1~I-4，回到 Implement。

### 修复轮（Implement）
- I-1：Succeeded() 改为"4 阶段齐全且无 StageFail"；resume r3 增断言。
- I-2：runDryRotate 增加 invocationStart，Duration 只计量本次调用，StartedAt 保留首次时刻。
- I-3：certData.commitDryRunCheckpoint 在 cd.lock 同一临界区内复核 abandoned 再赋值；新增确定性交错测试 TestManager_DryRunRotate_clearDuringCommitDoesNotResurrect（经临时非原子实现验证该测试确实失败，恢复后通过）与并发压测 clearConcurrentWithRuns。
- I-4：文档（TTL 簿记/先到 opts/Resume 快照时效）、Debug 阶段开始日志、错误文案、新增 noActiveRoot 与 overlapElapsedOldRootRemovedActiveChainInvalid（覆盖 TestOverrideNow 与 active_cert_chain_invalid）。
- **Verification**: DryRun 18 个用例全绿；`go vet` 干净；`go test -race ./agent/leafcert/...` ok；gofmt 干净；`go build ./agent/...` exit 0。

### Review R2（fresh context，只读复审）
- **Result**: `pass`
- **Evidence**: I-1~I-4 逐条行级核验全部 fixed；锁顺序、误判风险、工件水合、时钟测试确定性均复核无问题；命令实测：`go test -race -run DryRun` 17/17 PASS、`go test` 全包 ok、`go vet` exit 0、全包 race ok。AC-1~AC-11 全覆盖简表成立。
- **Advisory（不阻塞）**: A-1 建议为 I-3 增加确定性回归锁定 —— 已在修复轮补齐（commit 临界区内钩子 + 非原子实现反向验证），本条实质关闭。
- **Final**: 无 actionable 遗留，最新 Review 结果为 pass。
