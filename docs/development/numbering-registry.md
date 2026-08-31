# 编号体系对照页(numbering registry)

LoomQ 历史修复/设计项使用多套编号前缀;本页是唯一对照成文点,发放新编号时同步更新本页。

## 1. 前缀族对照

成员语义以代码注释为唯一事实源(下述代表项的注释即锚点);本表为检索入口,不替代注释。

| 族 | 语义域(代表项锚点) | 已发放成员 |
|---|---|---|
| I | scheduler 组件拆分轮的不变量/收口(I4=索引即所有权账本,I5=collect-then-defer,I6=persistFailures 容错单一收口);连续无洞 | I1-I6 |
| W | wheel/命令层写守卫与冷锁原语(W8=create 主写入例外,IntentCanceler/WheelPersistence);W5-W7 空洞见 §2 | W1-W4、W8 |
| F | 冷路径命令修复(F6=守卫单一门,IntentCommandService writeFreshUnderColdLock) | F1-F6 |
| R | recovery/revision 修复(R8=种子,R9=终态推进,R10=overdue 推进,R15=墓碑守卫);余位空洞属正常 | R6、R8-R11、R13a、R15、R16、R21、R22 |
| C | 冷路径正确性(C2/C3/C4 子族)+ 深度审计轮(C18-,round 18;C18-5 为 r19 记档,见 Intent.setLastDeliveryId javadoc) | C2-1、C2-11、C3-2..C3-4、C4-1..C4-5、C18-1..C18-5 |
| P | 性能/耐久(P0=正确性级,P1=耐久/性能) | P0-2、P1-2、P1-4、P1-6 |

**守则**:新编号一律取所属族当前最大值 +1;禁止回填空洞、禁止重排既有编号;发放时同步更新本页。

## 2. W5-W7 空洞说明

> W5-W7 从未分配:W 系编号按修复落地时序顺次发放,W1-W4(冷锁守卫原语族,round 13-15)之后下一枚即 W8(create 主写入例外)。W5-W7 编号位被有意跳过——未预留语义、无对应被否决项、亦无删除记录,属编号时序的自然空洞。编号不连续是正常状态;检索历史修复时请勿假设"W 系连续"。

## 3. 两套 round 计数对照

- **系 1(历史项号,2026-08-13~08-16)**:commit 尾注与 spec 文件名中的 `round N` 为当批审计/修复**项号**,同日多枚并存(如 2026-08-13 当日 commit 群含 round 7/8/9/10/11/16/18 等多枚),数字空间与系 2 重叠;`docs/development/2026-08-15-r22..r27-fix-spec.md`(6 份,状态均为"已实现",落地 commit f84a695,2026-08-16)承接同一项号空间。
- **系 2(现行轮次,2026-08-24 起)**:commit 尾注 `(round N)` 为轮次计数,一轮一 commit(round 9/10 = 2026-08-24;round 11/12 = 2026-08-29;round 13-18 = 2026-08-30;截至本页更新最新为 round 18,commit 3eba7dc;每轮落地后更新本行)。
- **解读规则**:**commit 日期 < 2026-08-24 的 round N 按系 1 解读(查当日 commit 群与 docs/development spec 文件名);≥ 2026-08-24 按系 2**。同标签异义实证:2026-08-13 b44c5de "(round 9)"(项号)vs 2026-08-24 7140629 "(round 9)"(轮次)。
- **spec 文档在库情况**:docs/development 仅存 r22-r27 六份 fix-spec(及对应 execution-plan);其余历史设计稿在 `.claude/plans/`(gitignore,本地可溯、fresh clone 不可见;代码注释已按 r19 A8 自洽化,不依赖这些路径)。
