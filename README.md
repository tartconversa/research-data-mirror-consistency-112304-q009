# 跨区域科研数据镜像的一致性与可用目录

数据目录由本地合成科研记录组成，不包含真实实验室、研究人员或实验结果。使用 Python 3.11 至 3.14 和标准库，无需外部服务。

```sh
python3 -m unittest discover -s tests -v
python3 examples/demo_catalog.py
```

Windows PowerShell 可运行 `python -m unittest discover -s tests -v`。字段定义见 `docs/资料契约.md`，能力规则见 `docs/能力说明.md`。

## 交付能力

- **跨区域数据产品目录**：按数据集 × 区域展示副本版本、最后一致时间、覆盖分区与本地访问许可；撤回、未发布、过期、回报冲突的副本一律不显示为可用。
- **镜像接续**：按数据产品与分区折叠任务回报，检测部分成功、失败重试与源端修订过期，生成按优先级排序的接续计划（撤回传播 → 冲突核对 → 完整性修复 → 元数据/策略同步 → 源端刷新 → 重试/继续）。
- **按范围可用性查询**：支持 whole-release / partition-only / analysis 三种范围，结合共享范围 × 敏感级别 × 消费者身份的许可矩阵给出 allow / partial / deny，并随附快照锚点；已开始的读取凭快照保留身份，撤回或重新发布后附中文影响说明。
