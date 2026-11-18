# 实时计算服务中心

面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式源端 | `stream_source` | 数据源配置 | 源端名称、消息队列、Topic名称 |
| 窗口配置 | `window_config` | 窗口策略 | 策略名称、窗口类型、窗口大小 |
| 计算算子 | `calc_operator` | 算子定义 | 算子名称、算子类型、输入字段 |
| 状态存储 | `state_store` | 状态配置 | 配置名称、后端类型、状态键模式 |
| 水位追踪 | `watermark_track` | 水位记录 | 水位名称、数据源、当前水位 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控指标、触发条件 |
| 滑动指标 | `sliding_index` | 指标计算 | 指标名称、计算窗口、聚合函数 |
| 流关联 | `join_stream` | 关联配置 | 配置名称、左流源、右流源 |
| 结果落地 | `sink_output` | 输出配置 | 配置名称、输出类型、目标地址 |
| 补数任务 | `backfill_job` | 补数作业 | 作业名称、目标指标、补数范围 |
| CEP规则 | `cep_rule` | 事件序列 | 序列名称、事件类型、匹配顺序 |
| 吞吐管理 | `throughput_mgr` | 吞吐配置 | 配置名称、数据源、最大QPS |
| 检查点 | `checkpoint_mgr` | 检查点配置 | 配置名称、间隔时间、超时时长 |
| 故障恢复 | `restore_plan` | 恢复方案 | 方案名称、任务范围、恢复模式 |
| 指标血缘 | `metric_lineage` | 血缘记录 | 记录ID、指标名称、上游源端 |
| 迟到处理 | `late_handler` | 处理策略 | 策略名称、允许延迟、迟到动作 |
| 任务拓扑 | `task_topology` | 拓扑配置 | 拓扑名称、算子链、并行度 |
| 数据采样 | `data_sampler` | 采样策略 | 策略名称、采样方式、采样比例 |
| 调试追踪 | `debug_trace` | 调试会话 | 会话名称、目标算子、采样数据 |
| 聚合落库 | `aggregate_sink` | 聚合任务 | 任务名称、源指标集、聚合粒度 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 序号7：流关联结果版本（可恢复、可检查点消费）

流关联模块在通用 CRUD 之外提供关联结果版本能力，代码位于
`backend/app/services/join_stream.py` 与 `backend/app/routers/join_pipeline.py`，
前端入口为「流关联工作台」（`frontend/src/views/JoinWorkbench.vue`，路由 `/join_stream`）。

- **确定性版本**：版本 ID 只由 `关联配置 + 窗口起止 + 合并模式` 决定，
  同窗口并发重试命中同一版本，只保留一版且完全幂等；窗口与版本是唯一映射，
  每个关联键只能在一个窗口版本中生成确定结果。
- **重复键与迟到事件**：`window_cutoff`（窗口结束时间统一截断、保留最早）和
  `last_arrival`（按最后到达事件合并、保留最新）两种模式；晚于窗口结束的事件被截断计数。
- **原子可见**：关联结果数据、聚合落库快照（`aggregate_sink`）与检查点引用
  （`checkpoint_mgr`，保存窗口、键和聚合结果）在同一事务内生效，任一步失败整体回滚并保留原状态。
- **单向生命周期**：`计算中 → 待检查点 → 已确认 → 已归档`，
  已确认/已归档窗口不能回退；中断恢复只补未完成键；归档版本只能凭检查点引用只读恢复。
- **检查点联动**：`GET /api/join-stream/checkpoints/locate/{checkpoint_id}`
  可由检查点反查完整版本用于回放；版本详情窗口可在同版本检查点历史内切换/刷新引用，
  并发刷新只保留一个活动检查点。
- **工作台**：支持按关联配置、窗口结束时间、生命周期筛选，
  每个版本展示检查点引用与聚合落库状态，状态详情可按窗口版本核对数据与快照。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/join-stream/versions` | 读取双流事件生成关联结果版本（幂等） |
| GET | `/api/join-stream/versions` | 工作台窗口筛选 |
| GET | `/api/join-stream/versions/{id}` | 版本状态详情（快照+检查点+状态链路） |
| POST | `/api/join-stream/versions/{id}/resume` | 中断恢复，只补未完成键 |
| POST | `/api/join-stream/versions/{id}/confirm` | 确认窗口（封存不可回退） |
| POST | `/api/join-stream/versions/{id}/archive` | 归档（归档条件写入版本数据） |
| POST | `/api/join-stream/versions/{id}/restore-read` | 归档版本只读恢复 |
| POST | `/api/join-stream/versions/{id}/checkpoint/switch` | 切换活动检查点引用 |
| POST | `/api/join-stream/versions/{id}/checkpoint/refresh` | 刷新检查点 |
| GET | `/api/join-stream/checkpoints/locate/{cp_id}` | 检查点回放定位完整版本 |
| GET | `/api/join-stream/checkpoints` | 检查点模块联动入口 |
| GET | `/api/join-stream/statistics` | 版本生命周期分布统计 |

领域：数据工程
子方向：数据分析与数据应用
