<template>
  <section class="page" data-module="join_stream">
    <header class="page-head">
      <div>
        <h2>流关联工作台（序号7）</h2>
        <p class="page-desc">
          双流事件按窗口版本生成确定性关联结果，聚合落库快照与检查点引用原子可见、可恢复
        </p>
      </div>
      <div class="page-actions">
        <button
          class="btn"
          :class="{ primary: tab === 'versions' }"
          data-testid="tab-versions"
          type="button"
          @click="switchTab('versions')"
        >
          关联结果版本
        </button>
        <button
          class="btn"
          :class="{ primary: tab === 'configs' }"
          data-testid="tab-configs"
          type="button"
          @click="switchTab('configs')"
        >
          关联配置
        </button>
      </div>
    </header>

    <!-- ============ 关联结果版本控制台 ============ -->
    <template v-if="tab === 'versions'">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">版本总数</span>
          <strong class="stat-value">{{ versions.length }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已确认/归档</span>
          <strong class="stat-value">{{ confirmedCount }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">检查点已关联</span>
          <strong class="stat-value">{{ checkpointLinkedCount }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">快照已落库</span>
          <strong class="stat-value">{{ snapshotCount }}</strong>
        </article>
      </div>

      <form class="filter-bar" @submit.prevent="reloadVersions">
        <label class="filter-item">
          <span>关联配置</span>
          <select v-model="filters.join_name" data-testid="filter-join">
            <option value="">全部配置</option>
            <option v-for="cfg in joinNames" :key="cfg" :value="cfg">{{ cfg }}</option>
          </select>
        </label>
        <label class="filter-item">
          <span>窗口结束时间</span>
          <input
            v-model="filters.window_end"
            data-testid="filter-window-end"
            placeholder="按窗口结束时间筛选"
          />
        </label>
        <label class="filter-item">
          <span>生命周期</span>
          <select v-model="filters.status" data-testid="filter-status">
            <option value="">全部状态</option>
            <option v-for="s in lifecycle" :key="s" :value="s">{{ s }}</option>
          </select>
        </label>
        <button class="btn" data-testid="version-query" type="submit">筛选窗口</button>
        <button class="btn ghost" type="button" @click="resetFilters">重置</button>
      </form>

      <form class="filter-bar" @submit.prevent="submitProduce">
        <label class="filter-item">
          <span>关联配置</span>
          <select v-model="produceForm.join_name" data-testid="produce-join" required>
            <option v-for="cfg in joinNames" :key="cfg" :value="cfg">{{ cfg }}</option>
          </select>
        </label>
        <label class="filter-item">
          <span>合并模式</span>
          <select v-model="produceForm.merge_mode" data-testid="produce-merge">
            <option value="window_cutoff">窗口结束时间统一截断（保留最早）</option>
            <option value="last_arrival">最后到达事件合并（保留最新）</option>
          </select>
        </label>
        <label class="filter-item">
          <span>窗口区间(秒)</span>
          <input v-model.number="produceForm.window_seconds" type="number" min="1" />
        </label>
        <button class="btn primary" data-testid="produce-version" type="submit">
          生成关联结果版本
        </button>
      </form>

      <p v-if="produceMessage" class="page-desc" data-testid="produce-message">
        {{ produceMessage }}
      </p>

      <table class="data-table" data-testid="version-table">
        <thead>
          <tr>
            <th>版本</th>
            <th>关联配置</th>
            <th>窗口区间</th>
            <th>合并模式</th>
            <th>键数(命中/未命中)</th>
            <th>重复键/迟到</th>
            <th>生命周期</th>
            <th>聚合落库</th>
            <th>检查点引用</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="version in versions"
            :key="version.version_id"
            :data-testid="`version-row-${version.version_id}`"
            :data-status="version.lifecycle_status"
          >
            <td>#{{ version.version_seq }}</td>
            <td>{{ version.join_name }}</td>
            <td>{{ formatTs(version.window_start) }} ~ {{ formatTs(version.window_end) }}</td>
            <td>{{ version.merge_mode === 'last_arrival' ? '最后到达' : '窗口截断' }}</td>
            <td>
              {{ version.stats.total_keys }}
              ({{ version.stats.matched }}/{{ version.stats.unmatched }})
            </td>
            <td>{{ version.duplicate_overwrites }}/{{ version.late_dropped }}</td>
            <td>{{ version.lifecycle_status }}</td>
            <td>{{ version.snapshot_status }}</td>
            <td>
              <span v-if="version.active_checkpoint_id">{{ version.active_checkpoint_id }}</span>
              <span v-else>—</span>
            </td>
            <td class="row-actions">
              <button class="link" type="button" @click="openDetail(version.version_id)">
                状态详情
              </button>
              <button
                v-if="version.lifecycle_status === '计算中'"
                class="link"
                type="button"
                @click="doResume(version.version_id)"
              >
                中断恢复
              </button>
              <button
                v-if="version.lifecycle_status === '待检查点'"
                class="link"
                type="button"
                @click="doConfirm(version.version_id)"
              >
                确认窗口
              </button>
              <button
                v-if="version.lifecycle_status === '已确认'"
                class="link"
                type="button"
                @click="doArchive(version.version_id)"
              >
                归档
              </button>
              <button
                v-if="version.lifecycle_status === '已归档'"
                class="link"
                type="button"
                @click="doRestoreRead(version.version_id)"
              >
                只读恢复
              </button>
            </td>
          </tr>
          <tr v-if="!versions.length">
            <td colspan="10" class="empty-state">暂无关联结果版本，可先生成一个版本</td>
          </tr>
        </tbody>
      </table>
    </template>

    <!-- ============ 关联配置（通用工作台数据） ============ -->
    <template v-else>
      <table class="data-table" data-testid="config-table">
        <thead>
          <tr>
            <th v-for="column in configMeta.fields" :key="column">{{ column }}</th>
            <th>当前状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in configRows" :key="String(row.id)">
            <td v-for="column in configMeta.fields" :key="column">{{ row[column] ?? '—' }}</td>
            <td>{{ row.status }}</td>
          </tr>
        </tbody>
      </table>
    </template>

    <!-- ============ 版本状态详情 ============ -->
    <div v-if="detail" class="modal-backdrop" data-testid="version-detail-modal">
      <div class="modal" style="width: min(760px, 100%)">
        <h3>版本状态详情 #{{ detail.version_seq }}</h3>
        <p class="page-desc">
          生命周期：<strong>{{ detail.lifecycle_status }}</strong>
          ｜ 版本ID：{{ detail.version_id }}
        </p>

        <h4>窗口与关联结果数据</h4>
        <table class="data-table">
          <tbody>
            <tr>
              <th>窗口区间</th>
              <td>{{ formatTs(detail.window_start) }} ~ {{ formatTs(detail.window_end) }}</td>
            </tr>
            <tr>
              <th>合并模式</th>
              <td>{{ detail.merge_mode === 'last_arrival' ? '最后到达合并' : '窗口结束截断' }}</td>
            </tr>
            <tr>
              <th>聚合指标</th>
              <td>
                总键 {{ detail.stats.total_keys }} ｜ 命中 {{ detail.stats.matched }} ｜
                未命中 {{ detail.stats.unmatched }} ｜
                命中率 {{ (detail.stats.match_rate * 100).toFixed(1) }}%
              </td>
            </tr>
            <tr>
              <th>已确认键</th>
              <td>{{ detail.confirmed_keys.join('、') || '（窗口未确认）' }}</td>
            </tr>
            <tr>
              <th>重复键覆盖 / 迟到截断</th>
              <td>{{ detail.duplicate_overwrites }} / {{ detail.late_dropped }}</td>
            </tr>
          </tbody>
        </table>

        <h4>聚合落库快照</h4>
        <p class="page-desc">
          状态：{{ detail.sink_record.snapshot_status }}
          ｜ 任务：{{ detail.sink_record.task_name ?? '—' }}
          ｜ 落库记录ID：{{ detail.sink_record.id ?? '—' }}
        </p>

        <h4>检查点引用（可切换）</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>检查点ID</th>
              <th>快照时间</th>
              <th>键数</th>
              <th>状态</th>
              <th>活动引用</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="cp in detail.checkpoint_rows" :key="cp.checkpoint_id">
              <td>{{ cp.checkpoint_id }}</td>
              <td>{{ formatTs(cp.snapshot_ts) }}</td>
              <td>{{ cp.join_keys.length }}</td>
              <td>{{ cp.status }}</td>
              <td>{{ cp.is_active ? '✅ 当前' : '' }}</td>
              <td class="row-actions">
                <button
                  v-if="!cp.is_active"
                  class="link"
                  type="button"
                  @click="doSwitchCheckpoint(detail.version_id, cp.checkpoint_id)"
                >
                  切换引用
                </button>
                <button
                  v-else
                  class="link"
                  type="button"
                  @click="doRefreshCheckpoint(detail.version_id)"
                >
                  刷新检查点
                </button>
              </td>
            </tr>
          </tbody>
        </table>

        <h4>归档/恢复条件</h4>
        <p class="page-desc">归档条件：{{ detail.archive_condition }}</p>
        <p class="page-desc">恢复条件：{{ detail.restore_condition }}</p>

        <h4>单向状态链路</h4>
        <ul class="page-desc">
          <li v-for="item in detail.history" :key="`${item.ts}-${item.to}`">
            {{ formatTs(item.ts) }} {{ item.from ?? '—' }} → {{ item.to }}：{{ item.reason }}
          </li>
        </ul>

        <div class="page-actions">
          <button class="btn" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ versions.length }} 个关联结果版本</span>
      <span v-if="errorMessage" class="error-text" data-testid="version-error">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  archiveJoinVersion,
  confirmJoinVersion,
  getJoinVersion,
  getLifecycleMeta,
  listJoinCheckpoints,
  listJoinVersions,
  produceJoinVersion,
  refreshJoinCheckpoint,
  resumeJoinVersion,
  restoreArchivedVersion,
  switchJoinCheckpoint,
  type JoinVersionDetail,
  type JoinVersionView,
} from '@/api/join-service'
import { listEntries, moduleMeta } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const configMeta = moduleMeta('join_stream')
const tab = ref<'versions' | 'configs'>('versions')
const lifecycle = ref<string[]>([])
const versions = ref<JoinVersionView[]>([])
const configRows = ref<EntryRow[]>([])
const detail = ref<JoinVersionDetail | null>(null)
const errorMessage = ref('')
const produceMessage = ref('')

const filters = ref({ join_name: '', window_end: '', status: '' })
const produceForm = ref({
  join_name: '',
  merge_mode: 'window_cutoff',
  window_seconds: 10,
})

const joinNames = computed(() =>
  configRows.value.map((row) => String(row[configMeta.fields[0]])).filter(Boolean),
)
const confirmedCount = computed(
  () =>
    versions.value.filter((v) => ['已确认', '已归档'].includes(v.lifecycle_status)).length,
)
const checkpointLinkedCount = computed(
  () => versions.value.filter((v) => v.active_checkpoint_id).length,
)
const snapshotCount = computed(
  () => versions.value.filter((v) => v.snapshot_status === '已落库').length,
)

function formatTs(ts: number): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false })
}

function switchTab(target: 'versions' | 'configs') {
  tab.value = target
}

function resetFilters() {
  filters.value = { join_name: '', window_end: '', status: '' }
  void reloadVersions()
}

async function reloadVersions() {
  errorMessage.value = ''
  try {
    const payload = await listJoinVersions({
      join_name: filters.value.join_name || undefined,
      window_end: filters.value.window_end
        ? Number(filters.value.window_end)
        : undefined,
      status: filters.value.status || undefined,
    })
    versions.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '关联版本读取失败'
  }
}

async function reloadConfigs() {
  const payload = await listEntries('join_stream')
  configRows.value = payload.items
  if (!produceForm.value.join_name && joinNames.value.length) {
    produceForm.value.join_name = joinNames.value[0]
  }
}

/** 生成演示双流事件：同一批事件重复提交时后端按确定性版本幂等处理。 */
function buildDemoEvents(joinName: string, windowSeconds: number) {
  const now = Math.floor(Date.now() / 1000)
  const stamp = `${joinName.slice(-1)}${now % 100000}`
  const left = ['u1', 'u2', 'u4'].map((key, i) => ({
    event_id: `L-${stamp}-${key}`,
    event_time: now + i,
    payload: { user_id: key, amount: 10 + i },
    source: 'left_stream',
  }))
  const right = ['u1', 'u2', 'u3'].map((key, i) => ({
    event_id: `R-${stamp}-${key}`,
    event_time: now + i + 0.5,
    payload: { user_id: key, amount: 20 + i },
    source: 'right_stream',
  }))
  return {
    left_events: left,
    right_events: right,
    window_start: now,
    window_end: now + windowSeconds,
  }
}

async function submitProduce() {
  errorMessage.value = ''
  produceMessage.value = ''
  try {
    const result = await produceJoinVersion({
      join_name: produceForm.value.join_name,
      merge_mode: produceForm.value.merge_mode,
      ...buildDemoEvents(produceForm.value.join_name, produceForm.value.window_seconds),
    })
    produceMessage.value = result.idempotent
      ? `幂等命中既有版本（${result.reason ?? '并发重试只保留一版'}）`
      : '关联结果版本已生成，聚合快照与检查点引用原子生效'
    await reloadVersions()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '版本生成失败'
  }
}

async function openDetail(versionId: string) {
  errorMessage.value = ''
  try {
    detail.value = await getJoinVersion(versionId)
    // 检查点模块另一个入口：联动拉取该版本的检查点列表
    await listJoinCheckpoints(versionId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '状态详情读取失败'
  }
}

async function runLifecycleAction(
  action: (id: string) => Promise<unknown>,
  versionId: string,
  reloadDetail = true,
) {
  errorMessage.value = ''
  try {
    await action(versionId)
    await reloadVersions()
    if (reloadDetail && detail.value?.version_id === versionId) {
      detail.value = await getJoinVersion(versionId)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '状态流转失败'
  }
}

const doResume = (id: string) => runLifecycleAction((v) => resumeJoinVersion(v), id)
const doConfirm = (id: string) => runLifecycleAction((v) => confirmJoinVersion(v), id)
const doArchive = (id: string) => runLifecycleAction((v) => archiveJoinVersion(v), id)
const doRestoreRead = (id: string) =>
  runLifecycleAction((v) => restoreArchivedVersion(v), id)

async function doSwitchCheckpoint(versionId: string, checkpointId: string) {
  errorMessage.value = ''
  try {
    await switchJoinCheckpoint(versionId, checkpointId)
    detail.value = await getJoinVersion(versionId)
    await reloadVersions()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检查点切换失败'
  }
}

async function doRefreshCheckpoint(versionId: string) {
  errorMessage.value = ''
  try {
    await refreshJoinCheckpoint(versionId)
    detail.value = await getJoinVersion(versionId)
    await reloadVersions()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检查点刷新失败'
  }
}

onMounted(async () => {
  const meta = await getLifecycleMeta()
  lifecycle.value = meta.lifecycle
  await reloadConfigs()
  await reloadVersions()
})
</script>
