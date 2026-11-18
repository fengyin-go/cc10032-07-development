/**
 * 序号7（流关联）关联结果版本接口：
 * 对接后端 /api/join-stream，提供版本生产、生命周期流转、检查点联动查询。
 */
import { request } from './client'

export type JoinVersionView = {
  version_id: string
  version_seq: number
  join_name: string
  join_type: string
  join_field: string
  window_start: number
  window_end: number
  merge_mode: string
  lifecycle_status: string
  stats: { total_keys: number; matched: number; unmatched: number; match_rate: number }
  late_dropped: number
  duplicate_overwrites: number
  key_count: number
  confirmed_keys: string[]
  snapshot_status: string
  checkpoint_id: string | null
  active_checkpoint_id: string | null
  checkpoint_count: number
  archive_condition: string
  restore_condition: string
  last_error: string | null
  updated_at: number
}

export type JoinVersionDetail = JoinVersionView & {
  snapshot: Record<string, unknown> | null
  sink_record: { id: number | null; snapshot_status: string; task_name: string | null }
  checkpoint_rows: Array<{
    checkpoint_id: string
    is_active: boolean
    status: string
    snapshot_ts: number
    join_keys: string[]
  }>
  history: Array<{ ts: number; from: string | null; to: string; reason: string }>
}

export type ProducePayload = {
  join_name: string
  left_events: Array<Record<string, unknown>>
  right_events: Array<Record<string, unknown>>
  merge_mode: string
  window_start?: number
  window_end?: number
  checkpoint_config?: string
  fail_sink?: boolean
  fail_checkpoint?: boolean
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body ?? {}),
  })
  const data = await response.json()
  if (!response.ok) {
    const detail = typeof data?.detail === 'object' ? data.detail : data
    throw new Error(detail?.error ?? detail?.message ?? `接口返回 ${response.status}`)
  }
  return data as T
}

async function get<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

export function listJoinVersions(params: {
  join_name?: string
  window_end?: number
  status?: string
} = {}): Promise<{ items: JoinVersionView[]; total: number }> {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== '') query.set(key, String(value))
  })
  return get(`/api/join-stream/versions?${query.toString()}`)
}

export function getJoinVersion(versionId: string): Promise<JoinVersionDetail> {
  return get(`/api/join-stream/versions/${versionId}`)
}

export function produceJoinVersion(payload: ProducePayload) {
  return post<{ ok: boolean; idempotent: boolean; version: JoinVersionView; reason?: string }>(
    '/api/join-stream/versions',
    payload,
  )
}

export function resumeJoinVersion(versionId: string) {
  return post<{ ok: boolean; version: JoinVersionView }>(
    `/api/join-stream/versions/${versionId}/resume`,
  )
}

export function confirmJoinVersion(versionId: string) {
  return post<{ ok: boolean; version: JoinVersionView }>(
    `/api/join-stream/versions/${versionId}/confirm`,
  )
}

export function archiveJoinVersion(versionId: string) {
  return post<{ ok: boolean; version: JoinVersionView }>(
    `/api/join-stream/versions/${versionId}/archive`,
  )
}

export function restoreArchivedVersion(versionId: string) {
  return post<{ ok: boolean; restored: boolean; checkpoint_id: string }>(
    `/api/join-stream/versions/${versionId}/restore-read`,
  )
}

export function switchJoinCheckpoint(versionId: string, checkpointId: string) {
  return post<{ ok: boolean; active_checkpoint_id: string }>(
    `/api/join-stream/versions/${versionId}/checkpoint/switch`,
    { checkpoint_id: checkpointId },
  )
}

export function refreshJoinCheckpoint(versionId: string) {
  return post<{ ok: boolean; checkpoint_id: string }>(
    `/api/join-stream/versions/${versionId}/checkpoint/refresh`,
    {},
  )
}

export function locateByCheckpoint(checkpointId: string) {
  return get<{ ok: boolean; replay_ready: boolean; version: JoinVersionDetail }>(
    `/api/join-stream/checkpoints/locate/${checkpointId}`,
  )
}

export function listJoinCheckpoints(versionId?: string) {
  const suffix = versionId ? `?version_id=${encodeURIComponent(versionId)}` : ''
  return get<{ items: Array<Record<string, unknown>>; total: number }>(
    `/api/join-stream/checkpoints${suffix}`,
  )
}

export function getLifecycleMeta() {
  return get<{
    lifecycle: string[]
    merge_modes: string[]
    archive_condition: string
    restore_condition: string
  }>('/api/join-stream/lifecycle')
}
