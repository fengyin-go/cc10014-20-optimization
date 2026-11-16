import { useCallback, useEffect, useMemo, useState } from 'react'

import { fetchJson, request } from '@/api/client'

const ENDPOINT = '/api/dedup'
const columns = [
  '批次编号',
  '关联数据集',
  '去重字段',
  '规则版本',
  '迟到记录数',
  '影响重复组',
  '归并结果',
  '复核状态',
]
const actions = ['执行归并', '重试归并', '停用批次']
const statuses = ['待归并', '归并中', '已归并', '空批次', '归并失败', '已中断', '已停用']

type Row = Record<string, string | number | string[] | null>

type PagePayload = { items: Row[]; total: number }
type ListPayload = { items: Row[]; total: number; drift?: number }

export default function DedupView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [dataset, setDataset] = useState<Row[]>([])
  const [groups, setGroups] = useState<Row[]>([])
  const [registry, setRegistry] = useState<Row[]>([])
  const [contractItems, setContractItems] = useState<Row[]>([])
  const [contractDrift, setContractDrift] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [reviewKey, setReviewKey] = useState('')
  const [reviewResult, setReviewResult] = useState<Row | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [createValues, setCreateValues] = useState<Record<string, string>>({
    批次编号: '',
    关联数据集: '客户主档',
    去重字段: 'customer_id',
    规则版本: 'v2',
    迟到边界: '',
    故障注入: '无',
  })
  const filterFields = useMemo(() => ['批次编号'], [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const [page, datasetPayload, groupPayload, registryPayload, contractPayload] = await Promise.all([
        fetchJson<PagePayload>(`${ENDPOINT}?${query}`),
        fetchJson<ListPayload>(`${ENDPOINT}/dataset/current`),
        fetchJson<ListPayload>(`${ENDPOINT}/groups`),
        fetchJson<ListPayload>(`${ENDPOINT}/registry`),
        fetchJson<ListPayload>(`${ENDPOINT}/contract/check`),
      ])
      setRows(page.items ?? [])
      setTotal(page.total ?? 0)
      setDataset(datasetPayload.items ?? [])
      setGroups(groupPayload.items ?? [])
      setRegistry(registryPayload.items ?? [])
      setContractItems(contractPayload.items ?? [])
      setContractDrift(contractPayload.drift ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '去重归并数据读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action } }),
      })
      const payload = await response.json()
      if (!payload.ok) {
        throw new Error(payload.message || '去重归并动作未生效')
      }
      setNotice(payload.message || '归并动作已执行')
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '去重归并操作失败')
    }
  }

  async function createRun() {
    setErrorMessage('')
    try {
      const response = await request(ENDPOINT, {
        method: 'POST',
        body: JSON.stringify({ values: createValues }),
      })
      const payload = await response.json()
      if (!payload.ok) {
        throw new Error(payload.message || '归并批次登记失败')
      }
      setNotice(payload.message || '归并批次已登记')
      setShowCreate(false)
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '归并批次登记失败')
    }
  }

  async function reviewGroup() {
    setErrorMessage('')
    setReviewResult(null)
    if (!reviewKey.trim()) {
      setErrorMessage('请输入要复核的去重键')
      return
    }
    try {
      const response = await request(`${ENDPOINT}/review?key=${encodeURIComponent(reviewKey.trim())}`)
      if (!response.ok) {
        throw new Error(`去重键 ${reviewKey} 不在清洗后的数据集中`)
      }
      setReviewResult((await response.json()) as Row)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '复核查询失败')
    }
  }

  const stats = [
    { label: '归并批次总数', value: total },
    { label: '当前重复组', value: dataset.length },
    { label: '待归并批次', value: rows.filter((row) => row.status === '待归并').length },
    { label: '契约漂移规则', value: contractDrift },
  ]

  return (
    <section className="page" data-module="dedup">
      <header className="page-head">
        <div>
          <h2>去重归并管理</h2>
          <p className="page-desc">
            围绕清洗后的数据集与迟到重复源记录做增量归并：按去重字段与规则版本仲裁，幂等写入，复核以清洗后的数据集为准。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setShowCreate((prev) => !prev)}>
            登记归并批次
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出去重归并清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      {showCreate && (
        <form
          className="filter-bar"
          onSubmit={(event) => {
            event.preventDefault()
            void createRun()
          }}
        >
          {Object.keys(createValues).map((field) => (
            <label key={field} className="filter-item">
              <span>{field}</span>
              <input
                value={createValues[field] ?? ''}
                placeholder={`填写${field}`}
                onChange={(event) => setCreateValues((prev) => ({ ...prev, [field]: event.target.value }))}
              />
            </label>
          ))}
          <button className="btn primary" type="submit">提交登记</button>
        </form>
      )}

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void reload()
        }}
      >
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <label className="filter-item">
          <span>状态</span>
          <select
            value={filters.status ?? ''}
            onChange={(event) => setFilters((prev) => ({ ...prev, status: event.target.value }))}
          >
            <option value="">全部状态</option>
            {statuses.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
        </label>
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>状态</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td>{row.status ?? '—'}</td>
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无去重归并数据，可先登记归并批次</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条去重归并记录</span>
        {notice && <span>{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>

      <h3>清洗后的数据集（查询只读取归并后的当前重复组）</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>去重键</th>
            <th>保留记录</th>
            <th>被归并行号</th>
            <th>规则版本</th>
            <th>归并批次</th>
          </tr>
        </thead>
        <tbody>
          {dataset.map((row) => (
            <tr key={String(row.去重键)}>
              <td>{row.去重键}</td>
              <td>{row.保留记录}</td>
              <td>{Array.isArray(row.被归并行号) && row.被归并行号.length > 0 ? row.被归并行号.join('、') : '—'}</td>
              <td>{row.规则版本}</td>
              <td>{row.归并批次}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h3>重复组台账（只随受影响重复组更新）</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>去重键</th>
            <th>版本历史</th>
            <th>重算次数</th>
            <th>最近归并批次</th>
            <th>影响来源批次</th>
            <th>状态</th>
          </tr>
        </thead>
        <tbody>
          {groups.map((row) => (
            <tr key={String(row.去重键)}>
              <td>{row.去重键}</td>
              <td>{Array.isArray(row.版本历史) ? row.版本历史.join(' → ') : '—'}</td>
              <td>{row.重算次数}</td>
              <td>{row.最近归并批次}</td>
              <td>{Array.isArray(row.影响来源批次) ? row.影响来源批次.join('、') : '—'}</td>
              <td>{row.状态}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h3>去重字段登记（清洗后的数据集同步到这里）</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>去重字段</th>
            <th>当前规则版本</th>
            <th>保留策略</th>
            <th>迟到边界</th>
            <th>当前重复组</th>
            <th>快照版本</th>
            <th>契约状态</th>
          </tr>
        </thead>
        <tbody>
          {registry.map((row) => (
            <tr key={String(row.去重字段)}>
              <td>{row.去重字段}</td>
              <td>{row.当前规则版本}</td>
              <td>{row.保留策略}</td>
              <td>{row.迟到边界}</td>
              <td>{row.当前重复组}</td>
              <td>{row.快照版本}</td>
              <td>{row.契约状态}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h3>清洗规则契约核对（其他业务模块要与主结果一致）</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>规则编号</th>
            <th>去重字段</th>
            <th>规则版本</th>
            <th>核对结果</th>
          </tr>
        </thead>
        <tbody>
          {contractItems.map((row) => (
            <tr key={String(row.规则编号)}>
              <td>{row.规则编号}</td>
              <td>{row.去重字段}</td>
              <td>{row.规则版本}</td>
              <td>{row.核对结果}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h3>复核查询（以清洗后的数据集为准）</h3>
      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void reviewGroup()
        }}
      >
        <label className="filter-item">
          <span>去重键</span>
          <input
            value={reviewKey}
            placeholder="如 C004"
            onChange={(event) => setReviewKey(event.target.value)}
          />
        </label>
        <button className="btn" type="submit">复核</button>
      </form>
      {reviewResult && (
        <table className="data-table">
          <tbody>
            {Object.entries(reviewResult).map(([key, value]) => (
              <tr key={key}>
                <th>{key}</th>
                <td>{Array.isArray(value) ? value.join('、') : String(value ?? '—')}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
