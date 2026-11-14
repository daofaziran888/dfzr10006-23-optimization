import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/transform'
const columns = [
  "规则编号",
  "输入格式",
  "输出格式",
  "映射表达式",
  "字段过滤",
  "编码设置",
  "转换日志",
  "规则状态"
]
const actions = [
  "启用规则",
  "暂停规则",
  "关闭规则"
]
const RULE_FIELDS = ["规则编号", "输入格式", "输出格式", "映射表达式", "字段过滤", "编码设置"]

type Row = Record<string, unknown> & { id?: number }

type ImportDetail = {
  规则编号: string
  类别: string
  解析版本?: string
  解析来源?: string
  输出格式?: string
  被裁剪字段?: string[]
  缺失字段?: string[]
  错误?: string[]
  转换日志?: string[]
  试运行?: {
    输出格式: { 格式: string; 字段: string[] }
    被裁剪字段: string[]
    转换日志: string[]
    书签: string
    导入版本: string
  }
}

type ImportResult = {
  ok: boolean
  批次?: { 批次号: string; 总数: number; 新增: number; 变更: number; 未变化: number; 重复: number; 失败: number; 状态: string }
  明细?: ImportDetail[]
  message?: string
}

type ResultRow = {
  规则编号: string
  书签: string
  导入版本: string
  解析版本: string
  状态: string
  当前版本: boolean
  下载路径: string | null
  创建时间: string
}

function cellText(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  if (Array.isArray(value)) return value.length ? value.join('；') : '—'
  return String(value)
}

export default function TransformView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const [importText, setImportText] = useState('')
  const [importResult, setImportResult] = useState<ImportResult | null>(null)
  const [results, setResults] = useState<ResultRow[]>([])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('加工规则列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据加工转换列表读取失败')
    }
  }, [filters])

  const reloadResults = useCallback(async () => {
    try {
      const response = await request(`${ENDPOINT}/results`)
      if (response.ok) {
        const payload = await response.json()
        setResults(payload.items ?? [])
      }
    } catch {
      // 加工结果读取失败不打断主列表
    }
  }, [])

  useEffect(() => {
    void reload()
    void reloadResults()
  }, [reload, reloadResults])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('数据加工转换动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据加工转换操作失败')
    }
  }

  async function fillRegressionBatch() {
    // 以 TRAN-0001 至 TRAN-0003 的当前规则内容作为导入回归批次
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}?size=200`)
      if (!response.ok) throw new Error('规则清单读取失败')
      const payload = await response.json()
      const batch = (payload.items ?? [])
        .filter((item: Row) => typeof item.规则编号 === 'string')
        .map((item: Row) => Object.fromEntries(RULE_FIELDS.map((field) => [field, item[field] ?? ''])))
      setImportText(JSON.stringify(batch, null, 2))
      setNotice(`已装入 ${batch.length} 条规则作为导入批次，可直接提交回归`)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '回归批次装配失败')
    }
  }

  async function submitImport() {
    setErrorMessage('')
    setNotice('')
    let rules: unknown
    try {
      rules = JSON.parse(importText || '[]')
    } catch {
      setErrorMessage('导入内容不是合法 JSON，请检查后再提交')
      return
    }
    if (!Array.isArray(rules) || rules.length === 0) {
      setErrorMessage('导入内容应为非空的规则数组')
      return
    }
    try {
      const response = await request(`${ENDPOINT}/import`, {
        method: 'POST',
        body: JSON.stringify({ rules, auto_dry_run: true }),
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.detail ?? '批量导入失败')
      setImportResult(payload)
      const batch = payload.批次
      setNotice(batch ? `批次 ${batch.批次号}：新增 ${batch.新增}、变更 ${batch.变更}、未变化 ${batch.未变化}、重复 ${batch.重复}、失败 ${batch.失败}` : '')
      await reload()
      await reloadResults()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '批量导入失败')
    }
  }

  async function resultAction(path: string, okMessage: string) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}${path}`, { method: 'POST', body: JSON.stringify({}) })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) throw new Error(payload.message ?? payload.detail ?? '操作失败')
      setNotice(payload.message ?? okMessage)
      await reload()
      await reloadResults()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : okMessage)
    }
  }

  return (
    <section className="page" data-module="transform">
      <header className="page-head">
        <div>
          <h2>数据加工转换管理</h2>
          <p className="page-desc">维护数据加工的格式映射、字段过滤与编码配置，为数据加工转换提供可验证的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('加工规则登记入口尚未接入审批流')}>
            登记加工规则
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出数据加工转换清单
          </button>
        </div>
      </header>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
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
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{cellText(row[column])}</td>)}
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
              <td className="empty-state" colSpan={columns.length + 1}>暂无数据加工转换数据，可先登记加工规则</td>
            </tr>
          )}
        </tbody>
      </table>

      <section className="page" style={{ paddingTop: 0 }}>
        <header className="page-head">
          <div>
            <h3>批量导入与试运行</h3>
            <p className="page-desc">
              已成功解析的加工规则直接复用缓存指纹，只解析新增或变更项；试运行输出输出格式、被裁剪字段和转换日志，失败标明规则编号和缺失字段。
            </p>
          </div>
          <div className="page-actions">
            <button className="btn" type="button" onClick={() => void fillRegressionBatch()}>
              装入 TRAN-0001~0003 回归批次
            </button>
            <button className="btn primary" type="button" onClick={() => void submitImport()}>
              批量导入并试运行
            </button>
          </div>
        </header>
        <textarea
          className="import-editor"
          rows={8}
          placeholder='[{"规则编号":"TRAN-0001","输入格式":"CSV:f1,f2","输出格式":"JSON:f1","映射表达式":"f1=f1","字段过滤":"f2","编码设置":"UTF-8"}]'
          value={importText}
          onChange={(event) => setImportText(event.target.value)}
        />
        {importResult?.明细 && (
          <table className="data-table">
            <thead>
              <tr>
                <th>规则编号</th><th>类别</th><th>解析版本</th><th>解析来源</th>
                <th>输出格式</th><th>被裁剪字段</th><th>缺失字段</th><th>转换日志</th>
              </tr>
            </thead>
            <tbody>
              {importResult.明细.map((detail, index) => (
                <tr key={`${detail.规则编号}-${index}`}>
                  <td>{detail.规则编号}</td>
                  <td>{detail.类别}</td>
                  <td>{detail.解析版本 ?? '—'}</td>
                  <td>{detail.解析来源 ?? '—'}</td>
                  <td>{detail.试运行 ? `${detail.试运行.输出格式.格式}(${detail.试运行.输出格式.字段.join(', ')})` : cellText(detail.输出格式)}</td>
                  <td>{cellText(detail.试运行?.被裁剪字段 ?? detail.被裁剪字段)}</td>
                  <td>{cellText(detail.缺失字段)}</td>
                  <td>{cellText(detail.转换日志 ?? detail.试运行?.转换日志)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section className="page" style={{ paddingTop: 0 }}>
        <header className="page-head">
          <div>
            <h3>加工结果</h3>
            <p className="page-desc">
              加工结果带书签并保留导入版本；核对后才能下载与同步订阅推送，回滚可恢复上一版加工结果。
            </p>
          </div>
        </header>
        <table className="data-table">
          <thead>
            <tr>
              <th>书签</th><th>规则编号</th><th>导入版本</th><th>解析版本</th>
              <th>状态</th><th>当前版本</th><th>下载路径</th><th>可执行动作</th>
            </tr>
          </thead>
          <tbody>
            {results.map((item) => (
              <tr key={item.书签}>
                <td>{item.书签}</td>
                <td>{item.规则编号}</td>
                <td>{item.导入版本}</td>
                <td>{item.解析版本}</td>
                <td>{item.状态}</td>
                <td>{item.当前版本 ? '是' : '否'}</td>
                <td>
                  {item.下载路径
                    ? <a href={item.下载路径} target="_blank" rel="noreferrer">{item.下载路径}</a>
                    : '核对后可见'}
                </td>
                <td className="row-actions">
                  <button className="link" type="button" onClick={() => void resultAction(`/results/${item.书签}/verify`, '核对失败')}>核对</button>
                  <button className="link" type="button" onClick={() => void resultAction(`/results/${item.书签}/sync-subscribe`, '同步订阅失败')}>同步订阅</button>
                  <button className="link" type="button" onClick={() => void resultAction(`/rollback/${item.规则编号}`, '回滚失败')}>回滚</button>
                </td>
              </tr>
            ))}
            {results.length === 0 && (
              <tr><td className="empty-state" colSpan={8}>暂无加工结果，先批量导入并试运行</td></tr>
            )}
          </tbody>
        </table>
      </section>

      <footer className="page-foot">
        <span>共 {total} 条数据加工转换记录</span>
        {notice && <span>{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
