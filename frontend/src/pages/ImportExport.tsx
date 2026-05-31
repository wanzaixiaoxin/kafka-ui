import { useState, useEffect, useCallback } from 'react'
import {
  Card, Tabs, Form, InputNumber, Button, Typography, Alert, Spin,
  Radio, Input, Progress, Space, Statistic, Row, Col, AutoComplete
} from 'antd'
import {
  ExportOutlined, ImportOutlined, FolderOpenOutlined
} from '@ant-design/icons'
import { save, open } from '@tauri-apps/plugin-dialog'
import { kafkaApiClient } from '../services/kafkaApiClient'
import { useActiveConnection } from '../hooks/useActiveConnection'
import type {
  ExportOptions, ImportOptions, ImportExportProgress, TopicInfo
} from '../types/kafka'

const { Title } = Typography

/** 导入导出页面 */
export default function ImportExport(): JSX.Element {
  const { hasConn } = useActiveConnection()

  if (!hasConn) {
    return (
      <Card title="消息导入导出" size="small">
        <Alert message="请先选择一个 Kafka 连接" type="warning" showIcon />
      </Card>
    )
  }

  return (
    <div>
      <Title level={4} style={{ marginTop: 0, marginBottom: 12 }}>消息导入导出</Title>
      <Tabs
        defaultActiveKey="export"
        items={[
          { key: 'export', label: <><ExportOutlined /> 导出消息</>, children: <ExportPanel /> },
          { key: 'import', label: <><ImportOutlined /> 导入消息</>, children: <ImportPanel /> },
        ]}
      />
    </div>
  )
}

/** 导出面板 */
function ExportPanel(): JSX.Element {
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [progress, setProgress] = useState<ImportExportProgress | null>(null)
  const [result, setResult] = useState<{ success: boolean; message: string } | null>(null)

  const [form] = Form.useForm()

  useEffect(() => {
    kafkaApiClient.topics.list().then(res => {
      if (!('error' in res)) setTopics(res as TopicInfo[])
    })
  }, [])

  const handleExport = useCallback(async () => {
    const values = await form.validateFields().catch(() => null)
    if (!values) return

    setLoading(true)
    setProgress(null)
    setResult(null)

    const opts: ExportOptions = {
      topic: values.topic,
      partition: values.partition,
      offsetStart: values.offsetStart != null ? String(values.offsetStart) : undefined,
      offsetEnd: values.offsetEnd != null ? String(values.offsetEnd) : undefined,
      maxCount: values.maxCount ?? 1000,
      format: values.format ?? 'jsonl',
      savePath: values.savePath ?? ''
    }

    const unsub = kafkaApiClient.importExport.onExportProgress((p) => {
      setProgress(p)
      if (p.status === 'completed') {
        setResult({ success: true, message: `成功导出 ${p.current} 条消息` })
        setLoading(false)
        unsub()
      } else if (p.status === 'error') {
        setResult({ success: false, message: p.error ?? '导出失败' })
        setLoading(false)
        unsub()
      }
    })

    try {
      await kafkaApiClient.importExport.exportStart(opts)
    } catch (err: unknown) {
      setLoading(false)
      setResult({ success: false, message: err instanceof Error ? err.message : '导出失败' })
      unsub()
    }
  }, [form])

  const handleSelectPath = async () => {
    const topic: string = form.getFieldValue('topic') || 'export'
    const fmt: string = form.getFieldValue('format') || 'jsonl'
    const path = await save({
      defaultPath: `${topic}_export.${fmt === 'csv' ? 'csv' : 'jsonl'}`,
      filters: [
        { name: 'JSON Lines', extensions: ['jsonl'] },
        { name: 'CSV', extensions: ['csv'] }
      ]
    })
    if (path) form.setFieldValue('savePath', path)
  }

  const topicOptions = topics.map(t => ({ value: t.topic, label: t.topic }))

  return (
    <Spin spinning={loading && !progress}>
      <Card size="small">
        <Form form={form} layout="horizontal" labelCol={{ flex: '0 0 90px' }} initialValues={{ maxCount: 1000, format: 'jsonl' }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item label="Topic" name="topic" rules={[{ required: true, message: '请选择' }]}>
                <AutoComplete options={topicOptions} placeholder="输入或选择 Topic" />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item label="分区" name="partition">
                <InputNumber style={{ width: '100%' }} min={0} placeholder="全部" />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item label="最大条数" name="maxCount">
                <InputNumber style={{ width: '100%' }} min={1} max={100000} />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={6}>
              <Form.Item label="起始 Offset" name="offsetStart">
                <InputNumber style={{ width: '100%' }} min={0} placeholder="最早" />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item label="结束 Offset" name="offsetEnd">
                <InputNumber style={{ width: '100%' }} min={0} placeholder="最新" />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item label="格式" name="format">
                <Radio.Group>
                  <Radio value="jsonl">JSONL</Radio>
                  <Radio value="csv">CSV</Radio>
                </Radio.Group>
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item label=" " colon={false}>
                <Button type="primary" icon={<ExportOutlined />} onClick={handleExport} loading={loading && !progress} block>
                  {loading ? '导出中...' : '开始导出'}
                </Button>
              </Form.Item>
            </Col>
          </Row>
          <Form.Item label="保存到" name="savePath" rules={[{ required: true, message: '请选择保存路径' }]}>
            <Space.Compact style={{ width: '100%' }}>
              <Input placeholder="点击右侧按钮选择保存路径" readOnly />
              <Button icon={<FolderOpenOutlined />} onClick={handleSelectPath}>选择路径</Button>
            </Space.Compact>
          </Form.Item>
        </Form>

        {progress && (
          <div style={{ marginTop: 8 }}>
            <Progress percent={Math.round(progress.percent)} size="small"
              status={progress.status === 'error' ? 'exception' : progress.status === 'completed' ? 'success' : 'active'} />
          </div>
        )}

        {result && (
          <Alert message={result.message} type={result.success ? 'success' : 'error'} showIcon closable style={{ marginTop: 8 }} />
        )}
      </Card>
    </Spin>
  )
}

/** 导入面板 */
function ImportPanel(): JSX.Element {
  const [loading, setLoading] = useState(false)
  const [progress, setProgress] = useState<ImportExportProgress | null>(null)
  const [result, setResult] = useState<{ success: boolean; message: string } | null>(null)
  const [topics, setTopics] = useState<TopicInfo[]>([])

  const [form] = Form.useForm()

  useEffect(() => {
    kafkaApiClient.topics.list().then(res => {
      if (!('error' in res)) setTopics(res as TopicInfo[])
    })
  }, [])

  const handleImport = useCallback(async () => {
    const values = await form.validateFields().catch(() => null)
    if (!values) return

    setLoading(true)
    setProgress(null)
    setResult(null)

    const opts: ImportOptions = {
      topic: values.topic,
      filePath: values.filePath ?? '',
      format: values.format ?? 'jsonl',
      keyField: values.keyField || undefined
    }

    const unsub = kafkaApiClient.importExport.onImportProgress((p) => {
      setProgress(p)
      if (p.status === 'completed') {
        const msg = `导入完成: 总计 ${p.current} 行, 成功 ${p.successCount ?? 0}, 失败 ${p.errorCount ?? 0}`
        setResult({ success: p.errorCount === 0, message: msg })
        setLoading(false)
        unsub()
      } else if (p.status === 'error') {
        setResult({ success: false, message: p.error ?? '导入失败' })
        setLoading(false)
        unsub()
      }
    })

    try {
      await kafkaApiClient.importExport.importStart(opts)
    } catch (err: unknown) {
      setLoading(false)
      setResult({ success: false, message: err instanceof Error ? err.message : '导入失败' })
      unsub()
    }
  }, [form])

  const handleSelectFile = async () => {
    const path = await open({
      multiple: false,
      filters: [
        { name: 'JSON Lines / CSV', extensions: ['jsonl', 'csv'] }
      ]
    })
    if (path) {
      form.setFieldValue('filePath', path)
      if (path.endsWith('.csv')) form.setFieldValue('format', 'csv')
      else form.setFieldValue('format', 'jsonl')
    }
  }

  const topicOptions = topics.map(t => ({ value: t.topic, label: t.topic }))

  return (
    <Spin spinning={loading && !progress}>
      <Card size="small">
        <Form form={form} layout="horizontal" labelCol={{ flex: '0 0 80px' }} initialValues={{ format: 'jsonl' }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item label="目标 Topic" name="topic" rules={[{ required: true, message: '请选择' }]}>
                <AutoComplete options={topicOptions} placeholder="输入或选择 Topic" />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item label="格式" name="format">
                <Radio.Group>
                  <Radio value="jsonl">JSONL</Radio>
                  <Radio value="csv">CSV</Radio>
                </Radio.Group>
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item label="Key 列" name="keyField">
                <Input placeholder="CSV 用" />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item label="源文件" name="filePath" rules={[{ required: true, message: '请选择文件' }]}>
            <Space.Compact style={{ width: '100%' }}>
              <Input placeholder="点击右侧按钮选择文件" readOnly />
              <Button icon={<FolderOpenOutlined />} onClick={handleSelectFile}>选择文件</Button>
            </Space.Compact>
          </Form.Item>
          <Form.Item label=" " colon={false}>
            <Button type="primary" icon={<ImportOutlined />} onClick={handleImport} loading={loading && !progress}>
              {loading ? '导入中...' : '开始导入'}
            </Button>
          </Form.Item>
        </Form>

        {progress && (
          <div style={{ marginTop: 8 }}>
            <Progress percent={Math.round(progress.percent)} size="small"
              status={progress.status === 'error' ? 'exception' : progress.status === 'completed' ? 'success' : 'active'} />
            <Row gutter={16} style={{ marginTop: 4, textAlign: 'center' }}>
              <Col span={8}>
                <Statistic title="已处理" value={progress.current} suffix={`/ ${progress.total}`} />
              </Col>
              <Col span={8}>
                <Statistic title="成功" value={progress.successCount ?? 0} valueStyle={{ color: '#3f8600' }} />
              </Col>
              <Col span={8}>
                <Statistic title="失败" value={progress.errorCount ?? 0} valueStyle={{ color: progress.errorCount ? '#cf1322' : undefined }} />
              </Col>
            </Row>
          </div>
        )}

        {result && (
          <Alert message={result.message} type={result.success ? 'success' : 'error'} showIcon closable style={{ marginTop: 8 }} />
        )}
      </Card>
    </Spin>
  )
}
