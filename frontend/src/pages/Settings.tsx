import { useState, useEffect } from 'react'
import { Card, Form, InputNumber, Select, Button, Typography, message, Space, Row, Col, Divider } from 'antd'
import { SettingOutlined, InfoCircleOutlined } from '@ant-design/icons'
import { loadSettings, saveSettings, setCachedSettings, DEFAULT_SETTINGS, type AppSettings } from '../utils/settings'

const { Text, Paragraph } = Typography

/** 设置页面 */
export default function Settings(): JSX.Element {
  const [form] = Form.useForm()
  const [settings, setSettings] = useState<AppSettings>(DEFAULT_SETTINGS)

  useEffect(() => {
    loadSettings().then((s) => {
      setSettings(s)
      setCachedSettings(s)
      form.setFieldsValue(s)
    })
  }, [form])

  const onSave = async (): Promise<void> => {
    try {
      const vals = form.getFieldsValue()
      const next = await saveSettings(vals)
      setSettings(next)
      setCachedSettings(next)
      message.success('设置已保存')
    } catch {
      message.error('保存设置失败')
    }
  }

  const onReset = async (): Promise<void> => {
    form.setFieldsValue(DEFAULT_SETTINGS)
    const next = await saveSettings(DEFAULT_SETTINGS)
    setSettings(next)
    setCachedSettings(next)
    message.info('已恢复默认设置')
  }

  return (
    <Card
      title={
        <Space>
          <SettingOutlined />
          <span>偏好设置</span>
        </Space>
      }
      size="small"
    >
      <Row gutter={24} align="bottom">
        <Col flex="auto">
          <Form form={form} layout="horizontal" labelCol={{ flex: '0 0 170px' }}>
            <Form.Item
              name="maxMessages"
              label="消息最大保留数量"
              tooltip="消费页面中保留的最大消息条数"
            >
              <InputNumber min={100} max={10000} step={100} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item
              name="autoRefreshInterval"
              label="自动刷新间隔（秒）"
              tooltip="0 表示禁用"
            >
              <InputNumber min={0} max={3600} step={5} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="theme" label="主题">
              <Select>
                <Select.Option value="light">浅色</Select.Option>
                <Select.Option value="dark">深色</Select.Option>
                <Select.Option value="system">跟随系统</Select.Option>
              </Select>
            </Form.Item>
          </Form>
        </Col>

        <Col flex="220px" style={{ borderLeft: '1px solid #f0f0f0', paddingLeft: 24 }}>
          <Space size={8}>
            <InfoCircleOutlined style={{ color: '#1677ff' }} />
            <div>
              <Text strong>Kafka Client v1.0.0</Text>
              <Paragraph type="secondary" style={{ margin: 0, fontSize: 12 }}>
                Tauri + React + TypeScript
              </Paragraph>
            </div>
          </Space>
          <Divider style={{ margin: '10px 0' }} />
          <Space direction="vertical" size={2}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              主题: {settings.theme === 'light' ? '浅色' : settings.theme === 'dark' ? '深色' : '跟随系统'}
            </Text>
            <Text type="secondary" style={{ fontSize: 12 }}>
              消息保留: {settings.maxMessages} 条
            </Text>
            <Text type="secondary" style={{ fontSize: 12 }}>
              自动刷新: {settings.autoRefreshInterval > 0 ? `${settings.autoRefreshInterval}s` : '已禁用'}
            </Text>
          </Space>
          <Divider style={{ margin: '10px 0' }} />
          <Space size={12}>
            <Button type="primary" onClick={onSave}>保存</Button>
            <Button onClick={onReset}>恢复默认</Button>
          </Space>
        </Col>
      </Row>
    </Card>
  )
}
