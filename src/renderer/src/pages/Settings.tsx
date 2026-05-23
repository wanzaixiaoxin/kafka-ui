import { useState, useEffect } from 'react'
import { Card, Form, InputNumber, Select, Button, Divider, Typography, message, Space } from 'antd'
import { SettingOutlined, InfoCircleOutlined } from '@ant-design/icons'
import { loadSettings, saveSettings, setCachedSettings, type AppSettings } from '../utils/settings'

const { Text, Paragraph } = Typography

const DEFAULTS: AppSettings = {
  maxMessages: 500,
  autoRefreshInterval: 0,
  theme: 'light'
}

/** 设置页面 - 应用配置和偏好设置 */
export default function Settings(): JSX.Element {
  const [form] = Form.useForm()
  const [settings, setSettings] = useState<AppSettings>(DEFAULTS)

  /** 挂载时从主进程加载设置 */
  useEffect(() => {
    loadSettings().then((s) => {
      setSettings(s)
      setCachedSettings(s)
      form.setFieldsValue(s)
    })
  }, [form])

  /** 保存设置 */
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

  /** 重置为默认设置 */
  const onReset = async (): Promise<void> => {
    form.setFieldsValue(DEFAULTS)
    const next = await saveSettings(DEFAULTS)
    setSettings(next)
    setCachedSettings(next)
    message.info('已恢复默认设置')
  }

  return (
    <div>
      {/* 应用信息 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space>
          <InfoCircleOutlined style={{ fontSize: 20, color: '#1677ff' }} />
          <div>
            <Text strong style={{ fontSize: 16 }}>Kafka Client v1.0.0</Text>
            <Paragraph type="secondary" style={{ margin: 0, fontSize: 12 }}>
              基于 Electron + React + TypeScript 构建的 Kafka 桌面客户端
            </Paragraph>
          </div>
        </Space>
      </Card>

      {/* 设置表单 */}
      <Card
        title={
          <Space>
            <SettingOutlined />
            <span>偏好设置</span>
          </Space>
        }
        size="small"
      >
        <Form
          form={form}
          layout="vertical"
          style={{ maxWidth: 480 }}
        >
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
            tooltip="设置为 0 表示禁用自动刷新"
          >
            <InputNumber min={0} max={3600} step={5} style={{ width: '100%' }} />
          </Form.Item>

          <Form.Item
            name="theme"
            label="主题"
          >
            <Select>
              <Select.Option value="light">浅色</Select.Option>
              <Select.Option value="dark">深色</Select.Option>
              <Select.Option value="system">跟随系统</Select.Option>
            </Select>
          </Form.Item>
        </Form>

        <Divider style={{ margin: '8px 0 16px' }} />

        <Space>
          <Button type="primary" onClick={onSave}>保存设置</Button>
          <Button onClick={onReset}>恢复默认</Button>
        </Space>

        <Divider style={{ margin: '16px 0 12px' }} />
        <Text type="secondary" style={{ fontSize: 12 }}>
          当前主题: {settings.theme === 'light' ? '浅色' : settings.theme === 'dark' ? '深色' : '跟随系统'}
          {' | '}
          消息保留: {settings.maxMessages} 条
          {' | '}
          自动刷新: {settings.autoRefreshInterval > 0 ? `${settings.autoRefreshInterval}s` : '已禁用'}
        </Text>
      </Card>
    </div>
  )
}
