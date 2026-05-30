import { Component, type ReactNode } from 'react'
import { Button, Result } from 'antd'

interface Props {
  children: ReactNode
}

interface State {
  hasError: boolean
  error: Error | null
}

/** 全局错误边界组件 - 捕获渲染异常并展示友好错误页面 */
export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  /** 捕获子组件抛出的错误 */
  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  /** 重新加载页面 */
  reload = (): void => {
    this.setState({ hasError: false, error: null })
    window.location.reload()
  }

  render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
          <Result
            status="error"
            title="应用出现异常"
            subTitle={this.state.error?.message || '发生了未知错误'}
            extra={[
              <Button type="primary" key="reload" onClick={this.reload}>
                重新加载
              </Button>
            ]}
          />
        </div>
      )
    }
    return this.props.children
  }
}
