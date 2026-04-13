import { Component, type ReactNode, type ErrorInfo } from 'react'
import { AlertCircle, RotateCcw } from 'lucide-react'
import { Button } from '@/components/ui/button'

interface Props {
  children: ReactNode
}

interface State {
  hasError: boolean
  error: Error | null
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('[ErrorBoundary]', error, info.componentStack)
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null })
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex flex-col items-center justify-center h-screen w-screen bg-background text-foreground gap-4 p-8">
          <AlertCircle className="h-10 w-10 text-destructive" />
          <h2 className="text-lg font-semibold">Something went wrong</h2>
          <p className="text-sm text-muted-foreground max-w-md text-center">
            {this.state.error?.message || 'An unexpected error occurred in the Stream Builder.'}
          </p>
          <div className="flex gap-2">
            <Button size="sm" variant="outline" onClick={this.handleReset}>
              <RotateCcw className="h-3.5 w-3.5 mr-1.5" />
              Try Again
            </Button>
            <Button size="sm" onClick={() => window.location.reload()}>
              Reload Page
            </Button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}
