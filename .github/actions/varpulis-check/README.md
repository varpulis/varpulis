# Varpulis VPL Check Action

A GitHub Action to validate VPL (Varpulis Pipeline Language) files in your repository. Reports parse and semantic errors as GitHub annotations.

## Usage

```yaml
name: VPL Validation

on: [push, pull_request]

jobs:
  check-vpl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: varpulis/varpulis/.github/actions/varpulis-check@main
        with:
          path: 'pipelines/'    # optional, defaults to '**/*.vpl'
          version: '0.4.0'      # optional, defaults to latest release
          strict: 'false'       # optional, treat warnings as errors
```

## Inputs

| Input     | Description                                  | Required | Default      |
|-----------|----------------------------------------------|----------|--------------|
| `path`    | Glob pattern or directory for VPL files      | No       | `**/*.vpl`   |
| `version` | Varpulis CLI version to use                  | No       | `latest`     |
| `strict`  | Treat warnings as errors                     | No       | `false`      |

## Outputs

Errors and warnings are reported as GitHub annotations, appearing inline on pull request diffs.

## Examples

### Check all VPL files in the repo

```yaml
- uses: varpulis/varpulis/.github/actions/varpulis-check@main
```

### Check specific directory with strict mode

```yaml
- uses: varpulis/varpulis/.github/actions/varpulis-check@main
  with:
    path: 'src/pipelines/'
    strict: 'true'
```

### Pin to a specific version

```yaml
- uses: varpulis/varpulis/.github/actions/varpulis-check@main
  with:
    version: '0.4.0'
```

## Supported Platforms

- Linux x86_64
- Linux ARM64
- macOS x86_64
- macOS ARM64 (Apple Silicon)
