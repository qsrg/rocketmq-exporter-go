
# configuration Specification

## Purpose
CLI flag + 环境变量配置替代 Spring `application.yml`，保留 Java 语义调参项，内置现代化默认值，启动时校验 cron 表达式并快速失败。

## Requirements

### Requirement: flag 与环境变量配置
系统 SHALL 从 CLI flag 加载配置，以环境变量为回退，替代 Spring `application.yml`。系统 SHALL 保留 Java 的语义调参项（namesrv、enable-collect、ACL、缓存 TTL、六个 cron 表达式、worker pool 规模、监听地址、telemetry 路径），但可自由重命名 key。

#### Scenario: flag 值优先
- **WHEN** 传入 `--namesrv=10.0.0.1:9876` 且 `NAMESRV_ADDR` 设为另一值
- **THEN** exporter 连接 `10.0.0.1:9876`。

#### Scenario: flag 未设时 env 回退
- **WHEN** 未传 `--namesrv` 且 `NAMESRV_ADDR=10.0.0.2:9876`
- **THEN** exporter 连接 `10.0.0.2:9876`。

### Requirement: 现代化默认值
系统 SHALL 内置默认值：监听 `:5557`、telemetry 路径 `/metrics`、namesrv `127.0.0.1:9876`、`enable-collect=true`、`enable-acl=false`、`cache-ttl=60s`、六个 cron 为 `15 0/1 * * * *`、pool core/max `10`、pool queue `5000`。HTTP 端口由 Java 的 `19876` 改为 `:5557` SHALL 在 change 中记录。

#### Scenario: 无 flag 无 env 时默认值合理
- **WHEN** 二进制无 flag 无 env 启动
- **THEN** 绑定 `:5557`，暴露 `/metrics`，缓存 60s，调度六个 `15 0/1 * * * *` 任务。

### Requirement: cron 表达式校验
系统 SHALL 在加载时校验每个配置的 cron 表达式；非法表达式 SHALL 在启动时快速失败并指明出错任务，而非静默跳过采集。

#### Scenario: 非法 cron 中止启动
- **WHEN** `cron.collectConsumerOffset` 设为非法表达式
- **THEN** 启动失败并报错指明 `collectConsumerOffset`。
