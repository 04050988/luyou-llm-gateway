# luyou · 统一 LLM API 网关

本地运行的 LLM API 网关（Python + FastAPI）：一个地址 + 一个 key，统一调用多家大模型平台。内置**配额感知调度、熔断降级、跨平台故障切换、动态模型发现、SSE 流式桥接**，带监控面板与每日用量台账。

> 为什么叫 luyou：路由（routing）是这个网关的核心问题——模型到平台、请求到 key、故障到备用链。

```
客户端 (Cherry Studio / Cline / curl)
        │  OpenAI 兼容协议 + master_key
        ▼
┌─────────────────────────────────────────────────┐
│                  luyou gateway                   │
│                                                  │
│  预检: 别名改写 → 三级路由解析 → 容量预判          │
│    │                                             │
│    ▼                                             │
│  调度器: 余量得分 × 历史成功率 × 连击衰减          │
│    │            │                                │
│    ▼            ▼                                │
│  熔断器       SSE 桥接(背压/断连取消/usage回传)    │
│  冷却→半开→恢复                                   │
│    │                                             │
│    ▼  全冷却时沿 fallback_chain 跨平台切换         │
└──────┬──────────┬──────────┬─────────────────────┘
       ▼          ▼          ▼
   商汤日日新   SiliconFlow   任意 OpenAI 兼容平台
```

## 特性

| 能力 | 说明 |
|------|------|
| **OpenAI 兼容入口** | `/v1/chat/completions`（流式/非流式）、`/v1/models`、`/v1/embeddings`、`/v1/images/generations` |
| **三级路由解析** | `route_to` 显式指定 > 静态 `models` 声明 > 动态目录（实时拉取上游 `/models`，TTL 缓存 + 负缓存防打爆）——平台上新模型零配置可调 |
| **配额感知调度** | 每 key 维护滑动窗口 TPM/RPM/并发，选 key = 归一化余量 × **60s 历史成功率权重** × **限流连击衰减**；老失败/老被限的 key 自动沉底 |
| **熔断与半开探测** | 429 只短冷却（15s 起按连击指数升级，封顶 `cooldown_seconds`），尊重上游 `Retry-After`；冷却到期不直接复活，先放行**单个探测请求**，成功才恢复、失败翻倍重冷 |
| **跨平台故障切换** | `fallback_chain` 声明平台优先级，主平台全冷却时自动切下一家；全链耗尽返回 429 + 全链最短剩余时间的 `Retry-After` |
| **SSE 流式桥接** | 有界队列背压、客户端断开取消上游、每个退出路径保证 `[DONE]` 收尾（无僵尸连接）、自动注入 `stream_options.include_usage` 让调度器拿到真实 token 数 |
| **错误语义分级** | 客户端参数错（4xx）透传不重试不罚 key；401 重罚长冷却；429 平台级只让位不锁死；密钥脱敏日志 |
| **可观测性** | `/admin/stats`（key 状态/成功率/连击）、`/admin/usage`(SQLite 每日台账)、`/admin/probe`（手动探活）、`/admin/dashboard` 监控面板 |
| **运维友好** | 配置热加载（原子替换、校验失败保留旧配置、fail-loudly 校验拼错即报错）、后台 key 探活、日志轮转（10MB×5）、Windows 计划任务开机自启 |

## 快速开始

```bash
# 1. 安装依赖（Python 3.11+）
pip install -r requirements.txt

# 2. 准备配置
cp config.example.yaml config.yaml   # 填入你的平台 key 与 master_key

# 3. 启动
python main.py

# 4. 验证
curl http://127.0.0.1:8000/v1/health
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer <master_key>" -H "Content-Type: application/json" \
  -d '{"model":"sensenova-6.7-flash-lite","messages":[{"role":"user","content":"hi"}]}'
```

客户端只需把 base_url 指向 `http://127.0.0.1:8000/v1`、API key 填 master_key 即可。

## 配置速查

```yaml
master_key: "..."                    # 客户端鉴权用主 key
gateway:
  max_retries: 2                     # 重试次数
  cooldown_seconds: 60               # 冷却封顶
  probe_interval: 300                # 探活周期，0 关闭
  fallback_chain: [a, b]             # 跨平台切换优先级
providers:
  <name>:
    type: sensenova | openai_compatible
    keys: [...]                      # 多 key 自动调度
    models: [...]                    # 静态声明（动态发现自动补充）
    strategy: quota | round_robin
    tpm_threshold / rpm_threshold / concurrency_limit / cooldown_seconds
aliases: {别名: 真实模型名}           # 可选
```

## 管理端点

| 端点 | 说明 |
|------|------|
| `GET /v1/health` | 存活与各平台 key 冷却概况（免鉴权） |
| `GET /admin/stats` | 每 key 实时状态、模型健康、动态目录缓存 |
| `GET /admin/usage?days=7` | 每日用量台账（天/平台/模型/key 四维） |
| `POST /admin/probe` | 手动探活全部 key |
| `POST /admin/reload` | 热加载配置 |
| `GET /admin/dashboard` | 监控面板（浏览器打开） |

## 设计要点

- **429 ≠ key 坏了**。商汤的限流多为账号/平台级，把 429 当失败重罚会锁死全部 key 形成恶性循环——所以限流只短冷却让位，失败惩罚留给真正的失败（401/5xx）。
- **半开探测而非定时恢复**。冷却到期瞬间流量涌回最容易二次触发限流，放行单个探测请求确认健康再放量。
- **调度要有记忆**。瞬时余量决定"现在谁有空"，历史成功率决定"最近谁靠谱"——两个信号相乘才不会把请求反复砸进正在退化的 key。
- **流式桥接最怕悬挂**。reader 协程的每条退出路径（正常完成/异常/取消）都必须让消费端能终止，否则客户端拿着半开连接永远等。
- **配置 fail-loudly**。`strategy: qutoa` 这种拼写错误如果静默回退默认值，排查成本远高于启动时报错。

## 测试

```bash
python -m unittest tests.test_gateway -v
# 42 个用例：鉴权/故障切换/调度加权/熔断半开/流式桥接/动态目录/热加载/用量台账...
```

测试走 ASGI 内存传输 + 本地 mock 上游，不发真实请求，1.5 秒跑完。

## License

MIT
