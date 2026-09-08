# imgpull — 可断点续传的 Docker 镜像下载器

imgpull 用 [go-containerregistry](https://github.com/google/go-containerregistry) 解析镜像引用、完成 registry 鉴权与平台选择，再把 manifest、config 和每一层 blob 交给 [aria2](https://aria2.github.io/) 单独断点续传下载。所有 blob 通过 SHA-256 原始字节校验后，原子提交进一个标准 **OCI Image Layout**，可直接被 skopeo / podman / containerd / crane 读取，也可导出为 `docker load` 可用的 docker-archive。

aria2 只负责"下载一个 URL 到一个文件"，**不会**整体拉镜像；manifest/config/每层 blob 都是独立任务，各自可续传。

## 特性

- **逐对象下载 + 逐对象续传**：每个 blob 一个 aria2 任务，`.part` 文件 + `.aria2` 控制文件保留在 `tmp/`，中断后重跑同一条命令即可从断点继续。
- **OCI Image Layout 输出**：`oci-layout` + `index.json` + `blobs/sha256/<hex>`，`index.json` 带 `org.opencontainers.image.ref.name` 与平台注解。
- **docker-archive 导出**（可选）：`-e docker-archive` 额外生成 `image.tar`，可直接 `docker load -i image.tar`。
- **完整 registry 协议**：401 → `WWW-Authenticate` → Bearer token 自动获取与刷新；307 重定向到带过期时间的签名 CDN URL 也能续传（对 CDN 的 Range 请求带上原始 Authorization）；token 过期时取消任务、重取 token、**保留 `.part`** 重新提交。
- **下载后强校验**：aria2 完成后再对原始压缩字节做一次 SHA-256 校验，通过才原子 rename 到 `blobs/sha256/`；不匹配则删除 `.part` 重新下载。
- **以 manifest digest 为缓存键**：同一 tag 指向不同摘要（如 tag 被更新）会生成新的下载目录，绝不混淆新旧镜像内容。
- **多架构支持**：从 OCI index 中选择 `-p` 指定的平台，自动跳过 `unknown/unknown` 的 attestation/证明 manifest。
- **实时进度与速度**：终端下显示动态面板——汇总行（已完成/总对象数、已下载/总字节、百分比、聚合速度、耗时）+ 每个在途对象一行的进度条与单独速度；管道/重定向时自动退化为周期汇总日志行。✓ 提交、重试、警告等事件行会先擦除面板再打印，不会与进度条交错错位。
- **崩溃恢复**：启动时读取 `state.json`，核对 `blobs/` 中已有文件、识别孤儿 `.part` 并交给 aria2 续传；全部就绪后才组装 `index.json`。
- **目录锁**：同一镜像目录用 `flock` 互斥，多进程并发拉取同一镜像不会互相踩踏。

## 安装

依赖：**Go 1.25+**；本机 `PATH` 中有 `aria2c`（或用 `-r` 指向远程 aria2 守护进程）。

```bash
make build            # 产出 bin/imgpull
# 或
go build -o bin/imgpull ./cmd/imgpull
```

## 快速上手

最简用法——不需要任何参数（默认平台 `linux/amd64`，下载到当前目录）：

```bash
imgpull docker.io/library/nginx:latest
# 产物在 ./docker.io/library/nginx/latest/
```

常用变体：

```bash
# 短参数：输出目录 + 并发数 + 平台
imgpull docker.io/library/nginx:latest -o ./nginx -c 3 -p linux/arm64

# 指定外部 aria2 守护进程（不自行拉起 aria2c）
imgpull quay.io/org/app:1.2 -r http://127.0.0.1:6800/jsonrpc -s TOKEN

# 同时导出 docker-archive
imgpull docker.io/library/alpine:3.20 -e docker-archive
docker load -i ./docker.io/library/alpine/3.20/image.tar

# 私有仓库（凭据默认读 docker login 配置，登录过就不用传；http registry 加 -k）
imgpull registry.local:5000/dev/foo:main -k
```

下载过程中的终端显示（动态刷新，每层一行进度条 + 实时速度；URL 解析、SHA-256 校验等阶段也会以 `preparing…` / `verifying…` 行显示，不会出现"卡住没输出"的假象）：

```text
imgpull: ✓ config c6348fa86ba0 (459 B) [1/2]
1/2 objects | 1.3 MiB / 2.1 MiB (61%) | 474.9 KiB/s | 1 active | 4s
layer b05093807bb0  [████████████░░░░░░░░]  61%  1.3 MiB/2.1 MiB  474.9 KiB/s
layer e3f84ba91c72  preparing… (resolving URL / waiting for aria2)
imgpull: ✓ layer b05093807bb0 (2.1 MiB) [2/2]
2/2 objects | 2.1 MiB / 2.1 MiB (100%) | 0 active | 6s
```

输出重定向到管道/文件时退化为每隔几秒一行汇总日志（`progress N/M files committed | … | …/s | … active`），不会刷屏。

镜像源对未缓存的 blob 回源较慢时（如某些高校/公益镜像），对象会先停在 `preparing…`；imgpull 会自动把 HEAD 探测降级为 GET 探测避免无限等待，超过 10 秒还会打一行 `resolving … took Ns (slow origin pull on the mirror?)` 提示。

## 全部选项

| 选项 | 简写 | 默认 | 说明 |
| --- | --- | --- | --- |
| `--platform OS/ARCH[/VARIANT]` | `-p` | `linux/amd64` | 目标平台，从多架构 index 中选取 |
| `--output DIR` | `-o` | `.`（当前命令执行目录） | 下载根目录 |
| `--aria2-rpc URL` | `-r` | 自行拉起 aria2c | 复用外部 aria2 JSON-RPC 守护进程 |
| `--aria2-secret TOKEN` | `-s` | 空 | 外部守护进程的 RPC 密钥 |
| `--concurrency N` | `-c` | 3 | 并行下载的层数（每层 1 连接） |
| `--max-retries N` | `-R` | 5 | 单个对象的最大重试次数（指数退避 + 抖动） |
| `--export FORMAT` | `-e` | 空 | `docker-archive`：额外写出 `image.tar` |
| `--username` / `--password` | `-u` / `-P` | 读 `~/.docker/config.json` | registry 凭据；默认使用 docker login 的登录凭据 |
| `--insecure` | `-k` | false | 用 `http://` 访问 registry |
| `--verify` | `-V` | false | 启动时对已提交 blob 重新做 SHA-256 校验 |
| `--version` | `-v` | — | 打印版本 |
| `--help` | `-h` | — | 帮助 |

短参数支持 POSIX 风格写法：`-c 3`、`-c3`、`-c=3` 等价；布尔短参数可组合，`-kV` 等价于 `-k -V`。

## Registry 凭据

imgpull 默认读取 **docker 配置文件**（`~/.docker/config.json`，可用 `$DOCKER_CONFIG` 改路径），即 `docker login` 保存的各 registry 凭据——登录过的仓库直接拉取，无需传参。credential helper（如 `docker-credential-*`）同样支持。

显式传 `-u user -P pass` 时优先使用显式凭据；两者都没有时按匿名拉取。

## 目录布局

```
<output>/<registry>/<repo>/<tag>/
├── oci-layout            # {"imageLayoutVersion":"1.0.0"}
├── index.json            # OCI index（含平台与 ref.name 注解）
├── manifest.json         # 所选平台的原始 manifest
├── state.json            # imgpull 任务状态（digest/URL/进度/重试）
├── image.tar             # docker-archive（仅 -e docker-archive 时）
├── .imgpull.lock         # 目录互斥锁
├── blobs/sha256/<hex>    # 已校验提交的 blob（manifest/config/层）
└── tmp/sha256_<hex>.part # aria2 断点续传文件（+ .aria2 控制文件）
```

- 镜像身份 = registry + repository + **manifest digest**：`state.json` 与目录按摘要对齐，tag 仅作展示。
- 下载中中断：`tmp/*.part` 与 `.aria2` 控制文件保留；**重跑同一条命令**即自动续传（外部守护进程模式下还会先查找守护进程里的存活任务直接复用）。
- 全部 blob 校验提交后才会写入 `index.json`，布局始终处于一致状态。

## 工作原理

每个镜像拆成若干下载对象（manifest、config、各层 blob），状态机为 `pending → downloading → committed`：

1. **解析**：go-containerregistry 拉取 manifest（多架构 index 中按平台选取），得到对象清单与 digest。
2. **对账**：读取 `state.json`，跳过已提交的 blob，识别孤儿 `.part`。
3. **下载**：把每个对象作为独立任务提交给 aria2（`dir`/`out`/`header`/`checksum` 每任务指定）；aria2 负责断点续传（Range + 控制文件）。
4. **校验**：aria2 完成后 imgpull 对文件再做一次 SHA-256 校验，通过才原子 rename 进 `blobs/sha256/`；失败则删除重下。
5. **收尾**：全部提交后组装 `index.json`（可选导出 docker-archive）。

失败分类驱动重试策略：token/URL 过期 → 刷新凭据并保留断点重试；网络/超时 → 指数退避重试（1s 起步、封顶 30s、带抖动）；磁盘/致命错误 → 立即失败；aria2 报校验/断点损坏 → 清理 `.part` 后重下。

## aria2 参数说明

imgpull 默认自行拉起一个 aria2c 守护进程（任务结束自动退出，`--stop-with-process` 绑定生命周期），每任务选项：

```
--continue=true --split=1 --max-connection-per-server=1 --file-allocation=none
```

每任务附加 `checksum`（`sha-256=<hex>`），aria2 完成时也会先自校验一遍；旧版 aria2 不认识该选项时自动降级重试。不透明 URL（如 307 跳转后的 CDN 签名地址）过期时，imgpull 会重新解析签名并保留断点文件重提任务。

## 开发

```bash
make test         # go vet + go test ./...（含拉起真实 aria2c 的集成测试，需 PATH 有 aria2c）
make test-short   # 跳过真实 aria2 集成测试
make fmt          # gofmt
make build        # bin/imgpull（-trimpath -ldflags "-s -w" 精简构建）
make install      # 安装到 /usr/local/bin（PREFIX=/path 可改安装位置）
```

### 项目结构

```
cmd/imgpull/            # CLI 入口：参数解析（长/短参数）、信号处理、目录锁
internal/reference/     # 镜像引用解析
internal/registry/      # 鉴权（token/basic）、manifest 解析、平台选择、blob URL 解析
internal/plan/          # 任务清单 + state.json 持久化 + 文件系统对账
internal/downloader/    # aria2 JSON-RPC 客户端、守护进程管理、下载调度器、重试分类
internal/oci/           # OCI Layout 组装（oci-layout / index.json）
internal/export/        # docker-archive 导出
internal/fsx, verify    # 原子文件写、SHA-256/大小校验
```

### 测试覆盖

- 单元测试：短参数展开、reference 解析、SHA-256 校验、plan 状态机/续传对账、aria2 RPC 协议、失败分类与退避上界、进度面板渲染（进度条、汇总行、TTY 擦除重绘与事件行穿插）。
- 集成测试（`TestRealAria2*`）：拉起**真实 aria2c** 守护进程 + httptest 模拟 registry（含 token 鉴权、307 签名 URL、限速服务端），覆盖限速下载、杀进程中断后按 Range 断点续传、`.part` 校验失败重下等场景。

## 已知问题（中国大陆网络）

`docker.io` 在部分地区存在 DNS 污染，直连会超时。可用镜像源代替（语法不变，替换 registry 即可）：

```bash
imgpull docker.m.daocloud.io/library/nginx:latest
```
