# AGH 钉钉渠道插件

[English](README.md) | 简体中文

## 市场与 Git 安装

官方市场收录后，可从“插件 → 市场”安装。也可使用下方 Git 来源检查、审阅并安装此版本：

```sh
agh plugins add git:https://github.com/lzc970718/agh-plugin-dingtalk.git#v0.3.8
```

发行提交包含完整 `dist/` 和图标，不需要用户构建或安装依赖。最低 AGH 合同版本为 0.1.0，且必须支持统一渠道桥和 Extension API ^5.8.0。


`@agnes/channel-dingtalk` 0.3.8，只提供钉钉平台适配器。需要包含统一渠道桥接的 AGH
（Extension API 5.8.0 或更高版本）。不包含微信、飞书或企业微信，不内置通用 runner、SDK、
daemon 发现或系统服务；这些能力由底座统一提供。

## 安装与入口

在 AGH「插件 → 从来源安装」选择本目录或 `agnes-channel-dingtalk-0.3.8.tgz`，审核并更新。
插件身份沿用 0.1.0，更新不会安装另一份插件。也可以手动执行：

```powershell
agh plugins add .\agnes-channel-dingtalk-0.3.8.tgz --yes
```

配置入口是「插件 → 渠道」，也可点击输入框旁的「渠道绑定」。
1. 在统一渠道页添加实例，选择 AGH 凭据、工作区和已启用的智能体，卡片模板 ID 可留空，默认使用普通 Markdown 消息。
2. 点击「保存并连接」，在 AGH 原生审批中确认，再点击「继续保存」。
3. 向钉钉机器人发送消息（群聊需 @），让底座确认聊天目标。
4. 打开已有 AGH 会话，点击「渠道绑定」，刷新、选择聊天目标，确认同步后绑定。
5. 页面显示连接状态及绑定目标，可显式解绑。新会话草稿须先发送消息创建会话。

只同步绑定后的新消息，未绑定聊天不会自动创建会话。重连后须先收到机器人消息重新确认目标。
待核对的绑定保持暂停，请显式解绑后重新绑定。原生审批在 AGH 界面中处理，不转发既有审批到钉钉。
0.2 插件升级到 0.3 时会新增三个会话服务权限，须审核更新后才能显示绑定能力。

## 配置

1. 在钉钉开放平台创建企业内部应用，启用机器人和 Stream 模式并发布到测试企业。
2. 授予机器人发消息、接收消息，以及需要的附件权限。只有启用互动卡片时才需卡片权限，并准备匹配本插件数据字段的钉钉卡片模板；
   模板号填写到实例的 `config.cardTemplateId`，不使用卡片时省略或留空。空白字符串同样视为未配置。普通消息模式不支持原地编辑和卡片按钮；已有模板配置保持卡片模式。
3. 在 AGH 凭据管理中分别保存 Client ID、Client Secret；取得对应的 `secret://` 引用。
   凭据不得写进本项目、实例配置或日志。底座只在连接时解析引用。
4. 以下为等价的初始配置结构（优先使用统一渠道页维护受管配置）。工作区必须是本机存在的绝对路径，智能体必须是在当前 AGH 可用的
   `package#bundle` 引用。`allowFrom` 填允许接入的钉钉用户 ID；群聊建议保留 `requireMention: true`。

```json
{
  "instances": [
    {
      "id": "company-a",
      "workspace": "D:\\Work\\YourProject",
      "agent": "@your-org/agent#assistant",
      "credentialRefs": {
        "clientId": "secret://dingtalk/client-id",
        "clientSecret": "secret://dingtalk/client-secret"
      },
      "allowFrom": ["your-dingtalk-user-id"],
      "requireMention": true,
      "config": { "cardTemplateId": "your-card-template-id" }
    }
  ]
}
```

配置也可以通过文件提交，避免凭据或配置进入命令行参数：

```powershell
agh plugins config validate @agnes/channel-dingtalk --file D:\Private\dingtalk-instances.json
agh plugins config set @agnes/channel-dingtalk --file D:\Private\dingtalk-instances.json
```

一个插件可配置多个企业实例，每个实例有独立工作区、智能体、会话命名空间和出站回执。
`credentialRefs` 可选字段为 `robotCode`、`agentId`，同样只接受 AGH 凭据引用。
实例 `config` 只接受非敏感的 `cardTemplateId`。

平台健康检查通过当前插件的 `channel.check` profile 服务读取，运行状态通过 `channel.status` 读取。
受管资源配置、重试、核对使用标准 `channel.configure/retry/reconcile` 效果服务并保留原生审批。
如果使用了受管资源的 `channel.configure` 覆盖值，它会优先于插件配置；后续应继续从同一入口维护配置。
未知的操作结果应先查询/核对，不能换一个操作编号盲目重放。

## 从 0.1.0 升级

旧 `configFile`、`stateDirectory` 字段仅为兼容升级保留，新版本不会读取 YAML 或原凭据文件。
先停止旧独立进程，备份旧文件，再按上面重新配置 AGH 凭据引用和实例。旧文件、消息回执和会话不删除，
也不会自动迁入新的实例命名空间。不要让两个版本同时连接同一机器人。

## 开发

使用与 Harness `.node-version` 一致的 Node，安装本项目开发依赖后执行 `node prepare.mjs`，
通过 `AGH_SOURCE` 指定 Harness 源码位置（默认取相邻 `agnes-harness`）。
`node build.mjs` 生成只包含钉钉 SDK 和适配器的产物，公开合同保留为宿主提供的 external，
不得把 `channelKind` 复制或打包为另一份 token。
适配器协议测试使用假网关；真实钉钉的群 @、卡片回调、断网重连和附件下载仍需部署账号验证。



## 回复消息所用的机器人编码

`robotCode` 为可选高级凭据。未显式配置时，插件从当前已认证 Stream 连接收到的机器人消息中获取 `robotCode`；显式配置的值优先。
切换应用凭据后会清除自动获取的机器人编码，旧连接的消息不能为新应用设置编码。若回调没有提供编码，可在高级设置选择保存的 `robotCode` 凭据，值取自钉钉应用的机器人配置。
普通 Markdown 回复使用同一会话中消息发送人的 staffId 作为单聊收件人；只有设置卡片模板后才调用互动卡片接口。

## 渠道图标

`assets/dingtalk.png` 是随包携带的钉钉图标原图。构建时将本地图片嵌入 provider 与 adapter 的同一份 `manifest.icon`，运行时不访问图片 CDN；需要 AGH Extension API 5.8.0。未提供或图片不可用时由底座显示默认渠道图标。

## 入站附件

0.3.5 修复图片、文件与语音的入站同步，并支持视频和富文本图片。插件提交真实字节，底座原生会话附件准入负责存储和授权；不再创建没有文件内容的 attachment:// 引用。单条消息最多 50 个附件、下载合计最多 20 MiB。下载失败、缺少下载码或超限时在同步消息中提示，已成功下载的附件仍保留。PNG/JPEG 进入原生图片输入，其他格式作为文件保存；音视频保存不意味着模型具备原生音视频理解能力。

0.3.5 兼容 Stream 回调中对象或 JSON 字符串形式的 `content`，避免已有下载码被误判为缺失。失败提示区分缺码、获取地址 HTTP 拒绝、文件下载 HTTP 拒绝、网络失败与超限，不包含凭据、下载码或临时 URL。

0.3.7 下载地址校验：默认端口的 HTTP 地址仅升级为 HTTPS 后使用，绝不通过明文下载；签名 URL 长度预算为 32768 字符。继续拒绝本机、内网、保留 IP、混合公网/内网 DNS 与带用户凭据的地址，并将已校验的 DNS 地址固定到 TLS 连接。DNS 失败、地址缺失、格式与公网地址拒绝分别提示，不回显签名 URL。

## 渠道名称语言

0.3.7 起使用 AGH Extension API 5.8.0 的 manifest.displayNames，英文界面显示 DingTalk，中文界面显示 钉钉。渠道身份与现有实例、绑定保持不变。请先更新到包含该能力的 Harness，再更新插件。
