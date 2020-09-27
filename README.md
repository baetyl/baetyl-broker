baetyl-broker
========

[![Build Status](https://travis-ci.org/baetyl/baetyl-broker.svg?branch=master)](https://travis-ci.org/baetyl/baetyl-broker)
[![Go Report Card](https://goreportcard.com/badge/github.com/baetyl/baetyl-broker)](https://goreportcard.com/report/github.com/baetyl/baetyl-broker) 
[![codecov](https://codecov.io/gh/baetyl/baetyl-broker/branch/master/graph/badge.svg)](https://codecov.io/gh/baetyl/baetyl-broker)
[![License](https://img.shields.io/github/license/baetyl/baetyl-broker.svg)](./LICENSE)

## 简介

Baetyl-Broker 基于 Golang 语言开发，是一个单机版地消息订阅和发布中心，采用 MQTT3.1.1 协议，可在低带宽、不可靠网络中提供可靠的消息传输服务。其作为 Baetyl 框架端侧的消息中间件，为所有服务提供消息驱动的互联能力。

目前支持 4 种接入方式：TCP、SSL（TCP + SSL）、WS（Websocket）及 WSS（Websocket + SSL），MQTT 协议支持度如下：

- 支持 `Connect`、`Disconnect`、`Subscribe`、`Publish`、`Unsubscribe`、`Ping` 等功能
- 支持 QoS 等级 0 和 1 的消息发布和订阅
- 支持 `Retain`、`Will`、`Clean Session`
- 支持订阅含有 `+`、`#` 等通配符的主题
- 支持符合约定的 ClientID 和 Payload 的校验
- 支持认证鉴权，在传输层使用 tls 证书做双向认证，在应用层支持 ACL 权限控制
- 暂时 **不支持** 发布和订阅以 `$` 为前缀的主题
- 暂时 **不支持** Client 的 Keep Alive 特性以及 QoS 等级 2 的发布和订阅

## 配置

Baetyl-Broker 的全量配置文件如下，并对配置字段做了相应解释：

```yaml
listeners: # [必须]监听地址，例如：
  - address: tcp://0.0.0.0:1883 # tcp 连接
  - address: ssl://0.0.0.0:1884 # ssl 连接，ssl 连接必须配置证书
    ca: example/var/lib/baetyl/testcert/ca.crt # Server 的 CA 证书路径
    key: example/var/lib/baetyl/testcert/server.key # Server 的服务端私钥路径
    cert: example/var/lib/baetyl/testcert/server.crt # Server 的服务端公钥路径
    anonymous: true # 如果 anonymous 为 true，服务端对该端口不进行 ACL 验证
  - address: ws://0.0.0.0:8883/mqtt # ws 连接
  - address: wss://0.0.0.0:8884/mqtt # wss 连接，wss 连接必须配置证书
    ca: example/var/lib/baetyl/testcert/ca.crt # Server 的 CA 证书路径
    key: example/var/lib/baetyl/testcert/server.key # Server 的服务端私钥路径
    cert: example/var/lib/baetyl/testcert/server.crt # Server 的服务端公钥路径
    anonymous: false # 如果 anonymous 为 true，服务端对该端口不进行 ACL 验证
principals: # ACL 权限控制，支持账号密码和证书认证
  - username: test # 用户名
    password: hahaha # 密码
    permissions: # 权限控制
      - action: pub # pub 权限
        permit: ["test"] # 允许的 topic，支持通配符
      - action: sub # pub 权限
        permit: ["test"] # 允许的 topic，支持通配符
  - username: client # 如果密码为空，username 表示客户端证书的 common name，用于做证书连接的客户端的 ACL 验证
    permissions: # 权限控制
      - action: pub # pub 权限
        permit: ["#"] # 允许的 topic，支持通配符
      - action: sub # pub 权限
        permit: ["#"] # 允许的 topic，支持通配符
session: # 客户端 session 相关的设置
  maxClients: 0 # 服务端最大客户端连接数，如果为 0 或者负数表示不做限制
  maxMessagePayloadSize: 32768 # 可允许传输的最大消息长度，默认 32768 字节（32K），最大值为 268,435,455字节(约256MB) - 1
  maxInflightQOS0Messages: 100 # QOS0 消息的飞行窗口
  maxInflightQOS1Messages: 20 # QOS1 消息的飞行窗口
  resendInterval: 20s # 消息重发间隔，如果客户端在消息重发间隔内没有回复确认（ack），消息会一直重发，直到客户端回复确认或者 session 关闭
  persistence: # 消息持久化相关配置
    store: # 底层存储插件配置
      driver: boltdb # 底层存储插件，默认 boltdb
      source: var/lib/baetyl/broker.db # 存储文件路径
    queue: # 存储
      batchSize: 10 # 消息通道缓存大小
      expireTime: 24h # 消息过期时间间隔，在此间隔前的消息在下次清理时会被清理掉
      cleanInterval: 1h # 消息清理间隔，后台会按照此间隔定期清理过期消息
      writeTimeout: 100ms # 批量写超时间隔，按照此间隔进行写操作，如果间隔时间内，缓存满了，也会触发写操作
      deleteTimeout: 500ms # 批量删除已确认消息超时间隔，按照此间隔进行对已确认的消息进行删除操作，如果间隔时间内，已确认消息缓存满了，也会触发删除操作 
  sysTopics: ["$link", "$baidu"] # 系统主题

logger: # 日志
  level: info # 日志等级
```