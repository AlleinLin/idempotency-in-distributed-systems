# 示例与实验

三组代码各自运行，使用独立目录和依赖。Python 模块保持在所属示例中，直接从该目录运行脚本。

| 目录 | 用途 | 说明 |
| --- | --- | --- |
| [chapter-01](chapter-01/README.md) | 最小下单服务 | HTTP、PostgreSQL、本地扣款事务 |
| [transactional](transactional/README.md) | 第 15 章完整协议 | 四个数据库、外部效果、回退恢复、退款模型 |
| [multipartition](multipartition/README.md) | 第四部分系统实验 | 多 Raft 组、服务进程、消息与迁移 |

实验运行记录写入新的 `runs/<name>` 目录；保留该目录即可追溯历史与检查结果。项目未附带数据库运行数据、第三方运行时或预编译二进制。默认监听地址为本机回环地址，测试账户与认证令牌在各示例中说明。
