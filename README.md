# mini-iceberg

用 Java 17 从零实现的开放表格式（open table format），一份可运行的最小表格式内核。实现聚焦三个问题：一张表由哪些文件构成，一次写入如何变成原子操作，一次查询如何在元数据上完成文件裁剪。

覆盖表格式的完整主线：

- **元数据分层**：metadata → snapshot → manifest-list → manifest → data-file
- **ACID**：快照隔离、原子提交、乐观并发与冲突重试
- **隐藏分区**：identity / bucket / truncate / day 变换，数据跳过与谓词投影
- **演进能力**：Schema 演进、分区演进、时间旅行
- **读写路径**：Scan Planning、Overwrite / Delete / Row Delta、表维护
- **引擎集成**：适配器把 mini-Iceberg 接入 mini-Spark，打通存储格式与计算引擎

整个实现保持精简，每个机制对应一个独立模块，便于按模块阅读、编译和运行。

## 模块速览

共 18 个编号章节模块与 1 个代码附录模块。

| 部分 | 模块 | 主题 |
|------|------|------|
| 第一部分 · 问题与动机 | Ch1-4 | 从一堆 Parquet 文件说起、表是一棵元数据树、Catalog 目录服务、写入路径 |
| 第二部分 · 快照与并发 | Ch5-8 | 快照隔离、原子提交、时间旅行、乐观并发与冲突 |
| 第三部分 · 隐藏分区 | Ch9-12 | 隐藏分区、数据跳过、谓词投影、分区演进 |
| 第四部分 · 查询与写入 | Ch13-15 | Schema 演进、Scan Planning、高级写入 |
| 第五部分 · 维护与集成 | Ch16-18 | 表维护、接入 mini-Spark、表格式实现复盘 |
| 附录 | B | 从行到列：分析型数据格式 |

## 构建

```bash
mvn -q compile                          # 编译全部模块
mvn -q -pl ch01-parquet-pain compile    # 只编译第 1 章
mvn -q -pl ch01-parquet-pain exec:java  # 跑第 1 章示例
```

> 需要 **JDK 17** 与 **Maven**。

第 17 章（接入 mini-Spark）额外依赖 mini-Spark 的 `ch15-real-spark` 构件。运行前先在本地的 mini-Spark 项目中安装该构件：

```bash
# 在 mini-Spark 项目根目录执行
mvn -q -pl ch15-real-spark -am install -DskipTests
```

## 代码组织

每个模块是一个独立的 Maven 子模块，包含该章的完整可运行代码。模块之间没有横向依赖，可独立编译、独立运行。

- `com.iceberglearn` — 各章实现（元数据、manifest、Catalog、Scan、写入、维护等）
- `com.iceberglearn.integration` — 第 17 章的 mini-Spark 适配器

运行时生成的数据文件落在模块的 `data/` 目录下，已在 `.gitignore` 中忽略。

## 许可

代码以 [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) 许可发布。
