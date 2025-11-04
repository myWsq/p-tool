# p-tool

一个高性能的并行文件复制工具，基于 manifest 文件实现快速、可靠的目录复制。

## 功能特性

- **并行复制**：支持多线程并发复制，充分利用系统资源，大幅提升复制速度
- **自动生成 manifest**：如果未指定 manifest 文件，工具会自动扫描源目录并生成
- **进度显示**：实时显示复制进度和速度（文件/秒）
- **目录结构保留**：自动创建目标目录结构，完整保留源目录的层级关系
- **智能优化**：针对小文件场景进行优化，减少系统调用和重复操作
- **错误处理**：完善的错误提示和异常处理机制

## 安装

### 从源码编译

```bash
git clone https://github.com/mywsq/p-tool.git
cd p-tool
go build -o p-tool
```

### 使用 Go 安装

```bash
go install github.com/mywsq/p-tool@latest
```

## 使用方法

### cp 命令 - 并行复制文件

根据 manifest 文件并行复制源目录内的文件至目标目录并保留对应结构。

**基本用法：**

```bash
p-tool cp <源目录> <目标目录>
```

**选项：**

- `--manifest-file <路径>`：指定 manifest 文件路径（可选，未指定时自动生成）
- `--concurrency <数量>`：指定并发数量，默认为 CPU 核数

**示例：**

```bash
# 自动生成 manifest 并复制
p-tool cp /source /dest

# 使用指定的 manifest 文件
p-tool cp /source /dest --manifest-file /tmp/manifest.txt

# 指定并发数量为 8
p-tool cp /source /dest --concurrency 8
```

### manifest 命令 - 生成 manifest 文件

扫描指定目录并生成一个 manifest 文件，文件中每一行都是该目录下文件的相对路径。

**基本用法：**

```bash
p-tool manifest <目录路径> <manifest文件路径>
```

**示例：**

```bash
p-tool manifest /root /tmp/manifest.txt
```

**manifest 文件格式：**

```
./file1.txt
./subdir/file2.txt
./subdir/nested/file3.txt
```

## 工作原理

1. **生成 manifest**：扫描源目录，生成包含所有文件相对路径的 manifest 文件
2. **预创建目录**：根据 manifest 文件预创建所有需要的目标目录结构
3. **并行复制**：使用多个 goroutine 并发读取和写入文件，提高复制效率
4. **进度监控**：实时更新复制进度和速度统计

## 性能优化

- **目录缓存**：使用 `sync.Map` 缓存已创建的目录，避免并发时重复创建
- **缓冲 I/O**：使用 64KB 缓冲区的读写器，减少系统调用次数
- **预创建目录**：在复制前批量创建所有目录，避免复制过程中的目录创建开销
- **节流更新**：进度更新使用 100ms 节流，避免高并发时频繁跳动

## 注意事项

- 工具会自动跳过目录和指向目录的符号链接，只处理普通文件
- 如果源文件不存在，会显示警告但不会中断整个复制过程
- 复制过程中会显示实时进度，格式为：`进度: 100/1000 (10.0%) | 速度: 50.0 文件/秒`
- 默认并发数为 CPU 核数，可根据实际情况调整以获得最佳性能

## 许可证

Copyright © 2025 NAME HERE <EMAIL ADDRESS>

## 贡献

欢迎提交 Issue 和 Pull Request！

