/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"archive/tar"
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

// untarCmd represents the untar command
var untarCmd = &cobra.Command{
	Use:   "untar <tar文件> <目标目录>",
	Short: "并行解压 tar 包",
	Long: `根据 tar 包内的 manifest 文件并行解压文件。

支持的功能：
- 自动读取 tar 包内的 manifest 文件
- 并行写入文件，提高解压速度
- 显示解压进度

示例：
  p-tool untar output.tar /dest
  p-tool untar output.tar /dest --concurrency 8`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		tarFile := args[0]
		destDir := args[1]

		concurrency, _ := cmd.Flags().GetInt("concurrency")

		// 验证 tar 文件
		tarInfo, err := os.Stat(tarFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法访问 tar 文件 %s: %v\n", tarFile, err)
			os.Exit(1)
		}
		if tarInfo.IsDir() {
			fmt.Fprintf(os.Stderr, "错误: %s 不是一个文件\n", tarFile)
			os.Exit(1)
		}

		// 获取目标目录绝对路径
		absDestDir, err := filepath.Abs(destDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法获取目标目录绝对路径: %v\n", err)
			os.Exit(1)
		}

		// 创建目标目录（如果不存在）
		if err := os.MkdirAll(absDestDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法创建目标目录: %v\n", err)
			os.Exit(1)
		}

		// 设置并发数
		if concurrency <= 0 {
			concurrency = runtime.NumCPU()
		}

		fmt.Fprintf(os.Stdout, "开始解压 tar 包（并发数: %d）...\n", concurrency)

		// 并行解压 tar 包
		if err := extractTarParallel(tarFile, absDestDir, concurrency); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 解压 tar 包失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stdout, "\n解压完成！\n")
	},
}

func init() {
	rootCmd.AddCommand(untarCmd)

	untarCmd.Flags().Int("concurrency", 0, "指定并发数量，默认为 CPU 核数")
}

// fileEntry 存储从 tar 包中读取的文件数据
type fileEntry struct {
	header  *tar.Header
	content []byte
	err     error
}

// extractTarParallel 并行解压 tar 包
func extractTarParallel(tarFile, destDir string, concurrency int) error {
	// 打开 tar 文件
	tarFileHandle, err := os.Open(tarFile)
	if err != nil {
		return fmt.Errorf("无法打开 tar 文件: %w", err)
	}
	defer tarFileHandle.Close()

	// 创建带缓冲的 reader 提高性能
	bufferedReader := bufio.NewReaderSize(tarFileHandle, 64*1024)
	tarReader := tar.NewReader(bufferedReader)

	// 第一步：读取所有文件数据到内存，并找到 manifest 文件
	fileDataMap := make(map[string]*fileEntry)
	var manifestContent []byte
	manifestName := ".__p-tool-manifest__.txt"

	fmt.Fprintf(os.Stdout, "正在读取 tar 包内容...\n")

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取 tar header 失败: %w", err)
		}

		// 规范化路径（移除 ./ 前缀，统一使用斜杠）
		normalizedPath := strings.TrimPrefix(header.Name, "./")
		normalizedPath = filepath.ToSlash(normalizedPath)

		// 检查是否是 manifest 文件
		if normalizedPath == manifestName || strings.HasSuffix(normalizedPath, manifestName) {
			// 读取 manifest 内容
			manifestContent, err = io.ReadAll(tarReader)
			if err != nil {
				return fmt.Errorf("读取 manifest 文件失败: %w", err)
			}
			continue
		}

		// 读取文件内容（目录没有内容）
		var content []byte
		if header.Typeflag == tar.TypeReg {
			content, err = io.ReadAll(tarReader)
			if err != nil {
				return fmt.Errorf("读取文件内容失败 %s: %w", normalizedPath, err)
			}
		} else {
			// 对于非普通文件（如目录、符号链接等），跳过内容读取
			if _, err := io.Copy(io.Discard, tarReader); err != nil {
				return fmt.Errorf("跳过文件内容失败 %s: %w", normalizedPath, err)
			}
		}

		// 存储文件数据
		fileDataMap[normalizedPath] = &fileEntry{
			header:  header,
			content: content,
		}
	}

	// 检查是否找到 manifest 文件
	if manifestContent == nil {
		return fmt.Errorf("未找到 manifest 文件（%s），无法并行解压", manifestName)
	}

	// 解析 manifest 文件，获取文件列表
	fileList, err := parseManifestContent(manifestContent)
	if err != nil {
		return fmt.Errorf("解析 manifest 文件失败: %w", err)
	}

	if len(fileList) == 0 {
		return fmt.Errorf("manifest 文件为空")
	}

	fmt.Fprintf(os.Stdout, "找到 %d 个文件，开始并行解压...\n", len(fileList))

	// 第二步：根据 manifest 文件列表并行写入文件
	totalFiles := int64(len(fileList))
	var processedFiles int64
	var failedFiles int64

	startTime := time.Now()

	// 创建任务通道和数据通道
	taskChan := make(chan string, concurrency*2)

	var wg sync.WaitGroup
	var mu sync.Mutex

	// 启动进度更新协程
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				updateUntarProgress(atomic.LoadInt64(&processedFiles), totalFiles, startTime)
			case <-progressDone:
				return
			}
		}
	}()

	// 启动文件写入工作协程（并行写入文件）
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for relPath := range taskChan {
				entry, exists := fileDataMap[relPath]
				if !exists {
					mu.Lock()
					fmt.Fprintf(os.Stderr, "警告: manifest 中列出的文件在 tar 包中不存在: %s\n", relPath)
					mu.Unlock()
					atomic.AddInt64(&failedFiles, 1)
					atomic.AddInt64(&processedFiles, 1)
					continue
				}

				if err := writeFileEntry(destDir, relPath, entry); err != nil {
					mu.Lock()
					fmt.Fprintf(os.Stderr, "警告: 写入文件失败 %s: %v\n", relPath, err)
					mu.Unlock()
					atomic.AddInt64(&failedFiles, 1)
					atomic.AddInt64(&processedFiles, 1)
					continue
				}

				atomic.AddInt64(&processedFiles, 1)
			}
		}()
	}

	// 发送任务
	for _, relPath := range fileList {
		taskChan <- relPath
	}
	close(taskChan)

	// 等待所有写入协程完成
	wg.Wait()

	// 停止进度更新协程
	close(progressDone)
	time.Sleep(120 * time.Millisecond)

	// 显示最终进度
	updateUntarProgress(atomic.LoadInt64(&processedFiles), totalFiles, startTime)

	if failedFiles > 0 {
		return fmt.Errorf("有 %d 个文件解压失败", failedFiles)
	}

	return nil
}

// parseManifestContent 解析 manifest 文件内容，返回文件相对路径列表
func parseManifestContent(content []byte) ([]string, error) {
	var fileList []string
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			// 移除开头的 ./
			normalizedPath := strings.TrimPrefix(line, "./")
			normalizedPath = filepath.ToSlash(normalizedPath)
			fileList = append(fileList, normalizedPath)
		}
	}
	return fileList, nil
}

// writeFileEntry 写入单个文件条目到目标目录
func writeFileEntry(destDir, relPath string, entry *fileEntry) error {
	// 构建目标文件路径
	targetPath := filepath.Join(destDir, relPath)

	// 根据文件类型处理
	switch entry.header.Typeflag {
	case tar.TypeReg:
		// 普通文件
		// 创建目录（如果需要）
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("创建目录失败 %s: %w", filepath.Dir(targetPath), err)
		}

		// 创建文件
		outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(entry.header.Mode))
		if err != nil {
			return fmt.Errorf("创建文件失败 %s: %w", targetPath, err)
		}

		// 写入文件内容
		if _, err := outFile.Write(entry.content); err != nil {
			outFile.Close()
			return fmt.Errorf("写入文件内容失败 %s: %w", targetPath, err)
		}

		// 设置文件权限和时间
		if err := os.Chmod(targetPath, os.FileMode(entry.header.Mode)); err != nil {
			// 权限设置失败不影响解压，只记录警告
		}
		if err := os.Chtimes(targetPath, entry.header.AccessTime, entry.header.ModTime); err != nil {
			// 时间设置失败不影响解压，只记录警告
		}

		outFile.Close()

	case tar.TypeDir:
		// 目录
		if err := os.MkdirAll(targetPath, os.FileMode(entry.header.Mode)); err != nil {
			return fmt.Errorf("创建目录失败 %s: %w", targetPath, err)
		}
		if err := os.Chmod(targetPath, os.FileMode(entry.header.Mode)); err != nil {
			// 权限设置失败不影响解压
		}
		if err := os.Chtimes(targetPath, entry.header.AccessTime, entry.header.ModTime); err != nil {
			// 时间设置失败不影响解压
		}

	case tar.TypeSymlink:
		// 符号链接
		// 创建目录（如果需要）
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("创建目录失败 %s: %w", filepath.Dir(targetPath), err)
		}

		if err := os.Symlink(entry.header.Linkname, targetPath); err != nil {
			// 如果符号链接已存在，尝试删除后重新创建
			if os.IsExist(err) {
				if err := os.Remove(targetPath); err != nil {
					return fmt.Errorf("删除已存在的符号链接失败 %s: %w", targetPath, err)
				}
				if err := os.Symlink(entry.header.Linkname, targetPath); err != nil {
					return fmt.Errorf("创建符号链接失败 %s: %w", targetPath, err)
				}
			} else {
				return fmt.Errorf("创建符号链接失败 %s: %w", targetPath, err)
			}
		}

	case tar.TypeLink:
		// 硬链接
		// 创建目录（如果需要）
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("创建目录失败 %s: %w", filepath.Dir(targetPath), err)
		}

		linkTarget := filepath.Join(destDir, entry.header.Linkname)
		if err := os.Link(linkTarget, targetPath); err != nil {
			// 如果硬链接已存在，尝试删除后重新创建
			if os.IsExist(err) {
				if err := os.Remove(targetPath); err != nil {
					return fmt.Errorf("删除已存在的硬链接失败 %s: %w", targetPath, err)
				}
				if err := os.Link(linkTarget, targetPath); err != nil {
					return fmt.Errorf("创建硬链接失败 %s: %w", targetPath, err)
				}
			} else {
				return fmt.Errorf("创建硬链接失败 %s: %w", targetPath, err)
			}
		}

	default:
		// 其他类型（如字符设备、块设备等），跳过
		return fmt.Errorf("不支持的文件类型: %c", entry.header.Typeflag)
	}

	return nil
}
