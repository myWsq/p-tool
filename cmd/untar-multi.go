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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

// untarMultiCmd 表示并行解压多个 tar 包的命令
var untarMultiCmd = &cobra.Command{
	Use:   "untar-multi <源目录> <目标目录>",
	Short: "并行解压多个 tar 包",
	Long: `并行解压由 tar-multi 命令生成的多个 tar 包到一个完整目录。

支持的功能：
- 自动检测源目录中的 part-*.tar 文件
- 并行解压多个 tar 包，提高解压速度
- 自动处理文件冲突（如果多个 tar 包包含相同文件，只解压一次）
- 显示解压进度

示例：
  p-tool untar-multi /output /dest
  p-tool untar-multi /output /dest --concurrency 8`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		sourceDir := args[0]
		destDir := args[1]

		concurrency, _ := cmd.Flags().GetInt("concurrency")

		// 验证源目录
		sourceInfo, err := os.Stat(sourceDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法访问源目录 %s: %v\n", sourceDir, err)
			os.Exit(1)
		}
		if !sourceInfo.IsDir() {
			fmt.Fprintf(os.Stderr, "错误: %s 不是一个目录\n", sourceDir)
			os.Exit(1)
		}

		// 获取源目录绝对路径
		absSourceDir, err := filepath.Abs(sourceDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法获取源目录绝对路径: %v\n", err)
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

		// 查找所有 tar 包文件
		tarFiles, err := findTarFiles(absSourceDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 查找 tar 包失败: %v\n", err)
			os.Exit(1)
		}

		if len(tarFiles) == 0 {
			fmt.Fprintf(os.Stderr, "错误: 在源目录中未找到 tar 包文件（part-*.tar）\n")
			os.Exit(1)
		}

		// 设置并发数
		if concurrency <= 0 {
			concurrency = runtime.NumCPU()
		}

		fmt.Fprintf(os.Stdout, "找到 %d 个 tar 包，开始并行解压（并发数: %d）...\n", len(tarFiles), concurrency)

		// 并行解压多个 tar 包
		if err := extractMultipleTarsParallel(absSourceDir, absDestDir, tarFiles, concurrency); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 解压 tar 包失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stdout, "\n解压完成！\n")
	},
}

func init() {
	rootCmd.AddCommand(untarMultiCmd)

	untarMultiCmd.Flags().Int("concurrency", 0, "指定并发数量，默认为 CPU 核数")
}

// findTarFiles 查找源目录中的所有 part-*.tar 文件
func findTarFiles(sourceDir string) ([]string, error) {
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("无法读取源目录: %w", err)
	}

	var tarFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "part-") && strings.HasSuffix(entry.Name(), ".tar") {
			tarFiles = append(tarFiles, entry.Name())
		}
	}

	// 按文件名排序，确保顺序一致
	sort.Strings(tarFiles)

	return tarFiles, nil
}

// extractMultipleTarsParallel 并行解压多个 tar 包
func extractMultipleTarsParallel(sourceDir, destDir string, tarFiles []string, concurrency int) error {
	// 用于跟踪已解压的文件，避免重复解压
	extractedFiles := sync.Map{}

	// 全局文件进度计数器
	var processedFiles int64
	var totalFiles int64

	startTime := time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failedTars int64

	// 启动进度更新协程
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				current := atomic.LoadInt64(&processedFiles)
				total := atomic.LoadInt64(&totalFiles)
				if total > 0 {
					updateUntarProgress(current, total, startTime)
				}
			case <-progressDone:
				return
			}
		}
	}()

	// 并行解压每个 tar 包
	for _, tarFile := range tarFiles {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()

			tarFilePath := filepath.Join(sourceDir, filename)
			count, err := extractSingleTarParallel(tarFilePath, destDir, &extractedFiles, &processedFiles, &totalFiles)
			if err != nil {
				mu.Lock()
				fmt.Fprintf(os.Stderr, "错误: 解压 tar 包 %s 失败: %v\n", filename, err)
				mu.Unlock()
				atomic.AddInt64(&failedTars, 1)
			} else {
				mu.Lock()
				fmt.Fprintf(os.Stdout, "已解压 %s: %d 个文件\n", filename, count)
				mu.Unlock()
			}
		}(tarFile)
	}

	// 等待所有 tar 包解压完成
	wg.Wait()

	// 停止进度更新协程
	close(progressDone)
	time.Sleep(120 * time.Millisecond)

	// 显示最终进度
	current := atomic.LoadInt64(&processedFiles)
	total := atomic.LoadInt64(&totalFiles)
	if total > 0 {
		updateUntarProgress(current, total, startTime)
	}

	if failedTars > 0 {
		return fmt.Errorf("有 %d 个 tar 包解压失败", failedTars)
	}

	return nil
}

// extractSingleTarParallel 解压单个 tar 包
func extractSingleTarParallel(tarFilePath, destDir string, extractedFiles *sync.Map, processedFiles, totalFiles *int64) (int, error) {
	// 打开 tar 文件
	tarFile, err := os.Open(tarFilePath)
	if err != nil {
		return 0, fmt.Errorf("无法打开 tar 文件: %w", err)
	}
	defer tarFile.Close()

	// 创建带缓冲的 reader 提高性能
	bufferedReader := bufio.NewReaderSize(tarFile, 64*1024)
	tarReader := tar.NewReader(bufferedReader)

	var fileCount int

	// 读取并解压文件
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fileCount, fmt.Errorf("读取 tar header 失败: %w", err)
		}

		// 跳过 manifest 文件（如果存在）
		if strings.HasSuffix(header.Name, ".__p-tool-manifest__.txt") {
			// 跳过文件内容
			if _, err := io.Copy(io.Discard, tarReader); err != nil {
				return fileCount, fmt.Errorf("跳过 manifest 文件失败: %w", err)
			}
			continue
		}

		// 规范化路径（移除 ./ 前缀，统一使用斜杠）
		normalizedPath := strings.TrimPrefix(header.Name, "./")
		normalizedPath = filepath.ToSlash(normalizedPath)

		// 检查文件是否已经解压过（使用 sync.Map 保证线程安全）
		if _, loaded := extractedFiles.LoadOrStore(normalizedPath, true); loaded {
			// 文件已经解压过，跳过
			if _, err := io.Copy(io.Discard, tarReader); err != nil {
				return fileCount, fmt.Errorf("跳过已解压文件失败: %w", err)
			}
			continue
		}

		// 更新总文件数（只统计第一次遇到的文件）
		atomic.AddInt64(totalFiles, 1)

		// 构建目标文件路径
		targetPath := filepath.Join(destDir, normalizedPath)

		// 创建目录（如果需要）
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fileCount, fmt.Errorf("创建目录失败 %s: %w", filepath.Dir(targetPath), err)
		}

		// 根据文件类型处理
		switch header.Typeflag {
		case tar.TypeReg:
			// 普通文件
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fileCount, fmt.Errorf("创建文件失败 %s: %w", targetPath, err)
			}

			// 复制文件内容
			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fileCount, fmt.Errorf("写入文件内容失败 %s: %w", targetPath, err)
			}

			// 设置文件权限和时间
			if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
				// 权限设置失败不影响解压，只记录警告
			}
			if err := os.Chtimes(targetPath, header.AccessTime, header.ModTime); err != nil {
				// 时间设置失败不影响解压，只记录警告
			}

			outFile.Close()
			fileCount++

		case tar.TypeDir:
			// 目录
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fileCount, fmt.Errorf("创建目录失败 %s: %w", targetPath, err)
			}
			if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
				// 权限设置失败不影响解压
			}
			if err := os.Chtimes(targetPath, header.AccessTime, header.ModTime); err != nil {
				// 时间设置失败不影响解压
			}

		case tar.TypeSymlink:
			// 符号链接
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				// 如果符号链接已存在，尝试删除后重新创建
				if os.IsExist(err) {
					if err := os.Remove(targetPath); err != nil {
						return fileCount, fmt.Errorf("删除已存在的符号链接失败 %s: %w", targetPath, err)
					}
					if err := os.Symlink(header.Linkname, targetPath); err != nil {
						return fileCount, fmt.Errorf("创建符号链接失败 %s: %w", targetPath, err)
					}
				} else {
					return fileCount, fmt.Errorf("创建符号链接失败 %s: %w", targetPath, err)
				}
			}

		case tar.TypeLink:
			// 硬链接
			linkTarget := filepath.Join(destDir, header.Linkname)
			if err := os.Link(linkTarget, targetPath); err != nil {
				// 如果硬链接已存在，尝试删除后重新创建
				if os.IsExist(err) {
					if err := os.Remove(targetPath); err != nil {
						return fileCount, fmt.Errorf("删除已存在的硬链接失败 %s: %w", targetPath, err)
					}
					if err := os.Link(linkTarget, targetPath); err != nil {
						return fileCount, fmt.Errorf("创建硬链接失败 %s: %w", targetPath, err)
					}
				} else {
					return fileCount, fmt.Errorf("创建硬链接失败 %s: %w", targetPath, err)
				}
			}

		default:
			// 其他类型（如字符设备、块设备等），跳过内容
			if _, err := io.Copy(io.Discard, tarReader); err != nil {
				return fileCount, fmt.Errorf("跳过不支持的文件类型失败: %w", err)
			}
		}

		// 更新已处理文件数
		atomic.AddInt64(processedFiles, 1)
	}

	return fileCount, nil
}

// updateUntarProgress 更新解压进度
func updateUntarProgress(current, total int64, startTime time.Time) {
	if total == 0 {
		return
	}
	percentage := float64(current) / float64(total) * 100

	// 计算每秒文件数
	elapsed := time.Since(startTime)
	var filesPerSec float64
	if elapsed.Seconds() > 0 {
		filesPerSec = float64(current) / elapsed.Seconds()
	}

	fmt.Fprintf(os.Stdout, "\r进度: %d/%d 文件 (%.1f%%) | 速度: %.1f 文件/秒", current, total, percentage, filesPerSec)
	os.Stdout.Sync()
}
