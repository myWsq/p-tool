/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
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

// cpCmd represents the cp command
var cpCmd = &cobra.Command{
	Use:   "cp <源目录> <目标目录>",
	Short: "并行复制文件",
	Long: `根据 manifest 文件并行复制源目录内的文件至目标目录并保留对应结构。

支持的功能：
- 自动创建目标目录
- 自动生成 manifest 文件（如果未指定）
- 并行复制文件，提高复制速度
- 显示复制进度

示例：
  p-tool cp /source /dest
  p-tool cp /source /dest --manifest-file /tmp/manifest.txt
  p-tool cp /source /dest --concurrency 8`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		sourceDir := args[0]
		destDir := args[1]

		manifestFile, _ := cmd.Flags().GetString("manifest-file")
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

		// 如果未指定 manifest 文件，自动生成
		if manifestFile == "" {
			tmpFile, err := os.CreateTemp("", "p-tool-manifest-*.txt")
			if err != nil {
				fmt.Fprintf(os.Stderr, "错误: 无法创建临时 manifest 文件: %v\n", err)
				os.Exit(1)
			}
			tmpFile.Close()
			manifestFile = tmpFile.Name()

			// 使用共享的 GenerateManifest 函数扫描源目录
			if err := GenerateManifest(absSourceDir, manifestFile); err != nil {
				os.Remove(manifestFile)
				fmt.Fprintf(os.Stderr, "错误: 生成 manifest 文件失败: %v\n", err)
				os.Exit(1)
			}

			fmt.Fprintf(os.Stdout, "提示: 已自动生成 manifest 文件: %s\n", manifestFile)
		}

		// 读取 manifest 文件
		fileList, err := readManifest(manifestFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 读取 manifest 文件失败: %v\n", err)
			os.Exit(1)
		}

		if len(fileList) == 0 {
			fmt.Fprintf(os.Stderr, "错误: manifest 文件为空\n")
			os.Exit(1)
		}

		// 创建目标目录
		if err := os.MkdirAll(destDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法创建目标目录 %s: %v\n", destDir, err)
			os.Exit(1)
		}

		// 获取目标目录绝对路径
		absDestDir, err := filepath.Abs(destDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法获取目标目录绝对路径: %v\n", err)
			os.Exit(1)
		}

		// 设置并发数
		if concurrency <= 0 {
			concurrency = runtime.NumCPU()
		}

		fmt.Fprintf(os.Stdout, "开始复制 %d 个文件（并发数: %d）...\n", len(fileList), concurrency)

		// 预创建所有目录（小文件场景优化：避免并发时重复创建目录）
		fmt.Fprintf(os.Stdout, "预创建目录结构中...\n")
		if err := precreateDirectories(absDestDir, fileList); err != nil {
			fmt.Fprintf(os.Stderr, "警告: 预创建目录失败，将按需创建: %v\n", err)
		}

		// 并行复制文件
		if err := copyFilesParallel(absSourceDir, absDestDir, fileList, concurrency); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 复制文件失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stdout, "\n复制完成！\n")
	},
}

func init() {
	rootCmd.AddCommand(cpCmd)

	cpCmd.Flags().String("manifest-file", "", "指定 manifest 文件路径（可选）")
	cpCmd.Flags().Int("concurrency", 0, "指定并发数量，默认为 CPU 核数")
}

// readManifest 读取 manifest 文件，返回文件相对路径列表
func readManifest(manifestPath string) ([]string, error) {
	file, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("无法打开 manifest 文件: %w", err)
	}
	defer file.Close()

	var fileList []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			// 移除开头的 ./
			line = strings.TrimPrefix(line, "./")
			fileList = append(fileList, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取 manifest 文件时出错: %w", err)
	}

	return fileList, nil
}

// copyFilesParallel 并行复制文件
func copyFilesParallel(sourceDir, destDir string, fileList []string, concurrency int) error {
	totalFiles := int64(len(fileList))
	var copiedFiles int64
	var failedFiles int64

	// 记录开始时间，用于计算每秒文件数
	startTime := time.Now()

	// 创建任务通道（增大缓冲区，避免生产者阻塞）
	taskChan := make(chan string, concurrency*2)
	var wg sync.WaitGroup
	var mu sync.Mutex

	// 目录缓存（小文件场景优化：避免重复创建目录）
	dirCache := sync.Map{}

	// 启动进度更新协程（节流更新，避免高并发时频繁跳动）
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				updateProgress(atomic.LoadInt64(&copiedFiles), totalFiles, startTime)
			case <-progressDone:
				return
			}
		}
	}()

	// 启动工作协程
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for relPath := range taskChan {
				sourcePath := filepath.Join(sourceDir, relPath)
				destPath := filepath.Join(destDir, relPath)

				// 复制文件（移除 Stat 检查，直接尝试打开，减少系统调用）
				if err := copyFile(sourcePath, destPath, &dirCache); err != nil {
					// 区分文件不存在和其他错误
					if os.IsNotExist(err) {
						mu.Lock()
						fmt.Fprintf(os.Stderr, "警告: 源文件不存在: %s\n", sourcePath)
						mu.Unlock()
						atomic.AddInt64(&failedFiles, 1)
					} else {
						mu.Lock()
						fmt.Fprintf(os.Stderr, "警告: 复制文件失败 %s -> %s: %v\n", sourcePath, destPath, err)
						mu.Unlock()
						atomic.AddInt64(&failedFiles, 1)
					}
				}

				atomic.AddInt64(&copiedFiles, 1)
			}
		}()
	}

	// 发送任务
	for _, relPath := range fileList {
		taskChan <- relPath
	}
	close(taskChan)

	// 等待所有协程完成
	wg.Wait()

	// 停止进度更新协程
	close(progressDone)
	time.Sleep(120 * time.Millisecond) // 等待最后一次更新完成

	// 显示最终进度
	updateProgress(atomic.LoadInt64(&copiedFiles), totalFiles, startTime)

	if failedFiles > 0 {
		return fmt.Errorf("有 %d 个文件复制失败或源文件不存在", failedFiles)
	}

	return nil
}

// precreateDirectories 预创建所有需要的目录（小文件场景优化）
func precreateDirectories(baseDir string, fileList []string) error {
	dirSet := make(map[string]bool)
	for _, relPath := range fileList {
		dir := filepath.Dir(relPath)
		if dir != "." && dir != "" {
			dirSet[dir] = true
		}
	}

	// 按深度排序，确保父目录先创建（使用更高效的排序）
	dirs := make([]string, 0, len(dirSet))
	for dir := range dirSet {
		dirs = append(dirs, dir)
	}

	// 使用 sort.Slice 进行排序（按路径深度和字典序）
	// 这里使用简单的插入排序，因为目录数量通常不会太多
	for i := 1; i < len(dirs); i++ {
		key := dirs[i]
		j := i - 1
		for j >= 0 && (len(dirs[j]) > len(key) || (len(dirs[j]) == len(key) && dirs[j] > key)) {
			dirs[j+1] = dirs[j]
			j--
		}
		dirs[j+1] = key
	}

	// 创建所有目录（MkdirAll 本身会处理父目录，所以顺序不是严格必需的，但可以避免一些重复检查）
	for _, dir := range dirs {
		fullPath := filepath.Join(baseDir, dir)
		if err := os.MkdirAll(fullPath, 0755); err != nil {
			return fmt.Errorf("创建目录失败 %s: %w", fullPath, err)
		}
	}

	return nil
}

// copyFile 复制单个文件（小文件场景优化版本）
func copyFile(sourcePath, destPath string, dirCache *sync.Map) error {
	// 使用缓存检查目录是否已创建（小文件场景优化：减少重复的 MkdirAll 调用）
	destDir := filepath.Dir(destPath)
	if _, exists := dirCache.Load(destDir); !exists {
		// 双重检查，避免并发时重复创建
		if _, loaded := dirCache.LoadOrStore(destDir, true); !loaded {
			if err := os.MkdirAll(destDir, 0755); err != nil {
				dirCache.Delete(destDir) // 创建失败，移除缓存
				return fmt.Errorf("无法创建目标目录: %w", err)
			}
		}
	}

	// 打开源文件（移除 Stat 检查，直接打开以减少系统调用）
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("无法打开源文件: %w", err)
	}
	defer sourceFile.Close()

	// 创建目标文件
	destFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("无法创建目标文件: %w", err)
	}
	defer destFile.Close()

	// 为源文件添加缓冲读取（小文件场景优化：减少系统调用）
	bufferedReader := bufio.NewReaderSize(sourceFile, 64*1024)
	// 使用带缓冲的 Writer 提高 I/O 性能（64KB 缓冲区）
	bufferedWriter := bufio.NewWriterSize(destFile, 64*1024)
	defer bufferedWriter.Flush()

	// 复制文件内容
	_, err = io.Copy(bufferedWriter, bufferedReader)
	if err != nil {
		return fmt.Errorf("复制文件内容失败: %w", err)
	}

	// 注意：移除了每个文件的 Sync() 调用
	// Sync() 会强制等待数据写入磁盘，对于大量文件来说极其缓慢
	// 系统会在适当的时候自动刷新缓冲区，或者可以使用 --sync 选项在最后统一同步
	return nil
}

// updateProgress 更新进度条
func updateProgress(current, total int64, startTime time.Time) {
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

	fmt.Fprintf(os.Stdout, "\r进度: %d/%d (%.1f%%) | 速度: %.1f 文件/秒", current, total, percentage, filesPerSec)
	os.Stdout.Sync()
}
