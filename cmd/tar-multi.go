/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"archive/tar"
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

// tarMultiCmd 表示并行生成多个 tar 包的命令
var tarMultiCmd = &cobra.Command{
	Use:   "tar-multi <源目录> <目标目录>",
	Short: "并行生成多个 tar 包",
	Long: `将文件列表分成多份，并行生成多个 tar 包。

支持的功能：
- 自动在内存中生成 manifest 列表（如果未指定 manifest 文件）
- 根据 manifest 列表将文件分成多份，每个 tar 包包含不同的文件
- 并行处理多个 tar 包，每个 tar 包独立读取和打包分配给它的文件
- 在目标目录生成多个 tar 包和 manifest 文件
- 显示打包进度

示例：
  p-tool tar-multi /source /output
  p-tool tar-multi /source /output --count 10
  p-tool tar-multi /source /output --manifest-file /tmp/manifest.txt`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		sourceDir := args[0]
		outputDir := args[1]

		manifestFile, _ := cmd.Flags().GetString("manifest-file")
		tarCount, _ := cmd.Flags().GetInt("count")
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

		// 验证目标目录
		absOutputDir, err := filepath.Abs(outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法获取目标目录绝对路径: %v\n", err)
			os.Exit(1)
		}

		// 创建目标目录（如果不存在）
		if err := os.MkdirAll(absOutputDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法创建目标目录: %v\n", err)
			os.Exit(1)
		}

		var fileList []string

		// 如果未指定 manifest 文件，在内存中生成
		if manifestFile == "" {
			var err error
			fileList, err = GenerateManifestInMemory(absSourceDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "错误: 生成 manifest 失败: %v\n", err)
				os.Exit(1)
			}
		} else {
			// 读取 manifest 文件
			fileList, err = readManifest(manifestFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "错误: 读取 manifest 文件失败: %v\n", err)
				os.Exit(1)
			}
		}

		if len(fileList) == 0 {
			fmt.Fprintf(os.Stderr, "错误: manifest 文件为空\n")
			os.Exit(1)
		}

		// 设置 tar 包数量
		if tarCount <= 0 {
			tarCount = runtime.NumCPU()
		}
		if tarCount > len(fileList) {
			tarCount = len(fileList)
		}

		// 设置并发数
		if concurrency <= 0 {
			concurrency = runtime.NumCPU()
		}

		fmt.Fprintf(os.Stdout, "开始打包 %d 个文件到 %d 个 tar 包（每个 tar 包并发数: %d）...\n", len(fileList), tarCount, concurrency)

		// 将文件列表分成多份
		fileChunks := splitFileList(fileList, tarCount)

		// 并行生成多个 tar 包
		if err := createMultipleTarsParallel(absSourceDir, absOutputDir, fileChunks, concurrency); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 生成 tar 包失败: %v\n", err)
			os.Exit(1)
		}

		// 生成总 manifest 文件
		manifestPath := filepath.Join(absOutputDir, "manifest.txt")
		if err := writeManifestFile(manifestPath, fileList); err != nil {
			fmt.Fprintf(os.Stderr, "警告: 生成 manifest 文件失败: %v\n", err)
		} else {
			fmt.Fprintf(os.Stdout, "已生成 manifest 文件: %s\n", manifestPath)
		}

		fmt.Fprintf(os.Stdout, "\n打包完成！\n")
	},
}

func init() {
	rootCmd.AddCommand(tarMultiCmd)

	tarMultiCmd.Flags().String("manifest-file", "", "指定 manifest 文件路径（可选）")
	tarMultiCmd.Flags().Int("count", 0, "指定生成的 tar 包数量，默认为 CPU 核数")
	tarMultiCmd.Flags().Int("concurrency", 0, "指定每个 tar 包的并发数量，默认为 CPU 核数")
}

// splitFileList 将文件列表分成多份
func splitFileList(fileList []string, count int) [][]string {
	if count <= 0 {
		count = 1
	}
	if count > len(fileList) {
		count = len(fileList)
	}

	chunks := make([][]string, count)
	chunkSize := len(fileList) / count
	remainder := len(fileList) % count

	start := 0
	for i := 0; i < count; i++ {
		end := start + chunkSize
		if i < remainder {
			end++
		}
		chunks[i] = fileList[start:end]
		start = end
	}

	return chunks
}

// createMultipleTarsParallel 并行生成多个 tar 包
func createMultipleTarsParallel(sourceDir, outputDir string, fileChunks [][]string, concurrency int) error {
	var failedTars int64

	// 计算总文件数
	var totalFiles int64
	for _, chunk := range fileChunks {
		totalFiles += int64(len(chunk))
	}

	// 全局文件进度计数器
	var processedFiles int64

	startTime := time.Now()

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
				updateMultiTarProgress(atomic.LoadInt64(&processedFiles), totalFiles, startTime)
			case <-progressDone:
				return
			}
		}
	}()

	// 并行生成每个 tar 包
	for i, chunk := range fileChunks {
		wg.Add(1)
		go func(index int, files []string) {
			defer wg.Done()

			tarFileName := fmt.Sprintf("part-%04d.tar", index+1)
			tarFilePath := filepath.Join(outputDir, tarFileName)

			if err := createSingleTarParallel(sourceDir, tarFilePath, files, concurrency, &processedFiles); err != nil {
				mu.Lock()
				fmt.Fprintf(os.Stderr, "错误: 生成 tar 包 %s 失败: %v\n", tarFileName, err)
				mu.Unlock()
				atomic.AddInt64(&failedTars, 1)
			}
		}(i, chunk)
	}

	// 等待所有 tar 包生成完成
	wg.Wait()

	// 停止进度更新协程
	close(progressDone)
	time.Sleep(120 * time.Millisecond)

	// 显示最终进度
	updateMultiTarProgress(atomic.LoadInt64(&processedFiles), totalFiles, startTime)

	if failedTars > 0 {
		return fmt.Errorf("有 %d 个 tar 包生成失败", failedTars)
	}

	return nil
}

// createSingleTarParallel 并行读取文件并生成单个 tar 包
func createSingleTarParallel(sourceDir, outputFile string, fileList []string, concurrency int, globalProgress *int64) error {
	var processedFiles int64
	var failedFiles int64

	// 创建输出文件
	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("无法创建输出文件: %w", err)
	}
	defer outFile.Close()

	// 创建带缓冲的 writer 提高性能（增大缓冲区到 256KB）
	bufferedWriter := bufio.NewWriterSize(outFile, 256*1024)
	tarWriter := tar.NewWriter(bufferedWriter)
	defer func() {
		tarWriter.Close()
		bufferedWriter.Flush()
	}()

	// 创建任务通道
	taskChan := make(chan string, concurrency*2)

	var wg sync.WaitGroup
	var mu sync.Mutex // 保护 tarWriter 的并发写入

	// 启动文件处理工作协程（并行读取文件并流式写入 tar）
	var writeErr error
	var writeErrMu sync.Mutex
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for relPath := range taskChan {
				// 如果已经有写入错误，跳过后续处理
				writeErrMu.Lock()
				if writeErr != nil {
					writeErrMu.Unlock()
					atomic.AddInt64(&processedFiles, 1)
					if globalProgress != nil {
						atomic.AddInt64(globalProgress, 1)
					}
					continue
				}
				writeErrMu.Unlock()

				// 读取文件 header（不读内容）
				header, err := readFileHeaderForTar(sourceDir, relPath)
				if err != nil {
					mu.Lock()
					if os.IsNotExist(err) {
						fmt.Fprintf(os.Stderr, "警告: 源文件不存在: %s\n", filepath.Join(sourceDir, relPath))
					} else {
						fmt.Fprintf(os.Stderr, "警告: 读取文件失败 %s: %v\n", relPath, err)
					}
					mu.Unlock()
					atomic.AddInt64(&failedFiles, 1)
					atomic.AddInt64(&processedFiles, 1)
					if globalProgress != nil {
						atomic.AddInt64(globalProgress, 1)
					}
					continue
				}

				// 加锁保护 tarWriter（tar 格式要求串行写入）
				mu.Lock()
				// 再次检查错误
				writeErrMu.Lock()
				if writeErr != nil {
					writeErrMu.Unlock()
					mu.Unlock()
					atomic.AddInt64(&processedFiles, 1)
					if globalProgress != nil {
						atomic.AddInt64(globalProgress, 1)
					}
					continue
				}
				writeErrMu.Unlock()

				// 写入 tar header
				if err := tarWriter.WriteHeader(header); err != nil {
					writeErrMu.Lock()
					writeErr = fmt.Errorf("写入 tar header 失败 %s: %w", relPath, err)
					writeErrMu.Unlock()
					mu.Unlock()
					atomic.AddInt64(&processedFiles, 1)
					if globalProgress != nil {
						atomic.AddInt64(globalProgress, 1)
					}
					return
				}

				// 流式写入文件内容
				if err := writeFileContentToTar(sourceDir, relPath, tarWriter); err != nil {
					writeErrMu.Lock()
					writeErr = fmt.Errorf("写入文件内容失败 %s: %w", relPath, err)
					writeErrMu.Unlock()
					mu.Unlock()
					atomic.AddInt64(&processedFiles, 1)
					if globalProgress != nil {
						atomic.AddInt64(globalProgress, 1)
					}
					return
				}

				mu.Unlock()
				atomic.AddInt64(&processedFiles, 1)
				if globalProgress != nil {
					atomic.AddInt64(globalProgress, 1)
				}
			}
		}()
	}

	// 发送任务
	for _, relPath := range fileList {
		taskChan <- relPath
	}
	close(taskChan)

	// 等待所有工作协程完成
	wg.Wait()

	writeErrMu.Lock()
	err = writeErr
	writeErrMu.Unlock()
	if err != nil {
		return err
	}

	if failedFiles > 0 {
		return fmt.Errorf("有 %d 个文件处理失败或源文件不存在", failedFiles)
	}

	// 将 manifest 文件也写入 tar 包
	if err := writeManifestToTar(tarWriter, fileList); err != nil {
		return fmt.Errorf("写入 manifest 文件到 tar 包失败: %w", err)
	}

	return nil
}

// writeManifestFile 将文件列表写入 manifest 文件
func writeManifestFile(manifestPath string, fileList []string) error {
	manifestFile, err := os.Create(manifestPath)
	if err != nil {
		return fmt.Errorf("无法创建 manifest 文件: %w", err)
	}
	defer manifestFile.Close()

	for _, relPath := range fileList {
		// 确保路径使用斜杠格式，并添加 ./ 前缀
		formattedPath := filepath.ToSlash(relPath)
		if !strings.HasPrefix(formattedPath, "./") {
			formattedPath = "./" + formattedPath
		}
		if _, err := fmt.Fprintf(manifestFile, "%s\n", formattedPath); err != nil {
			return fmt.Errorf("写入 manifest 文件失败: %w", err)
		}
	}

	return nil
}

// updateMultiTarProgress 更新多 tar 包打包进度
func updateMultiTarProgress(current, total int64, startTime time.Time) {
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
