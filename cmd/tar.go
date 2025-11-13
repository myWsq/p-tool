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

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

// tarCmd represents the tar command
var tarCmd = &cobra.Command{
	Use:   "tar <源目录> <输出tar文件>",
	Short: "并行生成 tar 包",
	Long: `根据 manifest 文件并行读取文件并生成 tar 包。

支持的功能：
- 自动在内存中生成 manifest 列表（如果未指定 manifest 文件）
- 并行读取文件，提高打包速度
- 显示打包进度

示例：
  p-tool tar /source output.tar
  p-tool tar /source output.tar --manifest-file /tmp/manifest.txt
  p-tool tar /source output.tar --concurrency 8`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		sourceDir := args[0]
		outputFile := args[1]

		manifestFile, _ := cmd.Flags().GetString("manifest-file")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		useZstd, _ := cmd.Flags().GetBool("zstd")

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

		// 设置并发数
		if concurrency <= 0 {
			concurrency = runtime.NumCPU()
		}

		fmt.Fprintf(os.Stdout, "开始打包 %d 个文件（并发数: %d）...\n", len(fileList), concurrency)

		// 并行生成 tar 包
		if err := createTarParallel(absSourceDir, outputFile, fileList, concurrency, useZstd); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 生成 tar 包失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stdout, "\n打包完成！\n")
	},
}

func init() {
	rootCmd.AddCommand(tarCmd)

	tarCmd.Flags().String("manifest-file", "", "指定 manifest 文件路径（可选）")
	tarCmd.Flags().Int("concurrency", 0, "指定并发数量，默认为 CPU 核数")
	tarCmd.Flags().Bool("zstd", false, "使用 zstd 算法压缩 tar 包")
}

// tarBufferPool 缓冲区池，用于复用缓冲区减少内存分配
var tarBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 256*1024)
		return &buf
	},
}

// createTarParallel 并行读取文件并生成 tar 包
func createTarParallel(sourceDir, outputFile string, fileList []string, concurrency int, useZstd bool) error {
	totalFiles := int64(len(fileList))
	var processedFiles int64
	var failedFiles int64

	// 记录开始时间
	startTime := time.Now()

	// 创建输出文件
	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("无法创建输出文件: %w", err)
	}
	defer outFile.Close()

	// 创建带缓冲的 writer 提高性能（增大缓冲区到 256KB）
	bufferedWriter := bufio.NewWriterSize(outFile, 256*1024)

	// 根据 useZstd 标志决定是否使用 zstd 压缩
	var writer io.Writer = bufferedWriter
	var zstdEncoder *zstd.Encoder
	if useZstd {
		var err error
		zstdEncoder, err = zstd.NewWriter(bufferedWriter, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(6)))
		if err != nil {
			return fmt.Errorf("创建 zstd 编码器失败: %w", err)
		}
		writer = zstdEncoder
	}

	tarWriter := tar.NewWriter(writer)
	defer func() {
		// 按顺序关闭：先关闭 tarWriter，再关闭 zstdEncoder，最后 flush buffer
		tarWriter.Close()
		if zstdEncoder != nil {
			zstdEncoder.Close()
		}
		bufferedWriter.Flush()
	}()

	// 创建任务通道
	taskChan := make(chan string, concurrency*2)

	var wg sync.WaitGroup
	var mu sync.Mutex // 保护 tarWriter 的并发写入

	// 启动进度更新协程
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				updateTarProgress(atomic.LoadInt64(&processedFiles), totalFiles, startTime)
			case <-progressDone:
				return
			}
		}
	}()

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
					return
				}

				// 流式写入文件内容
				if err := writeFileContentToTar(sourceDir, relPath, tarWriter); err != nil {
					writeErrMu.Lock()
					writeErr = fmt.Errorf("写入文件内容失败 %s: %w", relPath, err)
					writeErrMu.Unlock()
					mu.Unlock()
					atomic.AddInt64(&processedFiles, 1)
					return
				}

				mu.Unlock()
				atomic.AddInt64(&processedFiles, 1)
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

	// 停止进度更新协程
	close(progressDone)
	time.Sleep(120 * time.Millisecond)

	// 显示最终进度
	updateTarProgress(atomic.LoadInt64(&processedFiles), totalFiles, startTime)

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

// readFileHeaderForTar 读取文件信息并创建 tar header（不读文件内容）
func readFileHeaderForTar(sourceDir, relPath string) (*tar.Header, error) {
	fullPath := filepath.Join(sourceDir, relPath)

	// 获取文件信息
	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}

	// 创建 tar header
	header, err := tar.FileInfoHeader(fileInfo, "")
	if err != nil {
		return nil, fmt.Errorf("创建 tar header 失败: %w", err)
	}

	// 设置文件名（使用相对路径，确保路径使用斜杠）
	header.Name = filepath.ToSlash(relPath)

	return header, nil
}

// writeFileContentToTar 流式写入文件内容到 tar（优化内存占用）
func writeFileContentToTar(sourceDir, relPath string, tarWriter *tar.Writer) error {
	fullPath := filepath.Join(sourceDir, relPath)

	// 打开文件
	file, err := os.Open(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 从缓冲区池获取缓冲区
	bufPtr := tarBufferPool.Get().(*[]byte)
	defer tarBufferPool.Put(bufPtr)
	buf := *bufPtr

	// 使用流式复制，避免将整个文件读入内存
	_, err = io.CopyBuffer(tarWriter, file, buf)
	if err != nil {
		return fmt.Errorf("流式写入文件内容失败: %w", err)
	}

	return nil
}

// writeManifestToTar 将 manifest 文件写入 tar 包
func writeManifestToTar(tarWriter *tar.Writer, fileList []string) error {
	// manifest 文件使用特殊名称，便于解压时识别
	manifestName := ".__p-tool-manifest__.txt"

	// 生成 manifest 内容（每行一个文件路径，格式为 ./relative/path）
	var manifestContent strings.Builder
	for _, relPath := range fileList {
		// 确保路径使用斜杠格式，并添加 ./ 前缀
		formattedPath := filepath.ToSlash(relPath)
		if !strings.HasPrefix(formattedPath, "./") {
			formattedPath = "./" + formattedPath
		}
		manifestContent.WriteString(formattedPath)
		manifestContent.WriteString("\n")
	}

	content := []byte(manifestContent.String())

	// 创建 manifest 文件的 tar header
	header := &tar.Header{
		Name:     manifestName,
		Size:     int64(len(content)),
		Mode:     0644,
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}

	// 写入 header
	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("写入 manifest header 失败: %w", err)
	}

	// 写入内容
	if _, err := tarWriter.Write(content); err != nil {
		return fmt.Errorf("写入 manifest 内容失败: %w", err)
	}

	return nil
}

// updateTarProgress 更新打包进度
func updateTarProgress(current, total int64, startTime time.Time) {
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
