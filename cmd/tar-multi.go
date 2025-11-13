/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

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

		// 将文件列表分成多份
		fileChunks := splitFileList(fileList, tarCount)

		// 并行生成多个 tar 包
		if err := createMultipleTarsParallel(absSourceDir, absOutputDir, fileChunks, useZstd); err != nil {
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
	tarMultiCmd.Flags().Int("concurrency", 0, "保留参数（已弃用，系统 tar 命令不支持此参数）")
	tarMultiCmd.Flags().Bool("zstd", false, "使用 zstd 算法压缩 tar 包")
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
func createMultipleTarsParallel(sourceDir, outputDir string, fileChunks [][]string, useZstd bool) error {
	var failedTars int
	var wg sync.WaitGroup
	var mu sync.Mutex

	// 并行生成每个 tar 包
	for i, chunk := range fileChunks {
		wg.Add(1)
		go func(index int, files []string) {
			defer wg.Done()

			var tarFileName string
			if useZstd {
				tarFileName = fmt.Sprintf("part-%04d.tar.zst", index+1)
			} else {
				tarFileName = fmt.Sprintf("part-%04d.tar", index+1)
			}
			tarFilePath := filepath.Join(outputDir, tarFileName)

			if err := createSingleTarWithSystemTar(sourceDir, tarFilePath, files, useZstd); err != nil {
				mu.Lock()
				fmt.Fprintf(os.Stderr, "错误: 生成 tar 包 %s 失败: %v\n", tarFileName, err)
				failedTars++
				mu.Unlock()
			}
		}(i, chunk)
	}

	// 等待所有 tar 包生成完成
	wg.Wait()

	if failedTars > 0 {
		return fmt.Errorf("有 %d 个 tar 包生成失败", failedTars)
	}

	return nil
}

// createSingleTarWithSystemTar 使用系统 tar 命令生成单个 tar 包
func createSingleTarWithSystemTar(sourceDir, outputFile string, fileList []string, useZstd bool) error {
	if len(fileList) == 0 {
		return nil
	}

	// 创建临时 manifest 文件
	tmpManifest, err := os.CreateTemp("", "p-tool-tar-manifest-*.txt")
	if err != nil {
		return fmt.Errorf("无法创建临时 manifest 文件: %w", err)
	}
	defer os.Remove(tmpManifest.Name())
	defer tmpManifest.Close()

	// 写入文件列表到临时 manifest（使用相对路径，格式为 ./path）
	for _, relPath := range fileList {
		formattedPath := filepath.ToSlash(relPath)
		if !strings.HasPrefix(formattedPath, "./") {
			formattedPath = "./" + formattedPath
		}
		if _, err := fmt.Fprintf(tmpManifest, "%s\n", formattedPath); err != nil {
			return fmt.Errorf("写入临时 manifest 文件失败: %w", err)
		}
	}

	// 确保文件已写入磁盘
	if err := tmpManifest.Sync(); err != nil {
		return fmt.Errorf("同步临时 manifest 文件失败: %w", err)
	}
	tmpManifest.Close()

	// 获取输出文件的绝对路径
	absOutputFile, err := filepath.Abs(outputFile)
	if err != nil {
		return fmt.Errorf("无法获取输出文件绝对路径: %w", err)
	}

	// 构建 tar 命令参数
	args := []string{"-cf", absOutputFile, "-T", tmpManifest.Name()}
	if useZstd {
		args = append(args, "--zstd")
	}

	// 切换到源目录执行 tar 命令
	// tar -cf output.tar -T manifest.txt [--zstd]
	cmd := exec.Command("tar", args...)
	cmd.Dir = sourceDir

	// 执行命令并捕获输出
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("tar 命令执行失败: %w, 输出: %s", err, string(output))
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
