/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"

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

示例：
  p-tool untar-multi /output /dest
  p-tool untar-multi /output /dest --concurrency 8`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		sourceDir := args[0]
		destDir := args[1]

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
		tarFiles, err := findTarFiles(absSourceDir, useZstd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 查找 tar 包失败: %v\n", err)
			os.Exit(1)
		}

		if len(tarFiles) == 0 {
			if useZstd {
				fmt.Fprintf(os.Stderr, "错误: 在源目录中未找到 tar 包文件（part-*.tar.zst）\n")
			} else {
				fmt.Fprintf(os.Stderr, "错误: 在源目录中未找到 tar 包文件（part-*.tar）\n")
			}
			os.Exit(1)
		}

		fmt.Fprintf(os.Stdout, "找到 %d 个 tar 包，开始并行解压...\n", len(tarFiles))

		// 并行解压多个 tar 包
		if err := extractMultipleTarsParallel(absSourceDir, absDestDir, tarFiles, useZstd); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 解压 tar 包失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stdout, "\n解压完成！\n")
	},
}

func init() {
	rootCmd.AddCommand(untarMultiCmd)

	untarMultiCmd.Flags().Int("concurrency", 0, "保留参数（已弃用，系统 tar 命令不支持此参数）")
	untarMultiCmd.Flags().Bool("zstd", false, "解压缩经过 zstd 压缩的 tar 包")
}

// findTarFiles 查找源目录中的所有 part-*.tar 或 part-*.tar.zst 文件
func findTarFiles(sourceDir string, useZstd bool) ([]string, error) {
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("无法读取源目录: %w", err)
	}

	var tarFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "part-") {
			if useZstd {
				if strings.HasSuffix(entry.Name(), ".tar.zst") {
					tarFiles = append(tarFiles, entry.Name())
				}
			} else {
				if strings.HasSuffix(entry.Name(), ".tar") && !strings.HasSuffix(entry.Name(), ".tar.zst") {
					tarFiles = append(tarFiles, entry.Name())
				}
			}
		}
	}

	// 按文件名排序，确保顺序一致
	sort.Strings(tarFiles)

	return tarFiles, nil
}

// extractMultipleTarsParallel 并行解压多个 tar 包
func extractMultipleTarsParallel(sourceDir, destDir string, tarFiles []string, useZstd bool) error {
	var failedTars int
	var wg sync.WaitGroup
	var mu sync.Mutex

	// 并行解压每个 tar 包
	for _, tarFile := range tarFiles {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()

			tarFilePath := filepath.Join(sourceDir, filename)
			err := extractSingleTarWithSystemTar(tarFilePath, destDir, useZstd)
			if err != nil {
				mu.Lock()
				fmt.Fprintf(os.Stderr, "错误: 解压 tar 包 %s 失败: %v\n", filename, err)
				failedTars++
				mu.Unlock()
			}
		}(tarFile)
	}

	// 等待所有 tar 包解压完成
	wg.Wait()

	if failedTars > 0 {
		return fmt.Errorf("有 %d 个 tar 包解压失败", failedTars)
	}

	return nil
}

// extractSingleTarWithSystemTar 使用系统 tar 命令解压单个 tar 包
func extractSingleTarWithSystemTar(tarFilePath, destDir string, useZstd bool) error {
	// 获取 tar 文件的绝对路径
	absTarFilePath, err := filepath.Abs(tarFilePath)
	if err != nil {
		return fmt.Errorf("无法获取 tar 文件绝对路径: %w", err)
	}

	// 获取目标目录的绝对路径
	absDestDir, err := filepath.Abs(destDir)
	if err != nil {
		return fmt.Errorf("无法获取目标目录绝对路径: %w", err)
	}

	// 确保目标目录存在
	if err := os.MkdirAll(absDestDir, 0755); err != nil {
		return fmt.Errorf("无法创建目标目录: %w", err)
	}

	// 构建 tar 命令参数
	// tar -xf tarfile.tar -C destdir -k [--zstd]
	// 使用 -k 选项保持现有文件不被覆盖（处理多个 tar 包可能包含相同文件的情况）
	args := []string{"-xf", absTarFilePath, "-C", absDestDir, "-k"}
	if useZstd {
		args = append(args, "--zstd")
	}

	// 使用系统 tar 命令解压
	cmd := exec.Command("tar", args...)

	// 执行命令并捕获输出
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("tar 命令执行失败: %w, 输出: %s", err, string(output))
	}

	return nil
}
