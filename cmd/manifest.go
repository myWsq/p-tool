/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// GenerateManifest 扫描指定目录并生成 manifest 文件
// dirPath: 要扫描的目录路径（可以是相对路径或绝对路径）
// manifestPath: manifest 文件的输出路径
func GenerateManifest(dirPath, manifestPath string) error {
	// 创建 manifest 文件
	manifestFile, err := os.Create(manifestPath)
	if err != nil {
		return fmt.Errorf("无法创建 manifest 文件: %w", err)
	}
	defer manifestFile.Close()

	// 获取目录的绝对路径，用于计算相对路径
	absDirPath, err := filepath.Abs(dirPath)
	if err != nil {
		return fmt.Errorf("无法获取目录绝对路径: %w", err)
	}

	// 用于跟踪已访问的路径（解析后的真实路径），防止无限递归
	visited := make(map[string]bool)

	// 自定义的 walk 函数，支持跟随符号链接
	var walkDir func(string, string) error
	walkDir = func(currentPath, realPath string) error {
		// 获取当前路径的绝对路径（未解析符号链接的路径）
		absCurrentPath, err := filepath.Abs(currentPath)
		if err != nil {
			// 如果无法获取绝对路径，跳过
			return nil
		}

		// 解析符号链接获取真实路径
		absRealPath, err := filepath.EvalSymlinks(realPath)
		if err != nil {
			// 如果无法解析符号链接，尝试使用原始路径
			absRealPath, err = filepath.Abs(realPath)
			if err != nil {
				// 如果无法获取绝对路径，跳过
				return nil
			}
		}

		// 检查是否已访问过（防止无限递归）
		if visited[absRealPath] {
			return nil
		}

		// 标记为已访问
		visited[absRealPath] = true

		// 获取文件信息
		info, err := os.Stat(absRealPath)
		if err != nil {
			// 如果文件不存在或无法访问（如断开的符号链接），跳过
			return nil
		}

		// 如果是文件（非目录），记录到 manifest
		if !info.IsDir() {
			// 计算相对路径（使用原始路径，保持符号链接的结构）
			relPath, err := filepath.Rel(absDirPath, absCurrentPath)
			if err != nil {
				// 如果无法计算相对路径，跳过
				return nil
			}
			// 写入文件，格式为 ./relative/path
			_, err = fmt.Fprintf(manifestFile, "./%s\n", filepath.ToSlash(relPath))
			if err != nil {
				return err
			}
			return nil
		}

		// 如果是目录，继续遍历
		entries, err := os.ReadDir(absRealPath)
		if err != nil {
			// 如果无法读取目录（如权限问题），跳过
			return nil
		}

		for _, entry := range entries {
			// 构建子路径
			entryCurrentPath := filepath.Join(currentPath, entry.Name())
			entryRealPath := filepath.Join(absRealPath, entry.Name())

			// 递归遍历
			if err := walkDir(entryCurrentPath, entryRealPath); err != nil {
				// 如果递归过程中出现错误，继续处理其他文件
				// 不中断整个遍历过程
				continue
			}
		}

		return nil
	}

	// 开始遍历
	err = walkDir(dirPath, dirPath)
	if err != nil {
		return fmt.Errorf("扫描目录时出错: %w", err)
	}

	return nil
}

// manifestCmd represents the manifest command
var manifestCmd = &cobra.Command{
	Use:   "manifest <目录路径> <manifest文件路径>",
	Short: "生成用于加速并行命令的 manifest 文件",
	Long: `扫描指定目录并生成一个 manifest 文件，文件中每一行都是该目录下文件的相对路径。
例如：manifest /root /tmp/manifest.txt`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		dirPath := args[0]
		manifestPath := args[1]

		// 验证目录是否存在
		dirInfo, err := os.Stat(dirPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法访问目录 %s: %v\n", dirPath, err)
			os.Exit(1)
		}
		if !dirInfo.IsDir() {
			fmt.Fprintf(os.Stderr, "错误: %s 不是一个目录\n", dirPath)
			os.Exit(1)
		}

		// 使用共享函数生成 manifest
		if err := GenerateManifest(dirPath, manifestPath); err != nil {
			fmt.Fprintf(os.Stderr, "错误: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("成功生成 manifest 文件: %s\n", manifestPath)
	},
}

func init() {
	rootCmd.AddCommand(manifestCmd)
}
