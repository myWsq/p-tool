#!/bin/bash

# 压缩方式对比脚本
# 用法: ./compare-compression.sh <要打包的文件夹路径>
# 功能: 对比不同压缩方式的文件大小和耗时（包括压缩和解压缩）

# 检查参数
if [ $# -ne 1 ]; then
    echo "错误: 请提供一个参数 - 要打包的文件夹路径"
    echo "用法: $0 <文件夹路径>"
    exit 1
fi

SOURCE_DIR="$1"

# 检查目录是否存在
if [ ! -d "$SOURCE_DIR" ]; then
    echo "错误: 目录不存在: $SOURCE_DIR"
    exit 1
fi

# 获取目录的绝对路径和名称
SOURCE_DIR=$(cd "$SOURCE_DIR" && pwd)
DIR_NAME=$(basename "$SOURCE_DIR")
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "=========================================="
echo "压缩方式对比测试"
echo "=========================================="
echo "源目录: $SOURCE_DIR"
echo "临时目录: $TEMP_DIR"
echo ""

# 检查 p-tool 是否可用（优先使用 ./p-tool）
PTOOL_CMD=""
if [ -f "./p-tool" ] && [ -x "./p-tool" ]; then
    PTOOL_CMD="./p-tool"
    echo "使用本地 p-tool: ./p-tool"
elif command -v p-tool &> /dev/null; then
    PTOOL_CMD="p-tool"
    echo "使用系统 p-tool: p-tool"
else
    echo "警告: p-tool 未找到，将跳过 p-tool 相关测试"
    SKIP_PTOOL=true
fi

if [ -z "$PTOOL_CMD" ]; then
    SKIP_PTOOL=true
else
    SKIP_PTOOL=false
fi

# 检查系统 tar 是否支持 zstd
TAR_SUPPORTS_ZSTD=false
if tar --help 2>&1 | grep -q "zstd"; then
    TAR_SUPPORTS_ZSTD=true
fi

# 结果数组
declare -a RESULTS
declare -a METHODS
declare -a TIMES
declare -a COMPRESS_FILES
declare -a UNCOMPRESS_TIMES
declare -a USE_ZSTD_FLAGS

# 测试计数器
TEST_NUM=0
TOTAL_TESTS=0

# 计算总测试数
if [ "$SKIP_PTOOL" = false ]; then
    TOTAL_TESTS=$((TOTAL_TESTS + 2))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))  # 系统 tar (无压缩)
if [ "$TAR_SUPPORTS_ZSTD" = true ]; then
    TOTAL_TESTS=$((TOTAL_TESTS + 1))  # 系统 tar (zstd)
fi

# 函数：格式化文件大小
format_size() {
    local size=$1
    if [ $size -lt 1024 ]; then
        echo "${size}B"
    elif [ $size -lt 1048576 ]; then
        echo "$(awk "BEGIN {printf \"%.2f\", $size/1024}")KB"
    elif [ $size -lt 1073741824 ]; then
        echo "$(awk "BEGIN {printf \"%.2f\", $size/1048576}")MB"
    else
        echo "$(awk "BEGIN {printf \"%.2f\", $size/1073741824}")GB"
    fi
}

# 函数：获取文件大小
get_file_size() {
    if [ -f "$1" ]; then
        stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0
    else
        echo 0
    fi
}

# 函数：格式化时间（秒）
format_time() {
    local seconds=$1
    # 使用 awk 进行浮点数比较
    if awk "BEGIN {exit !($seconds < 1)}" 2>/dev/null; then
        # 小于1秒，显示毫秒
        local ms=$(awk "BEGIN {printf \"%.0f\", $seconds * 1000}")
        echo "${ms}ms"
    elif awk "BEGIN {exit !($seconds < 60)}" 2>/dev/null; then
        # 小于60秒，显示秒（保留2位小数）
        echo "$(awk "BEGIN {printf \"%.2f\", $seconds}")s"
    else
        # 大于60秒，显示分钟和秒
        local mins=$(awk "BEGIN {printf \"%.0f\", int($seconds / 60)}")
        local secs=$(awk "BEGIN {printf \"%.2f\", $seconds % 60}")
        echo "${mins}m${secs}s"
    fi
}

# 函数：获取当前时间戳（秒，带小数）
get_timestamp() {
    # 优先使用 python3，因为它能提供高精度时间戳
    if command -v python3 &> /dev/null; then
        python3 -c "import time; print(time.time())" 2>/dev/null
    # 其次尝试 gdate（GNU date，支持纳秒）
    elif command -v gdate &> /dev/null; then
        gdate +%s.%N 2>/dev/null
    # 再次尝试 date +%s.%N（某些系统支持）
    elif date +%s.%N 2>/dev/null | grep -q '\.'; then
        date +%s.%N 2>/dev/null
    # 最后使用 date +%s（只有秒精度）
    else
        date +%s
    fi
}

# 测试 1: p-tool tar (不加 zstd)
if [ "$SKIP_PTOOL" = false ]; then
    TEST_NUM=$((TEST_NUM + 1))
    echo "[$TEST_NUM/$TOTAL_TESTS] 测试 p-tool tar (不加 zstd)..."
    OUTPUT_FILE="$TEMP_DIR/ptool-tar.tar"
    START_TIME=$(get_timestamp)
    if $PTOOL_CMD tar "$SOURCE_DIR" "$OUTPUT_FILE" 2>/dev/null; then
        END_TIME=$(get_timestamp)
        ELAPSED=$(awk "BEGIN {printf \"%.3f\", $END_TIME - $START_TIME}")
        SIZE=$(get_file_size "$OUTPUT_FILE")
        RESULTS+=("$SIZE")
        TIMES+=("$ELAPSED")
        COMPRESS_FILES+=("$OUTPUT_FILE")
        USE_ZSTD_FLAGS+=("false")
        METHODS+=("p-tool tar (无压缩)")
        echo "  ✓ 压缩完成: $(format_size $SIZE) | 耗时: $(format_time $ELAPSED)"
        
        # 测试解压缩
        echo "  测试解压缩..."
        EXTRACT_DIR="$TEMP_DIR/extract-ptool-tar"
        mkdir -p "$EXTRACT_DIR"
        START_TIME=$(get_timestamp)
        if $PTOOL_CMD untar "$OUTPUT_FILE" "$EXTRACT_DIR" 2>/dev/null; then
            END_TIME=$(get_timestamp)
            UNCOMPRESS_ELAPSED=$(awk "BEGIN {printf \"%.3f\", $END_TIME - $START_TIME}")
            UNCOMPRESS_TIMES+=("$UNCOMPRESS_ELAPSED")
            echo "  ✓ 解压完成: 耗时: $(format_time $UNCOMPRESS_ELAPSED)"
        else
            UNCOMPRESS_TIMES+=("0")
            echo "  ✗ 解压失败"
        fi
        rm -rf "$EXTRACT_DIR"
    else
        RESULTS+=("0")
        TIMES+=("0")
        COMPRESS_FILES+=("")
        USE_ZSTD_FLAGS+=("false")
        UNCOMPRESS_TIMES+=("0")
        METHODS+=("p-tool tar (无压缩)")
        echo "  ✗ 失败"
    fi
    echo ""
fi

# 测试 2: p-tool tar (加 zstd)
if [ "$SKIP_PTOOL" = false ]; then
    TEST_NUM=$((TEST_NUM + 1))
    echo "[$TEST_NUM/$TOTAL_TESTS] 测试 p-tool tar (加 zstd)..."
    OUTPUT_FILE="$TEMP_DIR/ptool-tar-zstd.tar.zst"
    START_TIME=$(get_timestamp)
    if $PTOOL_CMD tar "$SOURCE_DIR" "$OUTPUT_FILE" --zstd 2>/dev/null; then
        END_TIME=$(get_timestamp)
        ELAPSED=$(awk "BEGIN {printf \"%.3f\", $END_TIME - $START_TIME}")
        SIZE=$(get_file_size "$OUTPUT_FILE")
        RESULTS+=("$SIZE")
        TIMES+=("$ELAPSED")
        COMPRESS_FILES+=("$OUTPUT_FILE")
        USE_ZSTD_FLAGS+=("true")
        METHODS+=("p-tool tar (zstd)")
        echo "  ✓ 压缩完成: $(format_size $SIZE) | 耗时: $(format_time $ELAPSED)"
        
        # 测试解压缩
        echo "  测试解压缩..."
        EXTRACT_DIR="$TEMP_DIR/extract-ptool-tar-zstd"
        mkdir -p "$EXTRACT_DIR"
        START_TIME=$(get_timestamp)
        if $PTOOL_CMD untar "$OUTPUT_FILE" "$EXTRACT_DIR" --zstd 2>/dev/null; then
            END_TIME=$(get_timestamp)
            UNCOMPRESS_ELAPSED=$(awk "BEGIN {printf \"%.3f\", $END_TIME - $START_TIME}")
            UNCOMPRESS_TIMES+=("$UNCOMPRESS_ELAPSED")
            echo "  ✓ 解压完成: 耗时: $(format_time $UNCOMPRESS_ELAPSED)"
        else
            UNCOMPRESS_TIMES+=("0")
            echo "  ✗ 解压失败"
        fi
        rm -rf "$EXTRACT_DIR"
    else
        RESULTS+=("0")
        TIMES+=("0")
        COMPRESS_FILES+=("")
        USE_ZSTD_FLAGS+=("false")
        UNCOMPRESS_TIMES+=("0")
        METHODS+=("p-tool tar (zstd)")
        echo "  ✗ 失败"
    fi
    echo ""
fi

# 测试 3: 系统 tar (不加压缩)
TEST_NUM=$((TEST_NUM + 1))
echo "[$TEST_NUM/$TOTAL_TESTS] 测试系统 tar (不加压缩)..."
OUTPUT_FILE="$TEMP_DIR/system-tar.tar"
START_TIME=$(get_timestamp)
if tar -cf "$OUTPUT_FILE" -C "$(dirname "$SOURCE_DIR")" "$DIR_NAME" 2>/dev/null; then
    END_TIME=$(get_timestamp)
    ELAPSED=$(awk "BEGIN {printf \"%.3f\", $END_TIME - $START_TIME}")
    SIZE=$(get_file_size "$OUTPUT_FILE")
    RESULTS+=("$SIZE")
    TIMES+=("$ELAPSED")
    COMPRESS_FILES+=("$OUTPUT_FILE")
    USE_ZSTD_FLAGS+=("false")
    METHODS+=("系统 tar (无压缩)")
    echo "  ✓ 压缩完成: $(format_size $SIZE) | 耗时: $(format_time $ELAPSED)"
    
    # 测试解压缩
    echo "  测试解压缩..."
    EXTRACT_DIR="$TEMP_DIR/extract-system-tar"
    mkdir -p "$EXTRACT_DIR"
    START_TIME=$(get_timestamp)
    if tar -xf "$OUTPUT_FILE" -C "$EXTRACT_DIR" 2>/dev/null; then
        END_TIME=$(get_timestamp)
        UNCOMPRESS_ELAPSED=$(awk "BEGIN {printf \"%.3f\", $END_TIME - $START_TIME}")
        UNCOMPRESS_TIMES+=("$UNCOMPRESS_ELAPSED")
        echo "  ✓ 解压完成: 耗时: $(format_time $UNCOMPRESS_ELAPSED)"
    else
        UNCOMPRESS_TIMES+=("0")
        echo "  ✗ 解压失败"
    fi
    rm -rf "$EXTRACT_DIR"
else
    RESULTS+=("0")
    TIMES+=("0")
    COMPRESS_FILES+=("")
    USE_ZSTD_FLAGS+=("false")
    UNCOMPRESS_TIMES+=("0")
    METHODS+=("系统 tar (无压缩)")
    echo "  ✗ 失败"
fi
echo ""

# 测试 4: 系统 tar (zstd 压缩，如果支持)
if [ "$TAR_SUPPORTS_ZSTD" = true ]; then
    TEST_NUM=$((TEST_NUM + 1))
    echo "[$TEST_NUM/$TOTAL_TESTS] 测试系统 tar (zstd 压缩)..."
    OUTPUT_FILE="$TEMP_DIR/system-tar-zstd.tar.zst"
    START_TIME=$(get_timestamp)
    if tar --zstd -cf "$OUTPUT_FILE" -C "$(dirname "$SOURCE_DIR")" "$DIR_NAME" 2>/dev/null; then
        END_TIME=$(get_timestamp)
        ELAPSED=$(awk "BEGIN {printf \"%.3f\", $END_TIME - $START_TIME}")
        SIZE=$(get_file_size "$OUTPUT_FILE")
        RESULTS+=("$SIZE")
        TIMES+=("$ELAPSED")
        COMPRESS_FILES+=("$OUTPUT_FILE")
        USE_ZSTD_FLAGS+=("true")
        METHODS+=("系统 tar (zstd)")
        echo "  ✓ 压缩完成: $(format_size $SIZE) | 耗时: $(format_time $ELAPSED)"
        
        # 测试解压缩
        echo "  测试解压缩..."
        EXTRACT_DIR="$TEMP_DIR/extract-system-tar-zstd"
        mkdir -p "$EXTRACT_DIR"
        START_TIME=$(get_timestamp)
        if tar --zstd -xf "$OUTPUT_FILE" -C "$EXTRACT_DIR" 2>/dev/null; then
            END_TIME=$(get_timestamp)
            UNCOMPRESS_ELAPSED=$(awk "BEGIN {printf \"%.3f\", $END_TIME - $START_TIME}")
            UNCOMPRESS_TIMES+=("$UNCOMPRESS_ELAPSED")
            echo "  ✓ 解压完成: 耗时: $(format_time $UNCOMPRESS_ELAPSED)"
        else
            UNCOMPRESS_TIMES+=("0")
            echo "  ✗ 解压失败"
        fi
        rm -rf "$EXTRACT_DIR"
    else
        RESULTS+=("0")
        TIMES+=("0")
        COMPRESS_FILES+=("")
        USE_ZSTD_FLAGS+=("false")
        UNCOMPRESS_TIMES+=("0")
        METHODS+=("系统 tar (zstd)")
        echo "  ✗ 失败"
    fi
    echo ""
else
    echo "跳过系统 tar (zstd) - 系统 tar 不支持 zstd"
    echo ""
fi

# 显示对比结果
echo "=========================================="
echo "对比结果"
echo "=========================================="
printf "%-30s %18s %18s %18s\n" "压缩方式" "文件大小" "压缩耗时" "解压耗时"
echo "------------------------------------------"

# 找到最小的文件大小和最快的时间作为基准
MIN_SIZE=0
MIN_COMPRESS_TIME=0
MIN_UNCOMPRESS_TIME=0
for i in "${!RESULTS[@]}"; do
    size="${RESULTS[$i]}"
    compress_time="${TIMES[$i]}"
    uncompress_time="${UNCOMPRESS_TIMES[$i]}"
    # 使用 awk 进行浮点数比较
    if [ $size -gt 0 ] && awk "BEGIN {exit !($compress_time > 0)}" 2>/dev/null; then
        if [ $MIN_SIZE -eq 0 ] || [ $size -lt $MIN_SIZE ]; then
            MIN_SIZE=$size
        fi
        if awk "BEGIN {exit !($MIN_COMPRESS_TIME == 0 || $compress_time < $MIN_COMPRESS_TIME)}" 2>/dev/null; then
            MIN_COMPRESS_TIME=$compress_time
        fi
    fi
    if awk "BEGIN {exit !($uncompress_time > 0)}" 2>/dev/null; then
        if awk "BEGIN {exit !($MIN_UNCOMPRESS_TIME == 0 || $uncompress_time < $MIN_UNCOMPRESS_TIME)}" 2>/dev/null; then
            MIN_UNCOMPRESS_TIME=$uncompress_time
        fi
    fi
done

# 显示结果
for i in "${!METHODS[@]}"; do
    SIZE="${RESULTS[$i]}"
    COMPRESS_TIME="${TIMES[$i]}"
    UNCOMPRESS_TIME="${UNCOMPRESS_TIMES[$i]}"
    METHOD="${METHODS[$i]}"
    
    if [ $SIZE -gt 0 ] && awk "BEGIN {exit !($COMPRESS_TIME > 0)}" 2>/dev/null; then
        SIZE_FORMATTED=$(format_size $SIZE)
        COMPRESS_TIME_FORMATTED=$(format_time $COMPRESS_TIME)
        
        # 构建大小标记
        SIZE_MARK=""
        if [ $MIN_SIZE -gt 0 ] && [ $SIZE -gt $MIN_SIZE ]; then
            RATIO=$(awk "BEGIN {printf \"%.1f\", ($SIZE - $MIN_SIZE) * 100 / $MIN_SIZE}")
            SIZE_MARK=" (+${RATIO}%)"
        elif [ $SIZE -eq $MIN_SIZE ]; then
            SIZE_MARK=" [最小]"
        fi
        
        # 构建压缩时间标记
        COMPRESS_TIME_MARK=""
        if awk "BEGIN {exit !($MIN_COMPRESS_TIME > 0 && $COMPRESS_TIME > $MIN_COMPRESS_TIME)}" 2>/dev/null; then
            TIME_RATIO=$(awk "BEGIN {printf \"%.1f\", ($COMPRESS_TIME - $MIN_COMPRESS_TIME) * 100 / $MIN_COMPRESS_TIME}")
            COMPRESS_TIME_MARK=" (+${TIME_RATIO}%)"
        elif awk "BEGIN {exit !($COMPRESS_TIME == $MIN_COMPRESS_TIME)}" 2>/dev/null; then
            COMPRESS_TIME_MARK=" [最快]"
        fi
        
        # 构建解压时间标记
        UNCOMPRESS_TIME_FORMATTED=""
        UNCOMPRESS_TIME_MARK=""
        if awk "BEGIN {exit !($UNCOMPRESS_TIME > 0)}" 2>/dev/null; then
            UNCOMPRESS_TIME_FORMATTED=$(format_time $UNCOMPRESS_TIME)
            if awk "BEGIN {exit !($MIN_UNCOMPRESS_TIME > 0 && $UNCOMPRESS_TIME > $MIN_UNCOMPRESS_TIME)}" 2>/dev/null; then
                TIME_RATIO=$(awk "BEGIN {printf \"%.1f\", ($UNCOMPRESS_TIME - $MIN_UNCOMPRESS_TIME) * 100 / $MIN_UNCOMPRESS_TIME}")
                UNCOMPRESS_TIME_MARK=" (+${TIME_RATIO}%)"
            elif awk "BEGIN {exit !($UNCOMPRESS_TIME == $MIN_UNCOMPRESS_TIME)}" 2>/dev/null; then
                UNCOMPRESS_TIME_MARK=" [最快]"
            fi
        else
            UNCOMPRESS_TIME_FORMATTED="-"
        fi
        
        printf "%-30s %-18s %-18s %-18s\n" "$METHOD" \
            "${SIZE_FORMATTED}${SIZE_MARK}" \
            "${COMPRESS_TIME_FORMATTED}${COMPRESS_TIME_MARK}" \
            "${UNCOMPRESS_TIME_FORMATTED}${UNCOMPRESS_TIME_MARK}"
    else
        printf "%-30s %-18s %-18s %-18s\n" "$METHOD" "失败" "-" "-"
    fi
done

echo "=========================================="

