#!/bin/bash
echo "=== 诊断 WITH_PARASTOR 问题 ==="
echo ""

# 1. 检查 Makefile 中的变量替换
echo "1. 检查 Makefile 中的 WITH_PARASTOR 变量替换:"
if [ -f "src/plugins/burst_buffer/Makefile" ]; then
    echo "   - 检查 @WITH_PARASTOR_TRUE@ 是否被替换:"
    if grep -q "@WITH_PARASTOR_TRUE@" src/plugins/burst_buffer/Makefile; then
        echo "     ❌ 错误: @WITH_PARASTOR_TRUE@ 没有被替换（变量未定义）"
        echo "     找到的内容:"
        grep "@WITH_PARASTOR_TRUE@" src/plugins/burst_buffer/Makefile | head -2
    else
        echo "     ✅ @WITH_PARASTOR_TRUE@ 已被替换"
        echo "     替换后的内容:"
        grep "am__append_2" src/plugins/burst_buffer/Makefile | head -2
    fi
    
    echo ""
    echo "   - 检查 SUBDIRS:"
    SUBDIRS_LINE=$(grep "^SUBDIRS" src/plugins/burst_buffer/Makefile | head -1)
    echo "     $SUBDIRS_LINE"
    if echo "$SUBDIRS_LINE" | grep -q "parastor"; then
        echo "     ⚠️  警告: SUBDIRS 包含 parastor"
    else
        echo "     ✅ SUBDIRS 不包含 parastor"
    fi
else
    echo "   ❌ Makefile 不存在，请先运行 ./configure"
fi

echo ""
echo "2. 检查 config.status 中的 WITH_PARASTOR 变量:"
if [ -f "config.status" ]; then
    if grep -q "WITH_PARASTOR_TRUE" config.status; then
        echo "   ✅ config.status 中包含 WITH_PARASTOR_TRUE"
        echo "     变量值:"
        grep "WITH_PARASTOR_TRUE" config.status | head -2
    else
        echo "   ❌ config.status 中不包含 WITH_PARASTOR_TRUE"
    fi
else
    echo "   ❌ config.status 不存在"
fi

echo ""
echo "3. 检查 config.log 中的 parastor 检测结果:"
if [ -f "config.log" ]; then
    echo "   - 检查 x_ac_have_parastor 的最终值:"
    grep "x_ac_have_parastor" config.log | tail -3
    
    echo ""
    echo "   - 检查是否检查了 curl 和 jansson:"
    if grep -q "checking for.*libcurl.*parastor" config.log; then
        echo "     ⚠️  警告: 检测到了 curl（不应该检测）"
        grep "checking for.*libcurl.*parastor" config.log | head -1
    else
        echo "     ✅ 没有检测 curl（正确）"
    fi
    
    if grep -q "checking for.*jansson.*parastor" config.log; then
        echo "     ⚠️  警告: 检测到了 jansson（不应该检测）"
        grep "checking for.*jansson.*parastor" config.log | head -1
    else
        echo "     ✅ 没有检测 jansson（正确）"
    fi
    
    echo ""
    echo "   - 检查 with_parastor 参数:"
    grep "with-parastor" config.log | head -3
else
    echo "   ❌ config.log 不存在"
fi

echo ""
echo "4. 检查 configure 脚本中的变量设置逻辑:"
if [ -f "configure" ]; then
    echo "   - 检查默认值设置:"
    if grep -A 2 "x_ac_parastor=no" configure | grep -q "else"; then
        echo "     ✅ 默认值设置为 no"
    else
        echo "     ❌ 默认值可能不是 no"
    fi
    
    echo ""
    echo "   - 检查条件判断:"
    if grep -q 'test "x$with_parastor" = "xno" || test -z "$with_parastor"' configure; then
        echo "     ✅ 条件判断正确"
    else
        echo "     ❌ 条件判断可能不正确"
    fi
fi

echo ""
echo "5. 检查实际生成的 Makefile 中的条件变量:"
if [ -f "src/plugins/burst_buffer/Makefile" ]; then
    echo "   - 查找所有 parastor 相关的内容:"
    grep -i "parastor" src/plugins/burst_buffer/Makefile | head -5
fi

echo ""
echo "=== 诊断完成 ==="
echo ""
echo "如果 @WITH_PARASTOR_TRUE@ 没有被替换，请检查："
echo "1. 是否使用了最新的 configure 脚本"
echo "2. 是否重新运行了 ./configure"
echo "3. config.status 中 WITH_PARASTOR_TRUE 的值是什么"

