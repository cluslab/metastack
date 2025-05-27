#!/bin/bash

# 安装目录

# 客户端插件目录
clinte_dir="/opt/gridview/slurm/etc"

# 预测工具目录
predictor_dir="/opt/gridview/slurm/etc/luaconfig/predictor"

# python环境目录
ai_environment_dir="$predictor_dir/AIenvironment"

# AI环境目录
sklearn_dir="$predictor_dir/prediction_methods/sklearn"

# 要安装的文件
# 预测工具文件
FILES=("predictUsers" "predictionTool.lua" "getParameter.lua" "README.md" "updateJobHistory.sh" "configuration" "jobHistory")

# AI随机森林文件
SKLEARN_FILES=("prediction_methods/sklearn/generative_model.py" "prediction_methods/sklearn/prediction_time.py" "prediction_methods/sklearn/update_model.sh")

# 获取当前日期
current_date=$(date +%Y-%m-%d)

# 创建安装目录
create_install_dir() {

	echo "创建客户端插件目录 $clinte_dir..."
	mkdir -p "$clinte_dir"

	echo "创建预测工具目录 $predictor_dir..."
	mkdir -p "$predictor_dir"
    
	echo "创建python环境目录 $ai_environment_dir..."
	mkdir -p "$ai_environment_dir"
    
	echo "创建AI环境目录 $sklearn_dir..."
	mkdir -p "$sklearn_dir"
    
	echo "安装目录创建完成。"
}

# 检查并备份文件
backup_if_exists() {
    
	local target_file=$1

	# 检查文件是否已存在，若已存在则备份
	if [[ -f "$target_file" ]]; then
		echo "文件 $target_file 已存在，备份为 $target_file.$current_date"
		mv "$target_file" "$target_file.$current_date"
	fi
}

# 安装文件
install_files() {

	echo "开始安装文件到 $clinte_dir..."

	# 备份原文件
	cli_target="$clinte_dir/cli_filter.lua"
	backup_if_exists "$cli_target"

	# 安装新文件
	install -m 755 "cli_filter.lua" "$cli_target"
	echo "安装文件 cli_filter.lua 到 $clinte_dir 已完成"
    
	echo "开始安装文件到 $predictor_dir..."
    
	for file in "${FILES[@]}"; do

		# 备份原文件
		predictor_target="$predictor_dir/$(basename "$file")"
		backup_if_exists "$predictor_target"

		# 安装新文件
		install -m 755 "$file" "$predictor_target"
		echo "安装文件 $file 到 $predictor_dir 已完成"
    
	done
    
	echo "安装 predictor 目录的文件已完成。"

	echo "开始安装 sklearn 相关文件到 $sklearn_dir..."

	for file in "${SKLEARN_FILES[@]}"; do

		# 备份原文件
		sklearn_target="$sklearn_dir/$(basename "$file")"
		backup_if_exists "$sklearn_target"

		# 安装新文件
		install -m 755 "$file" "$sklearn_target"
		echo "安装文件 $file 到 $sklearn_dir 已完成"
        
	done

	echo "安装 sklearn 目录的文件完成。"
}

# 清理临时文件
clean() {

	echo "开始清理临时文件..."
	rm -f *~ core
	echo "清理完成。"
}

# 主函数 (默认执行安装)
main() {

	# 创建目录
	create_install_dir

	# 安装文件
	install_files

}

# 检测命令行参数并执行相应功能
if [[ "$1" == "clean" ]]; then
	clean
else
	main
fi

