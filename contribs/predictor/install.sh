#!/bin/bash

# 安装目录
install_dir="/opt/gridview/slurm/etc/luaconfig/predictor"
ai_environment_dir="$install_dir/AIenvironment"
sklearn_dir="$install_dir/prediction_methods/sklearn"

# 要安装的文件
FILES=("predictUsers" "predictionTool.lua" "README.md" "updateJobHistory" "configuration")
SKLEARN_FILES=("prediction_methods/sklearn/generative_model.py" "prediction_methods/sklearn/prediction_time.py" "prediction_methods/sklearn/update_model")

# 创建安装目录
create_install_dir() {
  echo "创建安装目录 $install_dir..."
  mkdir -p "$install_dir"
  mkdir -p "$ai_environment_dir"
  mkdir -p "$sklearn_dir"
  echo "安装目录创建完成。"
}

# 安装文件
install_files() {
  echo "开始安装文件到 $install_dir..."
  for file in "${FILES[@]}"; do
    install -m 755 "$file" "$install_dir"
  done
  echo "安装 predictor 目录的文件完成。"

  echo "开始安装 sklearn 相关文件到 $sklearn_dir..."
  for file in "${SKLEARN_FILES[@]}"; do
    install -m 755 "$file" "$sklearn_dir"
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
  create_install_dir
  install_files
}

# 检测命令行参数并执行相应功能
if [[ "$1" == "clean" ]]; then
  clean
else
  main
fi

