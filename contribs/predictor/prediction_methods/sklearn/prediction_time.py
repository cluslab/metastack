import numpy as np
import joblib
import argparse
import warnings

# 禁用不必要的警告
warnings.filterwarnings("ignore")

# 获取配置
def get_config_value(key, filepath):
    with open(filepath, 'r') as file:
        for line in file:
            # 跳过注释行
            if line.startswith('#'):
                continue
            # 查找指定的键
            if line.startswith(key):
                return line.split('=')[1].strip().strip('"')
    return None

# 配置文件路径
configuration_path='/opt/gridview/slurm/etc/luaconfig/predictor/configuration'

# 定义predictor路径为变量
sklearn_path = get_config_value('sklearn_path', configuration_path)

# 全局加载模型和标签编码器，避免重复加载
model = joblib.load(f"{sklearn_path}/sklearn_model.pkl")
model.n_jobs = -1  # 使用所有可用CPU核心进行预测
label_encoders = joblib.load(f"{sklearn_path}/sklearn_label_encoders.pkl")

# 直接使用NumPy进行预测操作，优化特征编码处理
def predict_elapsed(model, label_encoders, user, partition, req_cpus, req_mem, req_nodes, timelimit):
    # 创建特征数组，避免不必要的转换
    input_data = np.array([[user, partition, req_cpus, req_mem, req_nodes, timelimit]])

    # 直接使用编码后的结果，减少重复工作
    for i, feature in enumerate(['User', 'Partition']):
        le = label_encoders[feature]
        input_data[0, i] = le.transform([input_data[0, i]])[0] if input_data[0, i] in le.classes_ else le.transform([le.classes_[0]])[0]

    # 进行并行预测
    predicted_elapsed = model.predict(input_data)[0]
    return int(np.ceil(predicted_elapsed / 60))  # 将秒数转换为分钟并向上取整

def main():
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='Predict elapsed time for a job.')
    parser.add_argument('user', type=str, help='User')
    parser.add_argument('partition', type=str, help='Partition')
    parser.add_argument('req_cpus', type=int, help='Requested CPUs')
    parser.add_argument('req_mem', type=str, help='Requested Memory (e.g., 112000M)')
    parser.add_argument('req_nodes', type=int, help='Requested Nodes')
    parser.add_argument('timelimit', type=int, help='Timelimit in seconds (e.g., 259200 for 3-00:00:00)')

    args = parser.parse_args()

    # 预测耗时
    predicted_elapsed = predict_elapsed(
        model,
        label_encoders,
        args.user,
        args.partition,
        args.req_cpus,
        args.req_mem,
        args.req_nodes,
        args.timelimit
    )
    print(predicted_elapsed)

if __name__ == '__main__':
    main()

