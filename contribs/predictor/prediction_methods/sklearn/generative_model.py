import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import joblib
from datetime import datetime
import os
import sys

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
predictor_path = get_config_value('predictor_path', configuration_path)
# 定义AI随机森林路径为变量
sklearn_path = get_config_value('sklearn_path', configuration_path)

# 检查文件内容是否为空
def is_file_empty(filepath):

	# 检查文件是否存在且非空
	if os.path.isfile(filepath) and os.path.getsize(filepath) > 0:

		with open(filepath, 'r') as file:

			# 读取第二行内容，如果第二行为空，则文件内容为空
			file.readline()  # 读取第一行并丢弃
			second_line = file.readline().strip() # 读取第二行数据

			return not bool(second_line)  # 如果第二行为空，则返回 True

	return True  # 文件不存在或为空

if is_file_empty(f"{predictor_path}/jobHistory"):
	sys.exit(1)  # 退出脚本，状态码1表示错误

# 读取数据
data = pd.read_csv(f"{predictor_path}/jobHistory", delimiter='|')

# 处理数据
# 去掉 ReqMem 中的 'M' 并将其转换为整数
data['ReqMem'] = pd.to_numeric(data['ReqMem'].str.replace('M', ''), errors='coerce')
data['ReqMem'] = data['ReqMem'].fillna(0).astype(int)

# 将 Elapsed 转换为秒数
data['Elapsed'] = pd.to_timedelta(data['Elapsed']).dt.total_seconds()

# 特征和标签
features = ['User', 'Partition', 'ReqCPUS', 'ReqMem', 'ReqNodes', 'Timelimit']
target = 'Elapsed'

X = data[features]
y = data[target]

# 标签编码
label_encoders = {}
for feature in ['User', 'Partition', 'Timelimit']:
    le = LabelEncoder()
    X.loc[:, feature] = le.fit_transform(X[feature])
    label_encoders[feature] = le

# 划分数据集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 创建并训练模型
model = RandomForestRegressor(n_estimators=50, max_depth=10, min_samples_split=5, min_samples_leaf=2, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

# 获取当前时间并格式化
current_time = datetime.now().strftime("%Y-%m-%d")

# 生成带时间戳的文件名和绝对路径
model_filename = os.path.join(sklearn_path, f'sklearn_model_{current_time}.pkl')
label_encoders_filename = os.path.join(sklearn_path, f'sklearn_label_encoders_{current_time}.pkl')

# 保存模型和标签编码器
joblib.dump(model, model_filename)
joblib.dump(label_encoders, label_encoders_filename)

