# 配置表文件地址
configuration_path="/opt/gridview/slurm/etc/luaconfig/predictor/configuration"

# 获取作业历史数据的周期,单位为月
period=$(grep "^[^#]*period=" "$configuration_path" | awk -F '=' '{print $2}')

# 定义etc配置文件路径
etc_path=$(grep "^[^#]*etc_path=" "$configuration_path" | awk -F '=' '{print $2}')

# 定义 sacct 命令的路径为变量
sacct_cmd=$(grep "^[^#]*sacct_cmd=" "$configuration_path" | awk -F '=' '{print $2}')

# 定义predictor路径为变量
predictor_path=$(grep "^[^#]*predictor_path=" "$configuration_path" | awk -F '=' '{print $2}')

#获取开始时间
start_date=$(date -d "-$period months" +"%Y-%m-%d")

#获取结束时间
end_date=$(date -d "tomorrow" +"%Y-%m-%d")

#获取当前时间
current_date=$(date +"%Y-%m-%d")

#获取预测功能开关选项值
prediction_option=$(grep "^[^#]*predictionFunction=" "$configuration_path" | awk -F '=' '{print $2}')

#获取白名单用户
user_list=$(sed '1d' "$predictor_path/predictUsers" | grep -v "#"   | paste -sd "," -)

# 判断 prediction_option 是否为空
if [ -z "$prediction_option" ]; then
	echo "$(date '+%Y-%m-%d %H:%M:%S') Error: prediction_option is empty or not found." >> $predictor_path/update.log
#	exit 1
fi

#判断sacct命令是否可用,容错机制
$sacct_cmd --help > /dev/null 2>&1
# 检查sacct命令是否失败
if [ $? -ne 0 ]; then
	#echo ("$(date '+%Y-%m-%d %H:%M:%S') Error: %s is unavailable. Exiting script.", sacct_cmd) >> $predictor_path/update.log
	printf "$(date '+%Y-%m-%d %H:%M:%S') Error: %s is unavailable. Exiting script.\n" "$sacct_cmd" >> "$predictor_path/update.log"
	exit 1
fi

# 检查并创建临时目录用于存放子周期文件，并创建临时临时数据集
temp_dir="$predictor_path/temp"
mkdir -p "$temp_dir"
touch "$temp_dir/jobHistory_$current_date"
echo "JobID|User|Partition|ReqCPUS|ReqMem|ReqNodes|Timelimit|Elapsed|State|" > "$temp_dir/jobHistory_$current_date"

#判断预测功能开关选项并更新jobHistory
#当prediction_option=1时,只将白名单内的用户作业保存在jobHistory_$current_date中
#当prediction_option=2时,将所有用户的作业保存在jobHistory_$current_date中
for (( i=0; i<$period; i++ )); do

	#子周期起始和结束时间
	sub_start_date=$(date -d "$start_date +$i months" +"%Y-%m-%d")
	sub_end_date=$(date -d "$start_date +$((i+1)) months" +"%Y-%m-%d")

	#判断结束时间
	if [ "$sub_end_date" \> "$end_date" ]; then
		sub_end_date="$end_date"
	fi

	#子周期数据文件
	sub_file="$temp_dir/jobHistory_$i"

	#获取子周期数据文件
	if [ "$prediction_option" -eq 0 ]; then
		echo "$(date '+%Y-%m-%d %H:%M:%S') Warning : prediction_option=0, The time prediction function is disabled" >> $predictor_path/update.log

	elif [ "$prediction_option" -eq 1 ]; then

		# 检查 user_list 是否为空
		if [ -n "$user_list" ]; then
			$sacct_cmd -u $user_list -s cd -X -p --units M -S $sub_start_date -E $sub_end_date -o jobid,user,Partition,ReqCPUS,ReqMem,ReqNodes,Timelimit,Elapsed,State | grep -v '||' | grep -v '|00:00:00|' > "$sub_file"
		else
			: > "$sub_file"
		fi

	elif [ "$prediction_option" -eq 2 ]; then

		$sacct_cmd -s cd -X -p --units M -S $sub_start_date -E $sub_end_date -o jobid,user,Partition,ReqCPUS,ReqMem,ReqNodes,Timelimit,Elapsed,State | grep -v '||' | grep -v '|00:00:00|' > "$sub_file"
	fi

	#将子周期数据文件追加到临时数据文件
	if [ -s "$sub_file" ]; then
		line_count=$(wc -l < "$sub_file")
		if [ "$line_count" -gt 1 ]; then
			tail -n +2 "$sub_file" >> "$temp_dir/jobHistory_$current_date"
		fi
	fi

done

#将临时数据文件拷贝到预测工具目录下并删除临时目录
cp "$temp_dir/jobHistory_$current_date" "$predictor_path/jobHistory_$current_date"
rm -rf "$temp_dir"

# 验证并替换 ReqNodes 列中 1K 为 1024
if [ -f "$predictor_path/jobHistory_$current_date" ]; then

	awk -F'|' 'BEGIN {OFS="|"} {if ($6 == "1K") $6="1024"} {print}' "$predictor_path/jobHistory_$current_date" > "$predictor_path/jobHistory_${current_date}_tmp" && mv "$predictor_path/jobHistory_${current_date}_tmp" "$predictor_path/jobHistory_$current_date"
fi

# 判断文件 jobHistory_$current_date 是否存在
if [ -f "$predictor_path/jobHistory_$current_date" ]; then

	#检查旧的jobHistory
	if [ -L $predictor_path/jobHistory ]; then
		unlink $predictor_path/jobHistory
	elif [ -e $predictor_path/jobHistory ]; then
		rm -rf $predictor_path/jobHistory
	fi

	# 存在则创建新的符号链接
	ln -s $predictor_path/jobHistory_$current_date $predictor_path/jobHistory

	# 删除旧的jobHistory
	find $predictor_path/ -maxdepth 1 -type f -name 'jobHistory_*' ! -name "jobHistory_$current_date" -exec rm -f {} +
else
	echo "$(date '+%Y-%m-%d %H:%M:%S') Warning : File jobHistory_$current_date does not exist, symbolic link not created." >> $log
fi


# 同步执行生成模型更新的脚本
sklearn_path=$(grep "^[^#]*sklearn_path=" "$configuration_path" | awk -F '=' '{print $2}')
. $sklearn_path/update_model.sh

#打印提示
echo " " >> $predictor_path/update.log
echo "$(date '+%Y-%m-%d %H:%M:%S') ################################################################################################" >> $predictor_path/update.log
echo "$(date '+%Y-%m-%d %H:%M:%S') ###   Start date is: $start_date                                                              ###" >> $predictor_path/update.log
echo "$(date '+%Y-%m-%d %H:%M:%S') ###   End date is: $end_date                                                                ###" >> $predictor_path/update.log
echo "$(date '+%Y-%m-%d %H:%M:%S') ###   Today is: $current_date                                                                   ###" >> $predictor_path/update.log
echo "$(date '+%Y-%m-%d %H:%M:%S') ###   The jobs in the above time interval are saved in 'jobHistory_$current_date'               ###" >> $predictor_path/update.log
echo "$(date '+%Y-%m-%d %H:%M:%S') ################################################################################################" >> $predictor_path/update.log
echo " " >> $predictor_path/update.log
