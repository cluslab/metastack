-- 配置表文件地址
configuration_path="/opt/gridview/slurm/etc/luaconfig/predictor/configuration"

-- 从配置文件中获取变量的值
function get_config_value(key)
	local file = io.open(configuration_path, "r")
	if not file then
		slurm.log_debug("Could not open config file: %s", configuration_path)
	end

	for line in file:lines() do
		if not line:match("^#") then
			local k, v = line:match("^(%S+)=(%S+)")
			if k == key then
				file:close()
				return v
			end
		end
	end

	file:close()
	return nil
end

-- 预测工具使用的方法
-- 均值法:predictionMethod=0
-- AI随机森林算法:predictionMethod=1
predictionMethod=get_config_value("predictionMethod")

-- 预测工具路径
predictor_path=get_config_value("predictor_path")

-- 历史作业数据
jobHistory=predictor_path .. "/jobHistory"

-- 定义etc路径的变量
etc_path=get_config_value("etc_path")

-- 历史作业数据文件路径
file_path=jobHistory

-- python 3.9 执行路径
python_executable=get_config_value("python_executable")

-- AI随机森林工具路径
sklearn_path=get_config_value("sklearn_path")

-- AI建模python脚本
prediction_script=sklearn_path .. "/prediction_time.py"

-- slurm.log_debug("etc_path is : %s", etc_path)
-- slurm.log_debug("sklearn_path is : %s", sklearn_path)
-- slurm.log_debug("prediction_script is : %s", prediction_script)

function timePredict(job_desc, user_name, partition_list, min_cpus, req_mem, min_nodes, time_limit)

	-- 获取user_name
	local user_name = user_name

	-- 获取partition_list
	local partition_list = partition_list

	-- 获取req_cpu
	local req_cpu = min_cpus

	-- 获取req_mem
	local req_mem = req_mem

	-- 获取req_nodes
	local req_nodes = min_nodes

	-- 初始化time_limit_standard
	local time_limit_standard = nil

	-- 遍历分区，预测作业运行时间，并取最大值
	if(partition_list ~= "nil") then
  
	    -- 定义predict_time
	    local predictTool_time = 0
        
	    -- 对多分区作业分区进行分割，并在每个分区内预测
	    local partition_p
	    local partitions_p = split(partition_list, ",")
        
	    -- 在各个分区中预测，并取最大值作为预测值
	    for key,value in pairs(partitions_p) do
	        partition_p = value

			-- 当作业未指定time_limit时，查找对应分区的DefaultTime
			if (time_limit ~= -1) then
				time_limit_standard = conversion_time_limit(time_limit)
			else
				local command = string.format(
					"cat %s/slurm_partition.conf | grep -v \"\\#\" | grep -i \"PartitionName=%s\" | grep -i DefaultTime | awk -F 'DefaultTime=' '{print $2}' | awk '{print $1}'",
					etc_path, partition_p
				)

				-- 执行命令并读取结果
				local handle = io.popen(command)
				local result = handle:read("*a")
				handle:close()

				-- 去除换行符和空格
				result = result:gsub("%s+", "")

				slurm.log_debug2("partition's DefaultTime is %s", result)
				-- 检查结果是否为空
				if (result == nil or result == "") then
					time_limit_standard = "UNLIMITED"
				else
					time_limit_standard = result
				end
			end
		
			-- 当作业脚本中未指定--mem或--mem-per-cpu时,使用分区默认配置*cpu数作为req_mem
			if (req_mem == 0) then
			
				-- 初始化	
				local req_p_mem = 0				

				-- 获取对应分区的DefMemPerCpu
				local partDefMemPerCpuConf = etc_path .. "/luaconfig/partDefMemPerCpuConf"
				req_p_mem = getPartDefMem(partDefMemPerCpuConf, partition_p)
			
				-- 对应分区有特殊配置的DefMemPerCPU
				if (req_p_mem ~= "nil") then
					req_mem = req_p_mem * req_cpu

				-- 未特殊配置,则使用全局DefMemPerCPU
				else 
					req_mem = DefMemPerCPU * req_cpu
				end
			end
	
			local predictTool_p_time = 0			

			-- 当req_mem==-1时,为作业中--mem=0的场景,不做全匹配,针对分区核存比白名单和通用市场
			if (req_mem ~= -1) then

				-- 记录开始时间
				-- local start_time = os.clock()

				-- 选择对应预测方法预测
				if (predictionMethod == "0" or predictionMethod == "") then

					slurm.log_debug2("The input parameters of the prediction tool are:user_name:%s, partition:%s, req_cpus:%s, req_mem:%s, req_nodes:%s, time_limit_standard:%s", user_name, partition_p, req_cpu, req_mem, req_nodes, time_limit_standard)

					-- 使用均值法,对作业进行全匹配,并预测作业运行时间
	        			predictTool_p_time = predict_time_min(user_name, partition_p, req_cpu, req_mem, req_nodes, time_limit_standard)

				elseif (predictionMethod == "1") then
		
					-- AI预测时传入时间格式为数值
					local ai_time_limit = parse_time(time_limit_standard)

					slurm.log_debug2("The input parameters of the prediction tool are:user_name:%s, partition:%s, req_cpus:%s, req_mem:%s, req_nodes:%s, ai_time_limit:%s", user_name, partition_p, req_cpu, req_mem, req_nodes, ai_time_limit)

					-- AI随机森林回归算法,预测作业运行时间
					predictTool_p_time = sklearn_prediction(user_name, partition_p, req_cpu, req_mem, req_nodes, ai_time_limit)

					-- 当AI法预测为负值或0时,进行全匹配修正
					predictTool_p_time = tonumber(predictTool_p_time)
					if ((predictTool_p_time <= 0) or predictTool_p_time == nil or predictTool_p_time == "") then
						slurm.log_debug2("The AI predicted value is abnormal, and full matching is carried out to find the mean correction.")
						local corrected_value = read_and_find_job_ai(file_path, user_name, partition_p, req_cpu, req_mem, req_nodes, time_limit_standard)

						corrected_value = tonumber(corrected_value)
						if (corrected_value == 0) then
							predictTool_p_time = 0
						else
							predictTool_p_time = corrected_value
						end
					end
	
				end

				-- 记录结束时间
				-- local end_time = os.clock()

				-- 计算并打印花费的时间
				-- local elapsed_time = end_time - start_time
				-- slurm.log_debug("Prediction took %.4f seconds", elapsed_time)
	        		
			end

	        predictTool_time = tonumber(predictTool_time)
			predictTool_p_time = tonumber(predictTool_p_time)
			slurm.log_debug3("predictTool_p_time is %d", predictTool_p_time)

			-- 在该分区内未找到相似作业,没有预测值时,或AI预测为负值时,做模糊匹配
			-- 找该用户对应分区得最近10条作业求平均值
			if ((predictTool_p_time <= 0) or (req_mem == -1)) then
				local fuzzy_value = read_and_find_job(file_path, user_name, partition_p)
				slurm.log_debug2("No similar job is found, fuzzy matching is performed, and the predicted value is %s minutes", fuzzy_value)
				
				fuzzy_value = tonumber(fuzzy_value)
				
				if(fuzzy_value > predictTool_time) then
					predictTool_time = fuzzy_value
				end
			
			-- 当predictTool_p_time为nil时
			-- AI预测下得一种异常现象
			elseif predictTool_p_time == nil or predictTool_p_time == "" then
				
				predictTool_time = 0	

			-- 在该分区内有预测值,直接赋值
			else

				if(predictTool_p_time > predictTool_time) then
					predictTool_time = predictTool_p_time
				end
			end	
	    end
	
		-- 当time_limit不为-1时,最终预测值和time_limit做比较
		if (time_limit ~= -1) then

			local time_limit_e = tonumber(time_limit)

			-- 预测值小于0或大于time_limit时,重置预测值为0,走后续赋值流程
			if (predictTool_time < 0 or predictTool_time > time_limit_e) then
				predictTool_time = 0
				slurm.log_debug2("The predicted value of job running time is less than 0 or greater than the time_limit, reset the predicted value to 0.")
			end

		else
			if (predictTool_time < 0) then
				predictTool_time = 0
				slurm.log_debug2("The predicted value is not reasonable, set it to 0 and go through the subsequent assignment process")
			end
		end

		predictTool_time = tostring(predictTool_time or "0")

		slurm.log_debug2("predictTool_time is %s", predictTool_time)

		-- 为作业替换预测值
		-- 作业有预测值
		if (predictTool_time ~= "0") then
			job_desc.predict_job = 1
			job_desc.time_min = predictTool_time
			slurm.log_debug("The predicted value of %s job running time is %s minutes, updating it to TimeMin.", user_name, predictTool_time)
		
		-- 模糊匹配后依然没有预测值,但作业指定了TimeLimit
		elseif (predictTool_time == "0" and time_limit ~= -1) then
			job_desc.predict_job = 1
			job_desc.time_min = math.ceil(time_limit / 2)
			slurm.log_debug("No similar job set for %s job is found, setting TimeMin to half of TimeLimit with a value of %s minutes.", user_name, job_desc.time_min)
			
		-- 模糊匹配后依然没有预测值,且作业未指定TimeLimit,但分区有DefaultTime,预测值为DefaultTime的一半
		elseif (predictTool_time == "0" and time_limit == -1 and time_limit_standard ~= "UNLIMITED") then
			time_limit_standard = time_string_to_minutes(time_limit_standard)
			job_desc.predict_job = 1
			job_desc.time_min = math.ceil(time_limit_standard / 2)
			slurm.log_debug("No similar job set for %s job is found and TimeLimit parameter is missing, setting TimeMin to half of partition's DefaultTime with a value of %s minutes.", user_name, job_desc.time_min)
			
		-- 模糊匹配后依然没有预测值,且作业未指定TimeLimit,分区没有DefaultTime,无法给出预测值
		elseif (predictTool_time == "0" and time_limit == -1 and time_limit_standard == "UNLIMITED") then
			slurm.log_debug("No similar job set for %s job is found, TimeLimit parameter and partition's DefaultTime are missing, No predicted value can be given.", user_name)

		end

	end

end

-- 计算请求的time_limit，返回为标准格式
function conversion_time_limit(minutes)
	
	-- 转化单位为秒
	local total_seconds = minutes * 60

	-- 计算天、小时、分钟和秒
	local days = math.floor(total_seconds / 86400)
	total_seconds = total_seconds % 86400

	local hours = math.floor(total_seconds / 3600)
	total_seconds = total_seconds % 3600

	local minutes = math.floor(total_seconds / 60)
	
	local secs = total_seconds % 60

	-- 返回格式为标准时间格式
	if days > 0 then
		return string.format("%d-%02d:%02d:%02d", days, hours, minutes, secs)
	else
		return string.format("%02d:%02d:%02d", hours, minutes, secs)
	end

end

-- 分割字符串。str -- 需要分割的字符串；reps -- 按照reps进行字符串分割
function split(str, reps)

	-- 当字符串为空，返回nil
	if(str == "nil") then
			return "nil"
	end

	local resultStrList = {}

	string.gsub(str,'[^'..reps..']+',function (w)
		table.insert(resultStrList,w)
	end)

	return resultStrList

end

-- 预测最小时间time_min
function predict_time_min(user_name, partition, req_cpu, req_mem, req_nodes, time_limit)
	
	-- 定义返回结果
	local predictTool_time_min = 0 

	-- 预测
	local predictTool_time_min_p = get_avg_time(user_name, partition, req_cpu, req_mem, req_nodes, time_limit)
	 slurm.log_debug3("predictTool_time_min_p is %s", predictTool_time_min_p)    

	predictTool_time_min_p = tonumber(predictTool_time_min_p)

	-- 当time_limit不为UNLIMITED时,预测值和time_limit做比较
	if (time_limit ~= "UNLIMITED") then
		local time_limit_p = parse_time(time_limit)
		time_limit_p = math.ceil(time_limit_p / 60)	
 
        	-- 判断并返回合适的time_min值
		if (predictTool_time_min_p > 0 and predictTool_time_min_p <= time_limit_p) then
			predictTool_time_min = predictTool_time_min_p
		else 
        		predictTool_time_min = 0
		end

	-- 当time_limit为UNLIMITED时,直接赋值
	else
		predictTool_time_min = predictTool_time_min_p
	end

	-- 返回预测的时间（分钟）
	return predictTool_time_min
end

-- 定义函数，接受五个参数，并返回total_minutes
function get_avg_time(user_name, partition, req_cpu, req_mem, req_nodes, time_limit)

	-- 初始化返回值
	local avg_time = 0

	-- 拼接请求的内存
	local req_mem = tostring(req_mem) .. "M"

	-- 读取jobHistory
	-- local file_path = jobHistory

	-- 匹配相似作业，并获取Elapsed
	local matched_times = read_and_match_job_history(file_path, user_name, partition, req_cpu, req_mem, req_nodes, time_limit)

	-- 若有相似作业，计算平均时间，若没有则为0
	if matched_times and #matched_times > 0 then
		local average_time = calculate_average_time(matched_times)	
	
		slurm.log_debug3("average_time is %s", average_time)
		avg_time = time_string_to_minutes(average_time)
	else
		avg_time = 0
	end

	-- slurm.log_debug3("avg_time is %s", avg_time)
	-- 返回平均时间	
	return avg_time
end

-- 读取jobHistory文件并做特征全匹配
function read_and_match_job_history(file_path, user_name, partition, req_cpu, req_mem, req_nodes, time_limit)
	
	req_cpu = tostring(req_cpu)
	req_nodes = tostring(req_nodes)

	local matched_times = {}

	-- 构造 grep 命令
	local command = string.format(
        	"grep '^.*|%s|%s|%s|%s|%s|%s|.*$' %s | awk -F'|' '{print $(NF-2)}'",
        	user_name, partition, req_cpu, req_mem, req_nodes, time_limit, file_path
    	)

	-- 执行命令并读取结果
	local handle = io.popen(command)
	local result = handle:read("*a")
	handle:close()

	-- 将结果插入matched_times
	local i = 1
	for time in result:gmatch("[^\r\n]+") do
		matched_times[i] = time
		i = i + 1
	end

	-- matched_times = result:gmatch("[^\r\n]+")

	return matched_times
end

-- 若预测值为0,读取jobHistory文件做模糊匹配
function read_and_find_job(file_path, user_name, partition)
	
	local matched_times = {}

	-- 构造 grep 命令
	local command = string.format(
                "grep '^.*|%s|%s|.*$' %s | awk -F'|' '{print $(NF-2)}' | tail -n 10",
                user_name, partition, file_path
        )

	-- 执行命令并读取结果
	local handle = io.popen(command)
        local result = handle:read("*a")
        handle:close()

	-- 将结果插入matched_times
	local i = 1
        for time in result:gmatch("[^\r\n]+") do
                matched_times[i] = time
                i = i + 1
        end

	-- 初始化返回值
	local avg_time = 0

	-- 若有相似作业，计算平均时间，若没有则为0
	if matched_times and #matched_times > 0 then
		local average_time = calculate_average_time(matched_times)	
	
		slurm.log_debug3("average_time is %s", average_time)
		avg_time = time_string_to_minutes(average_time)
	else
		avg_time = 0
	end

	return avg_time
end

-- 针对AI预测,若预测值小于等于0,读取jobHistory文件做全匹配
function read_and_find_job_ai(file_path, user_name, partition, req_cpu, req_mem, req_nodes, time_limit_standard)
	
	local req_cpu = tostring(req_cpu)
	local req_nodes = tostring(req_nodes)
		
	-- 拼接请求的内存
	local req_mem = tostring(req_mem) .. "M"

	local matched_times = {}

	-- 构造 grep 命令
	local command = string.format(
                "grep '^.*|%s|%s|%s|%s|%s|%s|.*$' %s | awk -F'|' '{print $(NF-2)}' | tail -n 10",
                user_name, partition, req_cpu, req_mem, req_nodes, time_limit_standard, file_path
        )

	-- 执行命令并读取结果
	local handle = io.popen(command)
        local result = handle:read("*a")
        handle:close()

	-- 将结果插入matched_times
	local i = 1
        for time in result:gmatch("[^\r\n]+") do
                matched_times[i] = time
                i = i + 1
        end

	-- 初始化返回值
	local avg_time = 0

	-- 若有相似作业，计算平均时间，若没有则为0
	if matched_times and #matched_times > 0 then
		local average_time = calculate_average_time(matched_times)	
	
		slurm.log_debug3("average_time is %s", average_time)
		avg_time = time_string_to_minutes(average_time)
	else
		avg_time = 0
	end

	return avg_time
end

-- 计算平均时间
function calculate_average_time(time_list)
	
	-- 定义初始参数
	local total_seconds = 0
	local count = #time_list

	-- 遍历list，时间求和
	for _, time_str in ipairs(time_list) do
		total_seconds = total_seconds + parse_time(time_str)
	end

	-- 计算平均时间
	local average_seconds = total_seconds / count
        slurm.log_debug2("The number of similar jobs is:%d, and the average time is: %d seconds", count, average_seconds)
	
	-- 返回标准格式的时间
	return format_time(average_seconds)
end

-- 解析时间字符串，将其转换为秒数
function parse_time(time_str)

	-- 定义初始参数
	local days, hours, minutes, seconds = 0, 0, 0, 0

	-- 从字符串中获取值
	if time_str:match("(%d+)-(%d+):(%d+):(%d+)") then
		days, hours, minutes, seconds = time_str:match("(%d+)-(%d+):(%d+):(%d+)")
	elseif time_str:match("(%d+):(%d+):(%d+)") then
		hours, minutes, seconds = time_str:match("(%d+):(%d+):(%d+)")
		days = 0
	else
		return 0
	end

	-- 转换为整数
	days = tonumber(days) or 0
	hours = tonumber(hours) or 0
	minutes = tonumber(minutes) or 0
	seconds = tonumber(seconds) or 0
	
	-- 返回数值型
	return days * 86400 + hours * 3600 + minutes * 60 + seconds
end

-- 将秒数转换回标准的时间格式
function format_time(seconds)

	-- 天
	local days = math.floor(seconds / 86400)
	seconds = seconds % 86400

	-- 小时
	local hours = math.floor(seconds / 3600)
	seconds = seconds % 3600
    
	-- 分钟
	local minutes = math.floor(seconds / 60)
	
	-- 秒
	seconds = seconds % 60

	-- 格式转换
	if days > 0 then
		return string.format("%d-%02d:%02d:%02d", days, hours, minutes, seconds)
	else
		return string.format("%02d:%02d:%02d", hours, minutes, seconds)
	end
end

-- 将时间字符串转换为以分钟为单位的整数值的函数
function time_string_to_minutes(time_string)

	-- 匹配并提取天、小时、分钟和秒
	local days, hours, minutes, seconds = time_string:match("(%d+)-(%d+):(%d+):(%d+)")

	if days then
	
		-- 转换为整数
		days = tonumber(days)
		hours = tonumber(hours)
		minutes = tonumber(minutes)
		seconds = tonumber(seconds)
	else

		-- 如果没有匹配到天，则尝试匹配小时、分钟和秒
		hours, minutes, seconds = time_string:match("(%d+):(%d+):(%d+)")

		-- 没有天时，天数设为0
		days = 0  
		hours = tonumber(hours)
		minutes = tonumber(minutes)
		seconds = tonumber(seconds)
	end

	-- slurm.log_debug3("days is %d, hours is %d, minutes is %d, secondsis %d", days, hours, minutes, seconds)

	-- 计算总分钟数
	local total_minutes = (days * 24 * 60) + (hours * 60) + minutes + (seconds / 60)

	-- 向上取整返回总分钟数
	return math.ceil(total_minutes)
end

-- 查找分区配置的DefMemPerCpu 
function getPartDefMem(partDefMemPerCpuConf, partitionName)
        local partDefMemPerCpu = getColumnValueInFile(partDefMemPerCpuConf, 2, partitionName);
        return partDefMemPerCpu;
end

-- 获取文件中某一列的值,columnNum是列的序号，1表示第一列
-- 如果没有该列，则返回空，如果该列的值不是数字也返回空
function getColumnValueInFile(filePath, columnNum, name)

	-- 初始化
	local result = "nil"

	-- 获取值
        local file = io.open(filePath ,"r");

        if(file == nil) then
                return result
        end

	for line in file:lines() do

		local itemLineArr = {}

		--将行按空格分隔
		for itemLine in string.gmatch(line, "%S+") do
                        table.insert(itemLineArr,itemLine)
                end

		-- 如果数组的第一个值与partitionName能匹配，则取数组的第二个值
		-- 第二列配置的DefMemPerCpuConf
		-- 第三列配置的CPusPerNode
		if(itemLineArr[1] == tostring(name)) then

			if(#itemLineArr >= columnNum) then

				-- 判断是否是数字，是数字的话则赋值给result
				-- 否则result还是nil
				local n = tonumber(itemLineArr[columnNum]);

				if n then
					result = itemLineArr[columnNum]
				end
			end
		end
	end

	file:close()
	return result
end

-- 随机森林预测作业执行时间
function sklearn_prediction(user_name, partition_p, req_cpu, req_mem, req_nodes, time_limit_standard)

	-- 判断 Python 可执行文件是否存在
	if not file_exists(python_executable) then
		slurm.log_debug("python3.9 executable is missing")
	end

	-- 判断 prediction_time.py 是否存在
	if not file_exists(prediction_script) then
		slurm.log_debug("prediction_time.py file is missing")
[O	end

	-- 构建 Python 命令
	local command = string.format(
		"%s -u %s %s %s %s %s %s %s",
		python_executable, prediction_script, user_name, partition_p, req_cpu, req_mem, req_nodes, time_limit_standard
	)

	-- 执行命令并捕获输出
	local handle = io.popen(command)
	local result = handle:read("*a")
	handle:close()

	return result
end

-- 检查文件是否存在的函数
function file_exists(path)

	-- 尝试以只读模式打开文件
	file = io.open(path, "r")
	if file then
		file:close()
		return true
	else
		return false
	end
end


