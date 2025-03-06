-- 配置表文件地址
configuration_path="/opt/gridview/slurm/etc/luaconfig/predictor/configuration"

function getParameter(options)

	-- 判断configuration文件是否缺失，并读取相关路径
	local file = io.open(configuration_path, "r")

	if file then
		file:close()

		-- 预测功能开关
		-- predictionFunction=0关闭预测流程
		-- predictionFunction=1开启预测流程只适用于白名单用户
		-- predictionFunction=2开启预测流程适用全部用户
		predictionFunction=get_config_value("predictionFunction")
                
		-- etc配置路径
		etc_path=get_config_value("etc_path")
                
		-- 预测工具路径
		predictor_path=get_config_value("predictor_path")

		-- 预测功能白名单
		predictUsers=predictor_path .. "/predictUsers"

	end
		
	-- 获取初始参数 

	-- 获取 user_name
	user_name = 0
	local user_id = options["uid"]

	local handle = io.popen("id -nu " .. user_id)
	local tmp_user_name = handle:read("*a"):gsub("%s+", "")
	handle:close()

	user_name = tmp_user_name

	-- slurm.log_info("allowed for user_name: %s", user_name)

	-- 获取 partition
	partition_list = 0
	local partition = options["partition"] or 0

	-- 当未指定partition时,获取默认分区
	if partition == 0 then

		local command = string.format(
			"cat %s/slurm_partition.conf | grep -v '#' | grep 'Default=YES' | awk '{for(i=1;i<=NF;i++) if($i ~ /^PartitionName=/) print substr($i, length(\"PartitionName=\")+1)}'",
			etc_path
		)

		-- 执行命令并读取结果
		local handle = io.popen(command)

		if handle then

			-- 读取输出
			local default_partition = handle:read("*l"):gsub("%s+", "")
			handle:close()

			-- 更新partition
			if default_partition and default_partition ~= "" then
				partition = default_partition
			else
				partition = "none"
			end

		end
        
	end

	partition_list = partition

	-- slurm.log_info("allowed for partition_list: %s", partition_list)

	-- 获取 req_node
	req_node = 0
	local nodes = options["nodes"]
	local ntasks = options["ntasks"]
	local ntasks_per_node = options["ntasks-per-node"]

	if string.find(nodes, "-") then

		-- 当未指定-N时
		-- 当指定-n 和 --ntasks-per-node时
		if ntasks ~= "1" and ntasks_per_node ~= "-2" then

			local tmp_ntasks = tonumber(ntasks)
			local tmp_ntasks_per_node = tonumber(ntasks_per_node)

			req_node = math.ceil(tmp_ntasks / tmp_ntasks_per_node)

		else 
            
			-- 无法准确计算出使用的节点时
			req_node = 1

		end

	else

		-- 当指定-N时
		req_node = nodes

	end

	-- slurm.log_info("allowed for req_node: %s", req_node)

	-- 获取 req_cpu
	req_cpu = 0
	local cpus_per_task = options["cpus-per-task"]

	if cpus_per_task == "0" then
		-- 1个task使用1个cpu

		-- 指定cpus-per-task
		if ntasks_per_node ~= "-2" then
			req_cpu = ntasks_per_node * req_node

		-- 指定ntasks
		elseif ntasks ~= "1" then
			req_cpu = ntasks

		-- task为1
 		else
			req_cpu = req_node

		end

	else
		-- 1个task使用cpus-per-task个cpu

		-- 指定cpus-per-task
		if ntasks_per_node ~= "-2" then
			req_cpu = cpus_per_task * ntasks_per_node * req_node

		-- 指定ntasks
		elseif ntasks ~= "1" then
			req_cpu = cpus_per_task * ntasks

		-- task为1
		else
			req_cpu = cpus_per_task * req_node
		end

	end

	-- slurm.log_info("allowed for req_cpu: %s", req_cpu)

	-- 获取 req_mem
	req_mem = 0
	local mem = options["mem"] or "nil"
	local mem_per_cpu = options["mem-per-cpu"] or "nil"

	-- 定义不可达值
	NO_VAL = nil

	-- 用于验证字节结束的辅助函数
	local function _end_on_byte(arg)

		-- 检查后缀是否有效且不包含额外字符
		return #arg == 1
	end

	-- 函数将字符串转换为兆字节
	local function str_to_mbytes(arg)

 		-- 对--mem=0做异常处理
		if not arg or type(arg) ~= "string" or arg:find("%?") then
			arg = "0"
		end

		-- 提取数字部分
		local num_part = arg:match("^%d+")
		if not num_part then

			-- 没有数字部分
			return NO_VAL
		end

		-- 将数字部分转换为数字
		local result = tonumber(num_part)
		if not result or result < 0 then

			-- 无效或负数
			return NO_VAL
		end

		-- 提取后缀（如果有的话）
		local suffix = arg:match("%D*$") or ""
		-- 规范化和删除空白
		suffix = suffix:lower():gsub("%s+", "")

		-- 根据后缀进行转换
		if suffix == "" then
			-- 无后缀，默认为MB
			return result

		-- 将KB转换为MB，四舍五入
		elseif suffix == "k" and _end_on_byte(suffix) then
			result = math.ceil(result / 1024)
        
		-- 已经在MB，什么都不做
		elseif suffix == "m" and _end_on_byte(suffix) then
        
		-- GB转换为MB
		elseif suffix == "g" and _end_on_byte(suffix) then
			result = result * 1024
        
		-- 将TB转换为MB
		elseif suffix == "t" and _end_on_byte(suffix) then
			result = result * 1024 * 1024

		-- 无效的后缀
		else
			return NO_VAL
		end

		return result
	end

	-- 当指定--mem
	if mem ~= "nil" then

		req_mem = tostring(str_to_mbytes(mem) * req_node)

	-- 当指定--mem-per-cpu
	elseif mem_per_cpu ~= "nil" then

		req_mem = tostring(str_to_mbytes(mem_per_cpu) * req_cpu)
    
	-- 都未指定时,使用DefMemPerCPU
	elseif mem == "nil" and mem_per_cpu == "nil" then

		-- req_mem在后续用DefMemPerCPU计算
		req_mem = "nil"
	end

	-- 指定了--mem=0
	if req_mem == "0" then
		req_mem = -1

	-- 未指定内存参数,后续用DefMemPerCPU计算
	elseif req_mem == "nil" then
		req_mem = 0

	end

	-- slurm.log_info("allowed for req_mem: %s", req_mem)

	-- 获取 time_limit
	time_limit = slurm.time_str2mins(options["time"])
	time_min = slurm.time_str2mins(options["time-min"])

	-- slurm.log_info("allowed for time_limit: %s", time_limit)
	-- slurm.log_info("allowed for time_min: %s", time_min)

end