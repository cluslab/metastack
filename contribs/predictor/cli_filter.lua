-- 配置表文件地址
configuration_path="/opt/gridview/slurm/etc/luaconfig/predictor/configuration"

-- Path to define apptype.properties
properties_path = "/opt/gridview/slurm/etc/apptype.properties"

function slurm_cli_pre_submit(options, pack_offset)

	local apptype_result = get_apptype(options)
	if apptype_result ~= nil then
		options["apptype"] = apptype_result
	end

	-- 加载预测工具并预测作业运行时间
	load_prediction_tool(options, pack_offset)

	return slurm.SUCCESS

end

function slurm_cli_setup_defaults(options, early_pass)

		return slurm.SUCCESS
end

function slurm_cli_post_submit(offset, job_id, step_id)

		return slurm.SUCCESS
end

-- 加载预测工具
function load_prediction_tool(options, pack_offset)

	local array_inx = options["array"]

	if array_inx ~= nil and array_inx ~= "invalid-context" then
		options["predict-job"] = -1
		return
	end

	local sig = options["signal"]

	if sig ~= '0@0' then
		options["predict-job"] = -1
		return
	end

	-- 默认关闭预测功能
	predictionFunction = "0"  

	-- 判断configuration文件是否缺失，并读取相关路径
	configuration_file = io.open(configuration_path, "r")

	if configuration_file then
		configuration_file:close()

		-- 预测功能开关
		-- predictionFunction=0关闭预测流程
		-- predictionFunction=1开启预测流程只适用于白名单用户
		-- predictionFunction=2开启预测流程适用全部用户
		predictionFunction = get_config_value("predictionFunction")

		-- 当预测功能开启时,判断基础工具,只有存在时才可预测
		if (predictionFunction ~= "0") then

			-- etc配置路径
			etc_path = get_config_value("etc_path")

			-- 预测工具路径
			predictor_path = get_config_value("predictor_path")

			-- 预测功能白名单
			predictUsers_file = predictor_path .. "/predictUsers"

			-- 获取作业参数脚本 getParameter.lua
			getParameter_file = predictor_path .. "/getParameter.lua"

			-- 预测工具脚本 predictionTool.lua
			predictionTool_file = predictor_path .. "/predictionTool.lua"

			-- 作业集 jobHistory
			jobHistory_file = predictor_path .. "/jobHistory"

			-- 检查文件是否存在
			local puf = io.open(predictUsers_file, "r")
			local gpf = io.open(getParameter_file, "r")
			local ptf = io.open(predictionTool_file, "r")
			local jhf = io.open(jobHistory_file, "r")

			if not (puf and gpf and ptf and jhf) then
				-- 如果任一文件不存在，则关闭预测功能
				predictionFunction = "0"
			end

			-- 关闭文件句柄
			if puf then puf:close() end
			if gpf then gpf:close() end
			if ptf then ptf:close() end
			if jhf then jhf:close() end
		end

		-- 判断AI法所需要的工具
		if (predictionFunction ~= "0") then

			-- 预测工具使用的方法
			-- 均值法:predictionMethod=0
			-- AI随机森林算法:predictionMethod=1
			predictionMethod_value=get_config_value("predictionMethod")

			if (predictionMethod_value == "1") then 

				-- python 3.9 执行路径
				python_executable_path=get_config_value("python_executable")

				-- AI随机森林工具路径
				sklearn_path=get_config_value("sklearn_path")

				-- AI建模python脚本
				prediction_script_file=sklearn_path .. "/prediction_time.py"

				local pep = io.open(python_executable_path, "r")
				local psf = io.open(prediction_script_file, "r")

				if not (pep and psf) then
					-- 如果任一文件不存在，则关闭预测功能
					predictionFunction = "0"
				end

				-- 关闭文件句柄
				if pep then pep:close() end
				if psf then psf:close() end

			end

		end

	end

	-- 为白名单用户开启预测功能
	if (predictionFunction == "1") then

		-- 加载参数获取工具
		dofile(getParameter_file)

		-- 获取初始参数
		getParameter(options)

		-- 将异构作业offset不等于0的作业在此重置一次time_min的标识
		if (pack_offset ~= 0) then

			time_min = -2
		end

		-- 判断作业是否已指定TimeMin，若未指定走预测流程
		if (time_min == -2) then
			
			-- 预测功能校验用户，若为预测功能用户，执行时间预测模块
			if (checkValueInFile(predictUsers_file, user_name) == 1) then

				-- 加载预测工具
				dofile(predictionTool_file)
	
				-- 预测作业运行时间
				timePredict(options, user_name, partition_list, req_cpu, req_mem, req_node, time_limit)
	
			-- 若不为预测功能用户，直接跳过预测模块
			else
			
				options["predict-job"] = -1

			end
	
		-- 若已指定TimeMin，直接跳过预测模块
		else

			options["predict-job"] = -1

		end

	-- 为所有用户开启预测功能
	elseif (predictionFunction == "2") then

		-- 加载参数获取工具
		dofile(getParameter_file)

		-- 获取初始参数
		getParameter(options)

		-- 将异构作业offset不等于0的作业在此重置一次time_min的标识
		if (pack_offset ~= 0) then

			time_min = -2
		end

		-- 判断作业是否已指定TimeMin，若未指定走预测流程
		if (time_min == -2) then

			-- 加载预测工具
			dofile(predictionTool_file)
	
			-- 预测作业运行时间
			timePredict(options, user_name, partition_list, req_cpu, req_mem, req_node, time_limit)

		-- 若已指定TimeMin，直接跳过预测模块
		else

			options["predict-job"] = -1

		end

	-- 未开启预测功能
	else

		options["predict-job"] = -1

	end

end

-- 从配置文件中获取变量的值
function get_config_value(key)

	local file = io.open(configuration_path, "r")

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

--检查文件中是否有该value值
-- 存在：返回1
-- 不存在，返回0
function checkValueInFile(filename,value)

	local result=0
	local file = io.open(filename ,"r")

	-- 若文件不存在，直接返回0
	if(file==nil) then

		return 0
	end

	-- 文件存在，进行解析
	for line in file:lines() do

		if(line == tostring(value)) then

			result = 1

			break
		end

	end

	file:close()

	if(result == 0) then
		return 0
	else 
		return 1
	end

end

-- Define a function to read the apptype.properties file and build a hash table
function read_properties(file_path)
	local hash_table = {}

	local file = io.open(file_path, "r")
	if not file then
		return nil
	end

	for line in file:lines() do
		if not line:match("^#") then
			local key, value = line:match("([^=]+)=([^=]+)")
			if key and value then
				hash_table[key:lower()] = value -- Convert key to lowercase
			end 
		end 
	end 

	file:close()

	return hash_table
end


-- Define a function to resolve the script path
function resolve_path(work_dir, script)
	if script:sub(1, 1) == "/" then
		return script -- Absolute path, return directly
	else
		return work_dir .. "/" .. script -- Relative path, append the working directory
	end
end

-- Process the script file and match using the hash table
function process_script(script_path, hash_table)
	local file = io.open(script_path, "r")
	if not file then
		return nil
	end

	local weak_apptypes = (hash_table["weak_apptype"] or ""):lower() -- Convert weak application types to lowercase
	local weak_match = nil -- Variable to store a weak application type match

	for line in file:lines() do
		-- First-level tokenization: Split by spaces
		for space_word in line:gmatch("%S+") do
			local lower_space_word = space_word:lower() -- Convert token to lowercase for case-insensitive match
			if hash_table[lower_space_word] then
				local value = hash_table[lower_space_word]
				if weak_apptypes:find(value:lower()) then
					weak_match = value -- Store weak match
				else
					file:close()
					return value -- Return strong match immediately
				end
			else
				-- Second-level tokenization: Split by "/"
				for slash_word in space_word:gmatch("[^/]+") do
					local lower_slash_word = slash_word:lower() -- Convert token to lowercase
					if hash_table[lower_slash_word] then
						local value = hash_table[lower_slash_word]
						if weak_apptypes:find(value:lower()) then
							weak_match = value -- Store weak match
						else
							file:close()
							return value -- Return strong match immediately
						end
					else
						-- Third-level tokenization: Split by ".", "-", "=", "_", "+"
						for fine_word in slash_word:gmatch("[^%.%-=_%+]+") do
							local lower_fine_word = fine_word:lower() -- Convert token to lowercase
							if hash_table[lower_fine_word] then
								local value = hash_table[lower_fine_word]
								if weak_apptypes:find(value:lower()) then
									weak_match = value -- Store weak match
								else
									file:close()
									return value -- Return strong match immediately
								end
							end
						end
					end
				end
			end
		end
	end

	file:close()
	return weak_match -- Return weak match if no strong match was found
end


-- A function that separates strings
function split_string(input, sep)
	sep = sep or "%s"
	local fields = {}
	for str in string.gmatch(input, "([^" .. sep .. "]+)") do
		table.insert(fields, str)
	end
	return fields
end

-- Parses the string and looks up the mapped value in the hash table
function process_string(input_str, hash_table)
	if not input_str or input_str:match("^%s*$") then
		return nil -- If it is empty or contains only whitespace characters, return nil
	end

	local weak_apptypes = (hash_table["weak_apptype"] or ""):lower() -- Retrieve weak application types as a lowercase string
	local weak_match = nil -- Variable to store a weak application type match

	-- Coarse-grained tokenization: split the string by spaces
	for coarse_word in input_str:gmatch("%S+") do
		local lower_coarse_word = coarse_word:lower() -- Convert token to lowercase
		-- Directly lookup in the hash table
		if hash_table[lower_coarse_word] then
			local value = hash_table[lower_coarse_word]
			if weak_apptypes:find(value:lower()) then
				weak_match = value -- Store weak match
			else
				return value -- Return strong match immediately
			end
		else
			-- Medium-grained tokenization: split by '/'
			for mid_word in coarse_word:gmatch("[^/]+") do
				local lower_mid_word = mid_word:lower() -- Convert token to lowercase
				if hash_table[lower_mid_word] then
					local value = hash_table[lower_mid_word]
					if weak_apptypes:find(value:lower()) then
						weak_match = value -- Store weak match
					else
						return value -- Return strong match immediately
					end
				else
					-- Fine-grained tokenization: split by '.-=_+'
					for fine_word in mid_word:gmatch("[^%.%-=_%+]+") do
						local lower_fine_word = fine_word:lower() -- Convert token to lowercase
						if hash_table[lower_fine_word] then
							local value = hash_table[lower_fine_word]
							if weak_apptypes:find(value:lower()) then
								weak_match = value -- Store weak match
							else
								return value -- Return strong match immediately
							end
						end
					end
				end
			end
		end
	end

	return weak_match -- Return weak match if no strong match was found
end

-- Determine how assignments are submitted
function check_command_type(line)
	local elements = split_string(line)
	if #elements > 0 then
		local first_element = elements[1]
		if first_element:sub(-6) == "sbatch" then
			return 0 -- sbatch
		elseif first_element:sub(-4) == "srun" then
			return 1 -- srun
		elseif first_element:sub(-6) == "salloc" then
			return 2 -- salloc
		else
			return -1 -- unknown command
		end
	else
		return -1 
	end
end

function get_apptype(options)

	-- 1. Retrieve submit_line, work_dir, and apptype
	local submit_line = options["submit-line"]
	local work_dir = options["chdir"] or ""
	local apptype = options["apptype"] 

	if apptype ~= nil and apptype == "unset" then
		return "unset"
	end

	-- 2. Get the properties file path and construct the hash table
	local hash_table = read_properties(properties_path)
	if not hash_table then return nil end

	-- 3. Determine the job submission type
	if not submit_line then return nil end
	local submit_type = check_command_type(submit_line)
	if not submit_type then return nil end

	-- 4. Map the application type with work path, submit command, comment, and jobname
	local jobname = options["job-name"] or ""
	local comment = options["comment"] or ""

	local temp = table.concat({jobname, comment, submit_line, work_dir}, " ")
	local result = process_string(temp, hash_table)
	if result ~= nil then
		options["apptype"] = result
		return result
	end

	-- 5. Handle different submission methods
	if submit_type == 0 then
		-- Extract the script path
		local script = submit_line:match("%S+$")
		-- Construct the script path using workdir
		if not work_dir then return nil end
		local script_path = resolve_path(work_dir, script)
		-- Process the script and return the application type
		local result = process_script(script_path, hash_table)
		-- Log the application type
		if result ~= nil then
		options["apptype"] = result
			return result
		end

	elseif submit_type == 1 then
		-- srun
		local result = process_string(apptype, hash_table)
		if result ~= nil then
			options["apptype"] = result
			return result
		end
	elseif submit_type == 2 then
		-- salloc
	end

	return nil
end 