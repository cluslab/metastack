disableAccountsFile = "/work/slurm_share/etc/disableAccounts"
-- 分区白名单
partitionOk="/work/slurm_share/etc/luaconfig/passPart"
-- 用户白名单
userOk="/work/slurm_share/etc/luaconfig/passUser"
-- 分区DefMemPerCpu配置
partDefMemPerCpuConf="/work/slurm_share/etc/luaconfig/partDefMemPerCpuConf"
-- 默认DefMemPerCpu和分区中的每个节点CPU数
DefMemPerCPU=3832
DefCpuPerNode=64



function slurm_job_submit(job_desc, part_list, submit_uid)

        -- check account

        local usedAccount = ""
        if (job_desc.account == null) then
                usedAccount = job_desc.default_account
        else
                usedAccount = job_desc.account
        end

        slurm.log_info("Account used is %s", usedAccount)


        if (checkValueInFile(disableAccountsFile, usedAccount) == 1) then
                slurm.log_info("The usedAccount is disabled!")
                return 1
        end


        -- check cpu and mem

        local partition_list=getValue(job_desc.partition,"partition")
        local min_mem_per_node=getValue(job_desc.min_mem_per_node,"min_mem_per_node")
        local min_mem_per_cpu=getValue(job_desc.min_mem_per_cpu,"min_mem_per_cpu")
        local shared=getValue(job_desc.shared,"shared")
        local min_cpus=getValue(job_desc.min_cpus,"min_cpus")
        local min_nodes=getValue(job_desc.min_nodes,"min_nodes")


        slurm.log_debug3("part_list is %s", partition_list)
        slurm.log_debug3("min_mem_per_node is %s", min_mem_per_node)
        slurm.log_debug3("min_mem_per_cpu is %s", min_mem_per_cpu)
        slurm.log_debug3("shared is %s", shared)
        slurm.log_debug3("min_cpus is %s", min_cpus)
        slurm.log_debug3("min_nodes is %s", min_nodes)
        --If the job specifies --exclusive, skip the MemCPUMatchCheck
        --Check whether the partition or the user should skip the MemCPUMatchCheck
        --0 不跳过MemCPUMatchCheck检查
        --1 跳过MemCPUMatchCheck检查

        --CareHere: check jobname for percent-sign
        slurm.log_info("CareHere: pattern job name is %s", job_desc.name)
        if StringContains(job_desc.name, "%") then
                slurm.log_error("jobname contains character of percent-sign")
                slurm.log_user("You cann't specify percent-sign at job name")
                return 2114
        else
                slurm.log_info("CareHere: jobname NOT contains character")
        end

		
        if((checkExclusive(shared)==1) or ((checkUser(submit_uid))==1)) then
                slurm.log_debug3("skip mem:cpu check")
                return slurm.SUCCESS
        end
        
        -- 作业指定了分区
        if(partition_list ~= "nil") then
                -- 判断指定的分区是否都在白名单
                local partition
                local partitions = split(partition_list, ",")
                local part_count = table.getn(partitions)
                local part_number = 0
                for key,value in pairs(partitions) do
                        partition = value
                        if(checkPart(partition)==1) then
                                part_number = part_number + 1
                        end 
                end

                -- 指定的分区都在白名单中，返回成功
                if(part_number == part_count and partitions ~= "nil") then
                        slurm.log_debug3("skip mem:cpu check")
                        return slurm.SUCCESS 
                end

                -- 指定的分区不全在白名单中
                -- 判断指定的分区是否都符合DefMemPerCpu等要求
                -- 若任何一个分区不符合要求，就返回提交错误
                for key,value in pairs(partitions) do
                        partition = value
                        part_number = 0
                        local partDefMemPerCpu = getPartDefMem(partDefMemPerCpuConf,partition)
                        local partCpuPerNode = getPartCpuPerNode(partDefMemPerCpuConf,partition)
                        --0 作业申请的内存:cpu 大于DefMemPerCPU
                        --1 作业申请的内存:cpu 不大于DefMemPerCPU
                        local MemCPUMatchCheckResult = MemCPUMatchCheck(partDefMemPerCpu, partCpuPerNode, min_mem_per_node, min_mem_per_cpu, min_nodes, min_cpus)

                        if( MemCPUMatchCheckResult == 0) then
                                return 2800
                        end
                end
                
                return slurm.SUCCESS
        -- 作业未指定分区        
        else
                local MemCPUMatchCheckResult = MemCPUMatchCheck(DefMemPerCPU, DefCpuPerNode, min_mem_per_node, min_mem_per_cpu, min_nodes, min_cpus)

                if( MemCPUMatchCheckResult == 0) then
                        return 2800
                else
                        return slurm.SUCCESS 
                end
        end
end


function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
            return slurm.SUCCESS
end


-- 分割字符串，用户多分区处理
-- str 需要分割的字符串
-- reps 按照reps进行字符串分割
function split(str,reps)
        if(str == "nil") then
                return "nil"
        end
        local resultStrList = {}
        string.gsub(str,'[^'..reps..']+',function (w)
            table.insert(resultStrList,w)
        end)
        return resultStrList
end

-- 判断作业申请的内存与申请的CPU是否符合要求
function MemCPUMatchCheck(partDefMemPerCpu, partCpuPerNode, minMemPerNode, minMemPerCpu, minNodes, minCpus)
        local MemPerCPU
        local cpuPerNode
        
        -- 判断分区DefMemPerCPU是否需要覆盖默认的DefMemPerCPU
        if(partDefMemPerCpu ~= "nil") then
                MemPerCPU = tonumber(partDefMemPerCpu)
        else
                MemPerCPU = DefMemPerCPU
        end
        if(partCpuPerNode ~= "nil") then
                cpuPerNode = tonumber(partCpuPerNode)
        else
                cpuPerNode = DefCpuPerNode
        end

        --  --mem和--mem-per-cpu均为指定，使用默认值
        if((minMemPerNode == "nil") and (minMemPerCpu == "nil")) then
                return 1
        --  申请节点的所有内存，需要使用节点的所有cpu
        elseif (minMemPerNode == 0 or minMemPerCpu == 0) then
                if((minCpus/minNodes) == cpuPerNode) then
                        return 1
                else
                        return 0
                end
        --  判断每个节点申请的内存是否与每个节点申请的CPU个数相匹配
        elseif (minMemPerNode ~= "nil")  then
                if(minMemPerNode/(minCpus/minNodes) <= MemPerCPU) then
                        return 1
                end
                return 0
        --  判断每个CPU申请的内存是否不超过DefMemPerCPU
        elseif (minMemPerCpu ~= "nil") then
                if(minMemPerCpu <= MemPerCPU) then
                        return 1
                end
                return 0
        end
end

-- 在partDefMemPerCpuConf配置文件中，按照名字查找指定分区配置的DefMemPerCpu
-- 若分区未配置DefMemPerCpu，返回nil
-- 若分区在配置文件中不存在，返回nil
-- 否则,返回分区定义的DefMemPerCpu

function getPartDefMem(partDefMemPerCpuConf,partitionName)
        local partDefMemPerCpu = getColumnValueInFile(partDefMemPerCpuConf, 2, partitionName);
        return partDefMemPerCpu;
end

-- 在partDefMemPerCpuConf配置文件中，按照名字查找指定该分区配置的节点cpu数
-- 若分区未配置节点Cpu，返回nil
-- 若分区在配置文件中不存在，返回nil
-- 否则,返回分区定义的CpuPerNode
function getPartCpuPerNode(partDefMemPerCpuConf,partitionName)
        local CpuPerNode = getColumnValueInFile(partDefMemPerCpuConf, 3, partitionName);
        return CpuPerNode;
end

-- 获取文件中某一列的值,columnNum是列的序号，1表示第一列
-- 如果没有该列，则返回空，如果该列的值不是数字也返回空
function getColumnValueInFile(filePath,columnNum,name)
	local result = "nil"
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



--所有job信息的特殊处理 值为空时赋值nil;
--值为65534,4294967294时赋值为-1
--sbatch提交作业不指定节点数时，默认min_nodes为4294967294
function getValue(value,name)
        if(value == null) then
                slurm.log_info("check value null")
                value="nil"
        elseif(value == 65534 or value == 4294967294) then
                if("min_nodes" == name) then
                        value=1
                else
                        value=-1
                end
        end
        return value
end


--检查作业所在队列是否需要执行MemCPUMatchCheck
function checkPart(partition)
        if(partition == "nil") then
                return 0
        end
        
        if(checkValueInFile(partitionOk,partition) == 1) then
                return 1
        else
                return 0
        end

end


--检查该用户的作业是否需要执行MemCPUMatchCheck
function checkUser(submit_uid)
        if(checkValueInFile(userOk,submit_uid) == 1) then
                return 1
        else
                return 0
        end 

end

--检查作业是否独占节点
function checkExclusive(shared)
        if(-1 ~= shared) then
                return 1
        else
                return 0
        end

end


--检查文件中是否有该value值
-- 存在：返回1
-- 不存在，返回0
function checkValueInFile(filename,value)
        local result=0
        local file = io.open(filename ,"r");
        -- 若文件不存在，直接返回0
        if(file==nil) then
                slurm.log_info("%s not exist!", filename)
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
--检查字符串中是否包含特定字符串，允许检查包括%在内的特殊字符
function StringContains(str, item)
    local t = {}
    local l = {}
    local index = 0
    for i = 1, string.len(str) do
        table.insert(t, string.byte(string.sub(str, i, i)))
    end

    for i = 1, string.len(item) do
        table.insert(l, string.byte(string.sub(item, i, i)))
    end
    if #l > #t then
        return false
    end

    for k, v1 in pairs(t) do
        index = index + 1
        if v1 == l[1] then
            local iscontens = true
            for i = 1, #l do
                if t[index + i - 1] ~= l[i] then
                    iscontens = false
                end
            end
            if iscontens then
                return iscontens
            end
        end
    end
    return false
end


slurm.log_info("initialized")
return slurm.SUCCESS
