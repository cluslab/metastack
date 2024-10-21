# coding=utf-8
import paramiko
import re
from copy import deepcopy


class Client(object):
    def __init__(self, hostname, password='', pkey='', username='root', port='22'):
        """
        Args:
        hostname (str): testbed host name
        password (str): testbed password
        username (str) : testbed username
        port (str): testbed port

        """
        self.hostname = hostname
        self.password = "Sugon@wangjy5123"
        self.username = username
        self.port = port

        self.pkey = pkey 

        # create SSH object
        self.ssh = paramiko.SSHClient()
        # allow to connect the host which is not in the known_hosts file
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # connect the the server
        #        self.ssh.connect(hostname=self.hostname, port=int(self.port), username=self.username, password=self.password, pkey=self.pkey)

        if len(pkey) == 0:
            self.ssh.connect(hostname=self.hostname, port=int(self.port), username=self.username,
                             password=self.password,timeout=30,auth_timeout=30,allow_agent=False,look_for_keys=False)
        else:
            key = paramiko.RSAKey.from_private_key_file(self.pkey)
            self.ssh.connect(hostname=self.hostname, username=self.username, pkey=key)

    def send_cmd(self, cmd):
        # execute command
        stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=180)
        # return the result
        result = stdout.read().decode()
        # return the error result; For stdout and stderr, only one can return at a time
        err = stderr.read().decode()
        if not err:
            print('stdout=%s'%result)
            return result
        else:
            # print('stderr')
            # logging.error('ERROR： [%s] command return error [%s]' % (cmd, err))
            return err

    def disconnect(self):
        # close the connection
        self.ssh.close()

    @staticmethod
    def gene_table(result, sep='|'):
        return create_table(result, sep)

    @staticmethod
    def gene_lines(result):
        return create_lines(result)

    @staticmethod
    def gene_dict(result):
        return create_dict(result)


def create_lines(str_input):
    """
    :param str_input:
    :return:
    """
    res_list = str_input.split('\n')
    res_list = [i for i in res_list if i != '']
    return res_list


def create_table(t_input, separator='|'):
    """ used for the table_input which has a separator. Need a header to create the dictionary key value

    Args:
        t_input (str): Lines from CLI containing table
        separator (str): Separator for t_input

    Returns:
        list<dict>: list of dictionaries containing column:value for the table
        demo：
            input:
                JobID|User|Group|Submit|Start|End|State|NodeList|NNodes|Timelimit|Eligible|ExitCode|TotalCPU

            output: [{'JobID': '480', 'User': 'putong_ac', 'Group': 'putong_ac', 'Submit': '2021-10-27T08:14:09',
            'Start': '2021-10-27T08:14:09', 'End': '2021-10-27T08:14:12', 'State': 'CANCELLED by 0', 'NodeList': 'gv43',
             'NNodes': '1', 'Timelimit': '1-00:00:00', 'Eligible': '2021-10-27T08:14:09', 'ExitCode': '0:0',
             'TotalCPU': '00:00:00'},...]
            :param separator:  default "|"

    """
    lines = create_lines(t_input)
    tbl = []

    if separator == ' ':
        # one space. Used for squeue -O related operation etc. Those who have return value like this can use it
        headers = re.sub(r"[\s]", " ", lines[0]).split() if lines else []
        # for line in lines[1:-1]:
        for line in lines[1:]:
            tbl.append(dict(zip(headers, re.sub(r"[\s]", " ", line).split())))

    else:
        # Used for 'sacctmgr show xxx -p' or 'squeue xxx -o'
        headers = lines[0].split(separator) if lines else []
        # for line in lines[1:-1]:
        for line in lines[1:]:
            tbl.append(dict(zip(headers, line.split(separator))))

    return tbl


def create_dict(t_input):
    """ used for "scontrol show partition -o" & "scontrol show -o job --details"
        & "scontrol show -o nodes/node" related command. '-o' parameter is mandatory.

    Args:
        t_input (str): Lines from CLI containing table

    Returns:
        list<dict>: list of dictionaries containing column:value for the table
        demo:
            input:
                JobId=112 ArrayJobId=112 ArrayTaskId=2 JobName=test.py UserId=test7_usr(10223) GroupId=test7_usr(10223)
                JobId=113 ArrayJobId=112 ArrayTaskId=1 JobName=test.py UserId=test7_usr(10223) GroupId=test7_usr(10223)

            output: [{''}, {'...'}]

    """
    lines = create_lines(t_input)

    lines_normal = []
    lines_abn = []

    for i in range(0, len(lines)):
        lines[i] = lines[i].split()  # split and delete space
        res_value = modify_abn(lines[i])
        lines_normal.append(res_value['normal'])
        lines_abn.append(res_value['abnormal'])

    # recreate the list. --- from [['', '', '', ...],['', '', '', ...],['', '', '', ...]..]  to ['','',''...]
    new_lines = []
    for line in lines_normal:
        new_lines.append(" ".join(line))

    # generate the dict key
    tbl = []

    for line in new_lines:
        tb_tmp = re.split('[' ' =]', line)
        # tb_tmp = re.split('[\s=]\s*',line)
        tbl.append(dict(zip(tb_tmp[::2], tb_tmp[1::2])))

    # add the abnormal info back to dict
    if lines_abn:
        for dic, dic1 in zip(tbl, lines_abn):
            dic.update(dic1)

    return tbl


def modify_abn(a_list):
    """ used for processing the return data in which there are some data not like 'A=B' format.
        Find these 'abnormal' data.

    Args:
        a_list (str): Like this ---
            ['NodeName=gv41New73', 'Arch=x86_64', 'CPUTot=4', 'OS=Linux', '3.10.0-1127.el7.x86_64', '#1', 'SMP']
            Here: '3.10.0-1127.el7.x86_64', '#1', 'SMP' are considered as 'abnormal' data

    Returns:
        dict<list, dict>:
                res_value['normal']: type--list. This is the list without 'abnormal' data
                res_value['abnormal']: type--dict. This is the dict with 'abnormal' data

    """
    a_list_new = deepcopy(a_list)

    bool_list = []  # used for abnormal string TAG
    abnormal_list = []  # abnormal list
    abn_list = []
    sp = '='  # separator

    # normal data set 1, and abnormal data set 0; remove abnormal data from a_list_new
    for s in a_list:
        count = s.count(sp)
        if count > 1:
            count = 0
        if count == 0:
            a_list_new.remove(s)
        bool_list.append(count)

    # Generate abnormal list
    for i in range(0, len(a_list) - 1):
        if bool_list[i] == 0:
            abn_list.append(a_list[i])
            if bool_list[i + 1] != 0:
                abnormal_list.append(abn_list)
                abn_list = []

    # handle the last one
    if bool_list[-1] == 0:
        abn_list.append(a_list[-1])
        abnormal_list.append(abn_list)

    dic = {}  # used for abnormal data
    ab_key = 'AbKey'  # abnormal data key
    for i in range(0, len(abnormal_list)):
        ab_key1 = ab_key + str(i)
        dic.update({ab_key1: " ".join(abnormal_list[i])})

    res_value = {'normal': a_list_new, 'abnormal': dic}

    return res_value


def create_dict_old(t_input):
    """ used for "scontrol show partition -o" & "scontrol show -o job --details"
        & "scontrol show -o nodes/node" related command. '-o' parameter is mandatory.

    Args:
        t_input (str): Lines from CLI containing table

    Returns:
        list<dict>: list of dictionaries containing column:value for the table
        demo:
            input:
                JobId=112 ArrayJobId=112 ArrayTaskId=2 JobName=test.py UserId=test7_usr(10223) GroupId=test7_usr(10223)
                JobId=113 ArrayJobId=112 ArrayTaskId=1 JobName=test.py UserId=test7_usr(10223) GroupId=test7_usr(10223)

            output: [{''}, {'...'}]

    """
    lines = create_lines(t_input)

    # Used to save the Hardware related lines. These lines have different data format
    hw_str = []
    # Used to save the OS related lines. These lines have different data format
    os_str = []
    reason_str = []

    lines_tmp = deepcopy(lines)

    for i in range(0, len(lines)):
        flag = False
        reason_flag = False
        # split and delete space
        lines[i] = lines[i].split()
        lines_tmp[i] = lines_tmp[i].split()

        # split Hardware and OS related info
        # !!!!!!! Note: For node info, maybe there still have some other data format this function cannot handle.
        for s in lines[i]:
            if 'RealMemory' in s:
                flag = False
            elif flag:
                os_str.append(s)
                lines_tmp[i].remove(s)
            # Used for "CfgTRES=cpu=64,mem=515468M,billing=64 "
            elif 'TRES=cpu' in s:
                hw_str.append(s)
                lines_tmp[i].remove(s)
            # Used for "OS=Linux 3.10.0-957.el7.x86_64 #1 SMP Thu Nov 8 23:39:32 UTC 2018 "
            elif 'OS=Linux' in s:
                os_str.append(s)
                lines_tmp[i].remove(s)
                flag = True
            # Used for node "Reason=Not responding [root@2021-12-16T21:05:11]"
            elif reason_flag or 'Reason=' in s:
                reason_str.append(s)
                lines_tmp[i].remove(s)
                reason_flag = True
        if os_str:
            lines_tmp[i].append('-'.join(os_str))
            os_str = []
        if reason_str:
            lines_tmp[i].append('-'.join(reason_str))
            reason_str = []

    # recreate the list. --- from [['', '', '', ...],['', '', '', ...],['', '', '', ...]..]  to ['','',''...]
    new_lines = []
    for line in lines_tmp:
        new_lines.append(" ".join(line))

    # generate the dict key
    # headers = re.split('[' ' =]',new_lines[0])
    tbl = []

    for line in new_lines:
        tb_tmp = re.split('[' ' =]', line)
        # tb_tmp = re.split('[\s=]\s*',line)
        tbl.append(dict(zip(tb_tmp[::2], tb_tmp[1::2])))

    # add the hardware info back to dict
    if hw_str:
        i = 0
        for dic in tbl:
            dic.update({'HWRes': hw_str[i]})
            i += 1

    return tbl
