import atf
import pytest
import os
import re
import time
import pwd
p1_node_str = ""
ERROR_TYPE = "error"
OUTPUT_TYPE = "output"

# Setup
@pytest.fixture(scope="module", autouse=True)
def setup():
    global node_list, p1_node_str

    atf.require_auto_config("wants to change partitions, modify/create nodes")
    atf.require_accounting(modify=True)
    atf.require_config_parameter_includes('SchedulerParameters', 'defer,bf_max_job_test=1,bf_interval=600,bf_max_time=60')
    node_info = atf.require_nodes_hpc(
        [
            (2, [('CPUs', 8),('Boards',1),('SocketsPerBoard',4),('CoresPerSocket',2),('ThreadsPerCore',1)]),
            (2, [('CPUs', 4),('Boards',1),('SocketsPerBoard',4),('CoresPerSocket',1),('ThreadsPerCore',1)]),
            (1, [('CPUs', 2),('Boards',1),('SocketsPerBoard',2),('CoresPerSocket',1),('ThreadsPerCore',1)])
        ]
    )
    node_list=[]
    for item in node_info:
        node_list.append(item['NodeName'])
    atf.require_config_parameter('PartitionName', {
     'p1'    : {
         'Nodes'         : ','.join(node_list[0:5]),
         'Default'       : 'NO',
         'State'         : 'UP',
         'MaxTime'       : 'INFINITE',
         'HetPart'       : 'YES'},
    
     'p2'    : {
         'Nodes'         : ','.join(node_list[0:5]),
         'Default'       : 'NO',
         'State'         : 'UP',
         'MaxTime'       : 'INFINITE',
         'HetPart'       : 'NO'}
     },source='slurm_partition')
    
    p1_node_str = ','.join(node_list[0:2])
    atf.require_slurm_running()

class FPC:
    def __init__(self,tmp_path):
        self.tmp_path = tmp_path

    # creates either an error or an output file with file name formatting
    # as a full path from the tmp_path
    def create_file_path(self, file_type = OUTPUT_TYPE):
        if file_type == ERROR_TYPE:
            return self.tmp_path / f"file_err.error"
        return self.tmp_path / f"file_out.output"

    # creates either an error or an output file with file name formatting
    def create_file(self, file_type = OUTPUT_TYPE):
        if file_type == ERROR_TYPE:
            return f"file_err.error"
        return f"file_out.output"

    # only works for file names, not full paths
    def remove_file(self, file_path):
        os.remove(str(self.tmp_path) + "/" + file_path)

    # returns the first file in the tmp_path
    # usful when you only have 1 file in tmp_path
    def get_tmp_file(self):
        return os.listdir(self.tmp_path)[0]

# Tests:

def test_ALL(tmp_path):
    test_user = pwd.getpwuid(os.getuid())[0]
    atf.run_command_output(f"scancel -u {test_user}",fatal=True)

   # job_id = atf.submit_job(f"-p p2 -N 1 -n 8 --wrap \"sleep 1000\"")
   # print("jobId=%s"%job_id)
   # assert atf.wait_for_job_state(job_id, "RUNNING")
   # time.sleep(10)
   # job_id = atf.submit_job(f"-p p2 -N 1 -n 8 --wrap \"sleep 1000\"")
   # print("jobId=%s"%job_id)
   # assert atf.wait_for_job_state(job_id, "RUNNING")
   # time.sleep(10)
   # job_id = atf.submit_job(f"-p p2 -N 1 -n 8 --wrap \"sleep 1000\"")
   # print("jobId=%s"%job_id)
   # assert atf.wait_for_job_state(job_id, "PENDING")
   # time.sleep(10)

   # job_id = atf.submit_job(f"-p p2 -N 1 -n 4 --wrap \"sleep 1000\"")
   # print("jobId=%s"%job_id)
   # assert atf.wait_for_job_state(job_id, "PENDING")
   # time.sleep(10)
   # 
   # test_user = pwd.getpwuid(os.getuid())[0]
   # atf.run_command_output(f"scancel -u {test_user}",fatal=True)
   # time.sleep(10)

   # job_id = atf.submit_job(f"-p p1 -N 1 -n 8 --wrap \"sleep 1000\"")
   # print("jobId=%s"%job_id)
   # assert atf.wait_for_job_state(job_id, "RUNNING")
   # time.sleep(10)

   # job_id = atf.submit_job(f"-p p1 -N 1 -n 8 --wrap \"sleep 1000\"")
   # print("jobId=%s"%job_id)
   # assert atf.wait_for_job_state(job_id, "RUNNING")
   # time.sleep(10)

   # job_id = atf.submit_job(f"-p p1 -N 1 -n 8 --wrap \"sleep 1000\"")
   # print("jobId=%s"%job_id)
   # assert atf.wait_for_job_state(job_id, "PENDING")
   # time.sleep(10)
   # 
   # job_id = atf.submit_job(f"-p p1 -N 1 -n 4 --wrap \"sleep 1000\"")
   # print("jobId=%s"%job_id)
   # assert atf.wait_for_job_state(job_id, "RUNNING")
   # time.sleep(10)
   # 
   # test_user = pwd.getpwuid(os.getuid())[0]
   # atf.run_command_output(f"scancel -u {test_user}",fatal=True)
   # time.sleep(10)
    fpc = FPC(tmp_path)
    file_out = fpc.create_file_path()
    file_in = tmp_path / "file_in.s.input"
    atf.make_bash_script(file_in, f"""
        srun sleep 1000 &
    """)
    os.chmod(file_in, 0o0777)
    job_id = atf.submit_job(f"-p p2 -N1 -n8 --output {str(file_out)} {str(file_in)}")
    print("job_id = %s"%job_id)
