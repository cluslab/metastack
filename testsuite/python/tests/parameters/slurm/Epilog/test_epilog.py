############################################################################
# Copyright (C) SchedMD LLC.
############################################################################
import atf
import os
import pytest
import shutil
import time

epilog_timeout = 60

# TODO: Temporary debug variable to troubleshoot bug 14466 (remove once fixed)
srun_ran_successfully = False


# Setup
@pytest.fixture(scope="module", autouse=True)
def setup():
    global node_name,node_ip_list
    atf.require_auto_config("wants to set the Epilog")
    atf.require_config_parameter('PrologEpilogTimeout', epilog_timeout)
    node_info = atf.require_nodes(1)
    node_ip_list=[]
    node_ip_list.append(node_info[0]["NodeAddr"])
    atf.require_slurm_running(node_ip_list)
    node_name = node_info[0]["NodeName"]

    # TODO: Temporary debug to troubleshoot bug 14466 (remove once fixed)
    yield
    if not srun_ran_successfully:
        atf.run_command("scontrol show partition")
        atf.run_command("sinfo")
        atf.run_command("scontrol show node")


def test_epilog(tmp_path):
    """Test Epilog"""

    sleep_symlink = str(tmp_path / f"sleep_symlink_{os.getpid()}")
    os.symlink(shutil.which('sleep'), sleep_symlink)
    epilog = str(tmp_path / 'epilog.sh')
    touched_file = str(tmp_path / 'touched_file')
    atf.make_bash_script(epilog, f"""touch {touched_file}
/usr/bin/sleep 60 &
exit 0
""")
    atf.set_config_parameter('Epilog', epilog)
    atf.restart_slurmd(node_ip_list)
    time.sleep(5)
    outfile = str(tmp_path / 'outfile')
    checksubprocfile = str(tmp_path / 'chechproc.sh')
    atf.make_bash_script(checksubprocfile,f"""
ssh root@{node_name} "ps -ef |grep -w '/usr/bin/sleep 60' | grep -v 'grep' | awk '{{print $2}}'" > {outfile}
""")

    # Verify that the epilog ran by checking for the file creation
    job_id = atf.run_job_id(f"-t1 true", fatal=True)

    # TODO: Temporary debug mechanics to troubleshoot bug 14466 (remove once fixed)
    global srun_ran_successfully
    srun_ran_successfully = True

    assert atf.wait_for_file(touched_file), f"File ({touched_file}) was not created"

    # The child (sleep) should continue running after the epilog exits until the epilog timeout
    time.sleep(1)

    # Verify that the job enters the completing state
    assert atf.wait_for_job_state(job_id, "COMPLETING", fatal=True)

    # Verify that the child processes of the epilog is still running
    atf.run_command(f"chmod 0777 {checksubprocfile}", user='root', fatal=True, quiet=True)
    atf.run_command(f"sh +x {checksubprocfile}", user='root', fatal=True, quiet=True)
    result = atf.run_command(f"cat {outfile}")
    assert result['stdout'] !='',"The epilog child process should continue running after the epilog completes (until the the epilog timeout)"
    #assert atf.run_command_exit(f"pgrep -f {sleep_symlink}") == 0, "The epilog child process should continue running after the epilog completes (until the the epilog timeout)"

    # After the epilog timeout, the job should complete and the process should be killed

    # Verify that the job enters the completing state
    assert atf.wait_for_job_state(job_id, "COMPLETED", fatal=True, timeout=epilog_timeout+5)
    atf.run_command(f"sh +x {checksubprocfile}", user='root', fatal=False, quiet=False)
    result = atf.run_command(f"cat {outfile}")
    assert result['stdout'] == '', "The epilog child process should have been killed after the epilog timeout"
   
    # Verify that the child processes of the epilog has been killed
    #assert atf.run_command_exit(f"pgrep -f {sleep_symlink}", xfail=True) != 0, "The epilog child process should have been killed after the epilog timeout"
