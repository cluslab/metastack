############################################################################
# Copyright (C) SchedMD LLC.
############################################################################
import atf
import pytest
import re

node_count = 5 


# Setup
@pytest.fixture(scope="module", autouse=True)
def setup():
    atf.require_accounting()
    atf.require_nodes(node_count)
    atf.require_slurm_running()


#def sacct_verify_unique_nodes(sacct_string):
#    sacct_output_list = sacct_string.split('\n')
#    print("sacct_output_list = %s"%sacct_output_list)
#    sacct_output_set = set()
#    print(sacct_output_list[2:-1])
#    # The steps don't start until the second index
#    for i in sacct_output_list[2:-1]:
#        print("i = %s"%i)
#        print("i.split('|')[0] = %s"%i.split('|')[0])
#        if i.find('[') == -1:
#            sacct_output_set.add(i.split('|')[0])
    # Convert to set verify that the set and list have the same number of elements
    # If not then the one of the steps ran on the same node which it shouldn't
#    if len(sacct_output_set) == node_count:
#        return True
#    return False


def sacct_verify_unique_nodes(sacct_string):
    sacct_output_list = sacct_string.split('\n')
    print("sacct_output_list = %s"%sacct_output_list)
    sacct_output_set = set() 
    print(sacct_output_list[2:-1])
    # The steps don't start until the second index
    for i in sacct_output_list[2:-1]:
        print("i = %s"%i)
        print("i.split('|')[0] = %s"%i.split('|')[0])
        if i.find('[') == -1:
            sacct_output_set.add(i.split('|')[0])
        else: ########k20r4n[02-04,06-09]
            index = i.find('[')
            prefix = i[:index]
            print("prefix = %s"%prefix)
            post = i[index+1:-1]
            print("post = %s"%post)
            if post.find(',') == -1:#####k20r4n[02-04]
                num = post.split('-')
                b = int(num[0])
                e = int(num[1])
                for i in range(b,e+1):
                    if i < 10:
                        strpo = '0'+str(i)
                    else:
                        strpo = str(i)
                    sacct_output_set.add(prefix+strpo)
            else:####k20r4n[02-04,06-09]
                num_list = post.split(',')
                print("num_list=%s"%num_list)
                for a in num_list:
                    n = a.split('-')
                    print(n)
                    b = int(n[0])
                    e = int(n[1])
                    for i in range(b, e + 1):
                        if i < 10:
                            strpo = '0' + str(i)
                        else:
                            strpo = str(i)
                        sacct_output_set.add(prefix + strpo)
    # Convert to set verify that the set and list have the same number of elements
    # If not then the one of the steps ran on the same node which it shouldn't
    if len(sacct_output_set) == node_count:
        return True
    return False


def test_exclusive(tmp_path):
    file_in = str(tmp_path / "exclusive.in")
    atf.make_bash_script(file_in, """srun -n1 --exclusive sleep 10 &
srun -n1 --exclusive sleep 10 &
srun -n1 --exclusive sleep 10 &
srun -n1 --exclusive sleep 10 &
srun -n1 --exclusive sleep 10 &
srun -n1 --exclusive sleep 10 &
srun -n1 --exclusive sleep 10 &
wait
    """)
    output = atf.run_command_output(f"sbatch -N{node_count} -t2 {file_in}")
    job_id = int(match.group(1)) if (match := re.search(r'(\d+)', output)) else None
    assert job_id is not None, "A job id was not returned"
    assert atf.wait_for_job_state(job_id, 'COMPLETED', timeout=60, poll_interval=2), f"Job ({job_id}) did not run"
    assert sacct_verify_unique_nodes(atf.run_command_output(f"sacct -j {job_id} --format=Nodelist --parsable2 --noheader")) is True, "Jobs ran on the same node"
