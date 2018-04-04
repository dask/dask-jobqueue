from dask_jobqueue import PBSCluster

#No mark in order to run these simple unit tests all the time


def test_header():
    with PBSCluster(walltime='00:02:00', processes=4, threads=2, memory='7GB') as cluster:

        assert '#PBS' in cluster.job_header
        assert '#PBS -N dask-worker' in cluster.job_header
        assert '#PBS -l select=1:ncpus=8:mem=27GB' in cluster.job_header
        assert '#PBS -l walltime=00:02:00' in cluster.job_header
        assert '#PBS -q' not in cluster.job_header
        assert '#PBS -A' not in cluster.job_header

    with PBSCluster(queue='regular', project='DaskOnPBS', processes=4, threads=2, memory='7GB',
                    resource_spec='select=1:ncpus=24:mem=100GB') as cluster:

        assert '#PBS -q regular' in cluster.job_header
        assert '#PBS -N dask-worker' in cluster.job_header
        assert '#PBS -l select=1:ncpus=24:mem=100GB' in cluster.job_header
        assert '#PBS -l select=1:ncpus=8:mem=27GB' not in cluster.job_header
        assert '#PBS -l walltime=' in cluster.job_header
        assert '#PBS -A DaskOnPBS' in cluster.job_header

    with PBSCluster() as cluster:

        assert '#PBS -j oe' not in cluster.job_header
        assert '#PBS -N' in cluster.job_header
        assert '#PBS -l select=1:ncpus=' in cluster.job_header
        assert '#PBS -l walltime=' in cluster.job_header
        assert '#PBS -A' not in cluster.job_header
        assert '#PBS -q' not in cluster.job_header

    with PBSCluster(job_extra=['-j oe']) as cluster:

        assert '#PBS -j oe' in cluster.job_header
        assert '#PBS -N' in cluster.job_header
        assert '#PBS -l select=1:ncpus=' in cluster.job_header
        assert '#PBS -l walltime=' in cluster.job_header
        assert '#PBS -A' not in cluster.job_header
        assert '#PBS -q' not in cluster.job_header


def test_job_script():
    with PBSCluster(walltime='00:02:00', processes=4, threads=2, memory='7GB') as cluster:

        job_script = cluster.job_script()
        assert '#PBS' in job_script
        assert '#PBS -N dask-worker' in job_script
        assert '#PBS -l select=1:ncpus=8:mem=27GB' in job_script
        assert '#PBS -l walltime=00:02:00' in job_script
        assert '#PBS -q' not in job_script
        assert '#PBS -A' not in job_script

        assert '/dask-worker tcp://' in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7GB' in job_script

    with PBSCluster(queue='regular', project='DaskOnPBS', processes=4, threads=2, memory='7GB',
                    resource_spec='select=1:ncpus=24:mem=100GB') as cluster:

        job_script = cluster.job_script()
        assert '#PBS -q regular' in job_script
        assert '#PBS -N dask-worker' in job_script
        assert '#PBS -l select=1:ncpus=24:mem=100GB' in job_script
        assert '#PBS -l select=1:ncpus=8:mem=27GB' not in job_script
        assert '#PBS -l walltime=' in job_script
        assert '#PBS -A DaskOnPBS' in job_script

        assert '/dask-worker tcp://' in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7GB' in job_script
