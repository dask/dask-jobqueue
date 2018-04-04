from dask_jobqueue import SLURMCluster

#No mark in order to run these simple unit tests all the time


def test_header():
    with SLURMCluster(walltime='00:02:00', processes=4, threads=2, memory='7GB') as cluster:

        assert '#SBATCH' in cluster.job_header
        assert '#SBATCH -J dask-worker' in cluster.job_header
        assert '#SBATCH -n 1' in cluster.job_header
        assert '#SBATCH --cpus-per-task=8' in cluster.job_header
        assert '#SBATCH --mem=27G' in cluster.job_header
        assert '#SBATCH -t 00:02:00' in cluster.job_header
        assert '#SBATCH -p' not in cluster.job_header
        assert '#SBATCH -A' not in cluster.job_header

    with SLURMCluster(queue='regular', project='DaskOnPBS', processes=4, threads=2, memory='7GB',
                      job_cpu=16, job_mem='100G') as cluster:

        assert '#SBATCH --cpus-per-task=16' in cluster.job_header
        assert '#SBATCH --cpus-per-task=8' not in cluster.job_header
        assert '#SBATCH --mem=100G' in cluster.job_header
        assert '#SBATCH -t ' in cluster.job_header
        assert '#SBATCH -A DaskOnPBS' in cluster.job_header
        assert '#SBATCH -p regular' in cluster.job_header

    with SLURMCluster() as cluster:

        assert '#SBATCH' in cluster.job_header
        assert '#SBATCH -J ' in cluster.job_header
        assert '#SBATCH -n 1' in cluster.job_header
        assert '#SBATCH --cpus-per-task=' in cluster.job_header
        assert '#SBATCH --mem=' in cluster.job_header
        assert '#SBATCH -t ' in cluster.job_header
        assert '#SBATCH -p' not in cluster.job_header
        assert '#SBATCH -A' not in cluster.job_header


def test_job_script():
    with SLURMCluster(walltime='00:02:00', processes=4, threads=2, memory='7GB') as cluster:

        job_script = cluster.job_script()
        assert '#SBATCH' in job_script
        assert '#SBATCH -J dask-worker' in job_script
        assert '#SBATCH -n 1' in job_script
        assert '#SBATCH --cpus-per-task=8' in job_script
        assert '#SBATCH --mem=27G' in job_script
        assert '#SBATCH -t 00:02:00' in job_script
        assert '#SBATCH -p' not in job_script
        assert '#SBATCH -A' not in job_script

        assert 'export ' not in job_script

        assert '/dask-worker tcp://' in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7GB' in job_script

    with SLURMCluster(walltime='00:02:00', processes=4, threads=2, memory='7GB',
                      env_extra=['export LANG="en_US.utf8"', 'export LANGUAGE="en_US.utf8"',
                                 'export LC_ALL="en_US.utf8"']
                      ) as cluster:
        job_script = cluster.job_script()
        assert '#SBATCH' in job_script
        assert '#SBATCH -J dask-worker' in job_script
        assert '#SBATCH -n 1' in job_script
        assert '#SBATCH --cpus-per-task=8' in job_script
        assert '#SBATCH --mem=27G' in job_script
        assert '#SBATCH -t 00:02:00' in job_script
        assert '#SBATCH -p' not in job_script
        assert '#SBATCH -A' not in job_script

        assert 'export LANG="en_US.utf8"' in job_script
        assert 'export LANGUAGE="en_US.utf8"' in job_script
        assert 'export LC_ALL="en_US.utf8"' in job_script

        assert '/dask-worker tcp://' in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7GB' in job_script
