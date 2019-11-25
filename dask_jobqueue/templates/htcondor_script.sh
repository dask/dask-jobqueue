{{ shebang }}

MY.DaskWorkerName = "htcondor--$F(MY.JobId)--"
RequestCpus = MY.DaskWorkerCores
RequestMemory = floor(MY.DaskWorkerMemory / 1048576)
RequestDisk = floor(MY.DaskWorkerDisk / 1024)
MY.JobId = "$(ClusterId).$(ProcId)"
MY.DaskWorkerCores = {{ worker_cores }}
MY.DaskWorkerMemory = {{ worker_memory }}
MY.DaskWorkerDisk = {{ worker_disk }}
{%- if job_extra is not none -%}
{%- for k,v in job_extra.items() %}
{{ k }} = {{ v }}
{%- endfor -%}
{%- endif %}
{% if  log_directory is not none -%}
LogDirectory = {{ log_directory }}
Output = $(LogDirectory)/worker-$F(MY.JobId).out
Error = $(LogDirectory)/worker-$F(MY.JobId).err
Log = $(LogDirectory)/worker-$(ClusterId).log
Stream_Output = True
Stream_Error = True
{%- endif %}

Environment = "{{ env_extra | env_lines_to_dict | quote_environment }} JOB_ID=$F(MY.JobId)"
Arguments = "-c '{{ worker_command }}'"
Executable = {{ executable }}

Queue