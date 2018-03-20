# job runner

gpu job schedule via file system.

0. do some cleaning work
    1. if there is a free gpu, and a running job file marked that gpu, then remark the file as `[{$GPU}UNKNOWN{$TIME}]`
1. when there is a free gpu
2. pick a job file (shell script) in `jobs/queue/`
3. move the job under `jobs/` and mark as `[{$GPU}RUN{$TIME}]`
4. start that job and redirect output to job's log file
5. if fail, or exception raised, mark the job as `[{$GPU}ERROR{$TIME}]`
6. if success, mark as `[DONE{$TIME}]`, and move job file and log file to `jobs/done/`
