Pluggable Polling Python Processing Process
===========================================

A python daemon to run headless processing jobs on a grid of processing
nodes. PPPPP finds new jobs by polling a shared FS location for folders that
contain new "job definition files". An SQL database is used for queuing /
scheduling, and remote jobs are launched via SSH.


Plan + Status
-------------

Planned features, with current implementation status (+) / (-):

* poll shared FS for new jobs (+)
* maintain an SQL-based job queue (-)
* run jobs asynchronously (-)
* fair scheduling mechanism, with job pausing (-)
* run jobs locally (+)
* run jobs remotely via SSH (-)


Installation & Setup
--------------------

Developed using python 2.7, and requires 2.6 for base functionality
(2.6 requires altered imports for some jobs).

A job-handling module foreach job type is defined in jobs/
(for new jobs, update RUN dict in pppppd.py and jobs/__init__.py)

