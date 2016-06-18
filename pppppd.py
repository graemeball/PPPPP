#!/usr/bin/env python

"""
PPPPP: Pluggable Polling Python Processing Program

Main pppppd.py script for running the polling daemon.
Run without args for usage info.

"""

__author__ = "Graeme Ball (graemeball@gmail.com)"
__copyright__ = "Copyright (c) 2016 Graeme Ball"
__license__ = "GPL v3"  # http://www.gnu.org/licenses/gpl.txt


import sys
import os
import json
import time
import glob
import commands

### CONFIG PARAMETERS
JOB_LOCATION = '/ngom/'
JOB_GLOB = os.path.join(JOB_LOCATION, '*/*.job.json')
POLL_DELAY = 10  # seconds, TODO: increase after testing

# define job handler in dict below for each job type
RUN = {'core2_decon': commands.core2.run}


def main():
    modes = ["process", "fake"]
    if len(sys.argv) != 2 or sys.argv[1] not in modes:
        print "\n\nUsage: %s [mode]   # valid modes: %s\n" % \
            (sys.argv[0], ", ".join(modes))
        return
    mode = sys.argv[1]
    started_jobs = []
    while True:
        try:
            jobfile_path = get_next_job()
            if jobfile_path is not None and jobfile_path not in started_jobs:
                print "new job: %s, started_jobs: %s" % (str(jobfile_path),
                                                         str(started_jobs))
                started_jobs.append(jobfile_path)
                jobtype = os.path.basename(jobfile_path).split('.')[0]
                with open(jobfile_path) as f:
                    jobs = json.load(f)
                result_path = os.path.dirname(jobfile_path)
                result_filename = jobtype + ".results.json"
                result_path = os.path.join(result_path, result_filename)
                results = []
                # TODO: job queue / DB, scheduling & multiple jobs
                # TODO: run jobs asynchronously
                # TODO: distributed processing
                with open(result_path, "w") as fr:
                    for job in split_jobs(jobs):
                        result = RUN[jobtype](job, mode, JOB_LOCATION)
                        result = json.dumps(result) + "\n"
                        fr.write(result)
                        results.append(result)
                print "wrote results to %s:\n%s" % (result_path, results)
            else:
                print "no new jobs (sleep %ds), started_jobs: %s" % \
                    (POLL_DELAY, str(started_jobs))
                time.sleep(POLL_DELAY)

        except KeyError:
            print "unknown job type, %s" % jobtype

        except IOError as e:
            print "IOError, abandon job!\n %s" % str(e)

        except KeyboardInterrupt:
            print "\n\nShutting down PPPPP daemon!\n"
            sys.exit()


def get_next_job():
    """Return first job file we find, since we run 1-at-a-time for now"""
    jobs = glob.glob(JOB_GLOB)
    if len(jobs) > 0:
        return jobs[0]
    else:
        return None


def split_jobs(jobs):
    """Return a list of single job dicts from a multi-job task."""
    inputs = jobs.pop('inputs')  # 'jobs' just has the paramters now
    joblist = []
    for inp in inputs:
        job = dict(jobs)
        job['input'] = inp
        joblist.append(job)
    return joblist


## test functions

def _test_get_next_job():
    print get_next_job()


def _test_split_jobs():
    with open('commands/test_data/core2_decon.job.json') as f:
        jobs = json.load(f)
    print str(split_jobs(jobs))


if __name__ == '__main__':
    #_test_get_next_job()
    #_test_split_jobs()
    main()
