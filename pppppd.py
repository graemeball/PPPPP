#!/usr/bin/env python

"""
PPPPP: Pluggable Polling Python Processing Process

Main pppppd.py script for running the polling daemon process.
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
import jobs

### CONFIG PARAMETERS
JOBS_AT = '/ngom/*/*.job.json'
POLL_DELAY = 10  # seconds, TODO: increase after testing

# define job handler in dict below for each job type
RUN = {'core2_decon.job': jobs.core2.run}


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
                print "new job: %s, started_jobs: %s" % (str(jobfile_path), str(started_jobs))
                jobtype = os.path.splitext(os.path.split(jobfile_path)[-1])[0]
                with open(jobfile_path) as f:
                    job = json.load(f)
                # TODO: job queue / DB, scheduling & multiple jobs
                # TODO: run jobs asynchronously
                # TODO: distributed processing
                results = RUN[jobtype](job, mode)
                result_path = os.path.splitext(os.path.splitext(jobfile_path)[0])[0]
                result_path += ".results.json"
                with open(result_path, "w") as fr:
                    fr.writelines(results)
                started_jobs.append(jobfile_path)
                print "wrote %s" % results
            else:
                print "no new jobs (sleep %ds), started_jobs: %s" % (POLL_DELAY, str(started_jobs))
                time.sleep(POLL_DELAY)

        except KeyError:
            print "unknown job type, %s" % jobtype

        except KeyboardInterrupt:
            print "\n\nShutting down PPPPP daemon!\n"
            sys.exit()


def get_next_job():
    """Return first job file we find, since we run 1-at-a-time for now"""
    jobs = glob.glob(JOBS_AT)
    if len(jobs) > 0:
        return jobs[0]
    else:
        return None


## test functions

def _test_get_next_job():
    print get_next_job()


if __name__ == '__main__':
    #_test_get_next_job()
    main()
