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
JOB_DIR = '/ngom/'
JOB_GLOB = os.path.join(JOB_DIR, '*/*.jobs')
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
                #jobtype = os.path.basename(jobfile_path).split('.')[0]
                with open(jobfile_path) as f:
                    jobs = [json.loads(l.rstrip()) for l in f.readlines()]
                result_path = os.path.splitext(jobfile_path)[0] + ".results"
                results = []
                with open(result_path, "w") as fr:
                    # TODO: job queue / DB, scheduling
                    # TODO: run jobs in parallel
                    # TODO: distributed processing
                    for job in jobs:
                        # for now, take only first command in list for each job
                        for com in [job[0]]:
                            command = com['command']
                            result = RUN[command](com, mode, JOB_DIR)
                            result = json.dumps(result) + "\n"
                            fr.write(result)
                            results.append(result)
                print "wrote results to %s:\n%s" % (result_path, results)
            else:
                print "no new jobs (sleep %ds), started_jobs: %s" % \
                    (POLL_DELAY, str(started_jobs))
                time.sleep(POLL_DELAY)

        except KeyError:
            print "unknown command, %s" % command

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


## test functions

def _test_get_next_job():
    print get_next_job()


if __name__ == '__main__':
    #_test_get_next_job()
    main()
