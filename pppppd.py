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
import shutil
import commands

### CONFIG PARAMETERS
LOCAL_JOB_DIR = '/ngom'
REMOTE_JOB_DIR = '/ngom'
JOB_GLOB = os.path.join(LOCAL_JOB_DIR, '*/*.jobs')
POLL_DELAY = 10  # seconds, TODO: increase after testing
LASTALIVE = 'lastalive.txt'

# define job handler in dict below for each job type
RUN = {'core2_decon': commands.core2.run}


def main():
    modes = ["fake", "fail", "local", "grid"]
    if len(sys.argv) != 2 or sys.argv[1] not in modes:
        print "\n\nUsage: %s [mode]   # valid modes: %s\n" % \
            (sys.argv[0], ", ".join(modes))
        return
    mode = sys.argv[1]
    if mode == "grid":
        print "\ngrid processing not implemented!"  # TODO
        sys.exit()
    started_jobs = []
    while True:
        try:
            jobfile_path = get_next_job(started_jobs)
            if jobfile_path is not None:
                print "new job: %s, started_jobs: %s" % (str(jobfile_path),
                                                         str(started_jobs))
                started_jobs.append(jobfile_path)
                jobfile_path = remote_to_local_path(jobfile_path)
                with open(jobfile_path) as f:
                    jobs = [json.loads(l.rstrip()) for l in f.readlines()]
                result_path = os.path.splitext(jobfile_path)[0] + ".results"
                results = []
                with open(result_path, "w") as fr:
                    # TODO: job queue / DB, scheduling
                    # TODO: run jobs in parallel
                    # TODO: distributed processing
                    for job in jobs:
                        cleanup_if_dead(jobfile_path)
                        # for now, take only first command in list for each job
                        for com in [job[0]]:
                            command = com['command']
                            for inp in com['inputs']:
                                inp['path'] = remote_to_local_path(inp['path'])
                            result = RUN[command](com, mode)
                            result['results'] = [local_to_remote_path(r) for r in result['results']]
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
            # TODO: signal in results that command unknown

        except IOError as e:
            print "IOError, abandon job!\n %s" % str(e)
            # TODO: signal in results that IOError has occurred

        except KeyboardInterrupt:
            print "\n\nShutting down PPPPP daemon!\n"
            sys.exit()


def get_next_job(started_jobs):
    """Return first job file found that has not already been started"""
    jobs = glob.glob(JOB_GLOB)
    if len(jobs) > 0:
        for j in jobs:
            if j not in started_jobs:
                return j
    return None

def cleanup_if_dead(jobpath):
    """Remove job folder if lastalive time implies OMERO session dead"""
    jobdir = os.path.split(jobpath)[0]
    lastalive_path = os.path.join(jobdir, LASTALIVE)
    with open(lastalive_path, "r") as f:
        la_info = [float(l.rstrip()) for l in f.readlines()]
    print "lastalive %fs (pulse %f)" % tuple(la_info)
    if time.time() - la_info[0] > la_info[1]:
        # perform basic sanity checks before removing!
        if jobdir is not None and jobdir.startswith(LOCAL_JOB_DIR):
            if os.path.exists(jobdir):
                shutil.rmtree(jobdir, ignore_errors=True)  # sometimes fails
                    
        raise IOError("OMERO session for %s died!" % jobpath)


def remote_to_local_path(fullpath):
    path, filename = os.path.split(fullpath)
    jobfolder = os.path.split(path)[-1]
    return os.path.join(LOCAL_JOB_DIR, jobfolder, filename)


def local_to_remote_path(fullpath):
    path, filename = os.path.split(fullpath)
    jobfolder = os.path.split(path)[-1]
    return os.path.join(REMOTE_JOB_DIR, jobfolder, filename)


## test functions

def _test_get_next_job():
    import shutil
    dirs = [os.path.join(LOCAL_JOB_DIR, "test%d" % n) for n in range(3)]
    for d in dirs:
        os.mkdir(d)
        with open(os.path.join(d, "test_job.jobs"), 'w+') as f:
            f.write("[]")
    started_jobs = []
    for i in range(len(dirs) + 1):
        jobfile_path = get_next_job(started_jobs)
        print "get_next_job call %d returned %s" % (i, str(jobfile_path))
        started_jobs.append(jobfile_path)
    for d in dirs:
        shutil.rmtree(d)
    nj = get_next_job(started_jobs)
    print "final get_next_job() call with no test jobs returned %s" % nj



def _test_remote_to_local_path():
    remote_path = '/Volumes/dif/gball/root_2016-06-22_12-26-45_CPY/BPAE_514_001_ID16.dv'
    print "\ntesting remote_to_local_path(%s):" % remote_path
    print remote_to_local_path(remote_path)

def _test_local_to_remote_path():
    local_path = '/dif/users/gball/root_2016-06-22_12-26-45_CPY/BPAE_514_001_ID16.dv'
    print "\ntesting local_to_remote_path(%s):" % local_path
    print local_to_remote_path(local_path)


if __name__ == '__main__':
    #_test_get_next_job()
    #_test_remote_to_local_path()
    #_test_local_to_remote_path()
    main()
