#!/usr/bin/env python

"""
core2.py module for running Priism ER deconvolution jobs
"""

__author__ = "Graeme Ball (g.ball@dundee.ac.uk)"
__copyright__ = "Copyright (c) 2016 Graeme Ball (Dundee Imaging Facility)"
__license__ = "GPL v3"  # http://www.gnu.org/licenses/gpl.txt


import os
import numpy as np
from collections import OrderedDict
import subprocess
import time
import shutil


### CONFIG PARAMETERS
PRIISM_SETUP = '/Users/gball/build/priism-4.2.9/Priism_setup.sh'
OTF_PATH = '/Users/gball/Documents/TestData/Deconvolution/otf'
FAKE_DELAY = 20  # sleep this many seconds when run in fake mode


def run(job, mode):
    """Run a core2 decon job (Priism)"""
    inp = job['inputs'][0]  # 1st input (only 1 needed!)
    if mode == "fake":
        print "fake core2 decon job: %s" % str(job)
        com, log, dv = fake_run(inp['path'])
    else:
        try:
            com, log, dv = generate_core2_com(
                inp['path'],
                job['par.alpha'],
                job['par.lamf'],
                job['par.niter'])
            exec_priism_com(com)
        except KeyError as ek:
            print str(ek)
        except RuntimeError as er:
            print str(er)
    result = {}
    result['results'] = [dv, com, log]
    result['inputID'] = inp['imageID']
    result['datasetID'] = inp['datasetID']
    #result['attachments'] = [com, log]
    return result


def fake_run(inp_path):
    """Fake processing by copying input, writing dummy com, log and sleeping"""
    base = os.path.splitext(inp_path)[0]
    com_path = base + "_ERD.com"
    log_path = base + "_ERD.log"
    decon_path = base + "_ERD.dv"
    with open(com_path, 'w') as fc:
        fc.write("# core2 decon dummy com file")
    with open(log_path, 'w') as fl:
        fl.write("# core2 decon dummy log file")
    shutil.copy(inp_path, decon_path)
    time.sleep(FAKE_DELAY)
    return com_path, log_path, decon_path


def generate_core2_com(path, alpha, lamf, niter):
    """
    Generate Priism .com script to run a core2 decon job.
    Raises: KeyError where OTF is not recognized
    Returns: paths to .com, .log and output .dv files
    """
    base = os.path.splitext(path)[0]
    com_path = base + "_ERD.com"
    log_path = base + "_ERD.log"
    decon_path = base + "_ERD.dv"

    otf = os.path.join(OTF_PATH, _lookup_otf(path))
    script_text = """\
#!/bin/sh
#Setting run time environment...
. '{priism}';
#command file for core2_decon
( time core2_decon \\
 "{base}.dv" \\
 "{base}_ERD.dv" \\
 "{otf}" \\
 -alpha={alpha} -lamratio=0:1 -lamf={lamf} -lampc=100 \\
 -lampos=1 -lamsmooth=100 -cuth=0.001 -na=1.4 -nimm=1.512 -ncycl={niter} \\
 -nzpad=64 -omega=0.8 -sub=1:1:1:1:1 -tol=0.0001 -np=4 -oplotfile="" ) \\
 >"{base}_ERD.log" 2>&1
""".format(priism=PRIISM_SETUP, base=base, otf=otf,
           alpha=alpha, lamf=lamf, niter=niter)
    with open(com_path, 'w') as com_file:
        print >> com_file, script_text

    return com_path, log_path, decon_path


def exec_priism_com(com_script):
    """Execute a Priism .com script, returning when done."""
    ssh = subprocess.Popen(["sh", com_script],
                           shell=False, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()
    result.extend(ssh.stderr.readlines())
    print "priism command result: %s " % str(result)


## helper functions and data for generate_core2_decon function

# Imsubs file header, see http://msg.ucsf.edu/IVE/IVE4_HTML/IM_ref2.html
DV_HDR_STRUCT = [
    ('3i4', 'Num'),
    ('1i4', 'PixelType'),
    ('3i4', 'mst'),
    ('3i4', 'm'),
    ('3f4', 'd'),
    ('3f4', 'angle'),
    ('3i4', 'axis'),
    ('3f4', 'mmm1'),
    ('1i4', 'nspg'),
    ('1i4', 'next'),
    ('1i2', 'dvid'),
    ('1i2', 'nblank'),
    ('1i4', 'ntst'),
    ('24a1', 'blank'),
    ('1i2', 'NumIntegers'),
    ('1i2', 'NumFloats'),
    ('1i2', 'sub'),
    ('1i2', 'zfac'),
    ('2f4', 'mm2'),
    ('2f4', 'mm3'),
    ('2f4', 'mm4'),
    ('1i2', 'type'),
    ('1i2', 'LensNum'),
    ('1i2', 'n1'),
    ('1i2', 'n2'),
    ('1i2', 'v1'),
    ('1i2', 'v2'),
    ('2f4', 'mm5'),
    ('1i2', 'NumTimes'),
    ('1i2', 'ImgSequence'),
    ('3f4', 'tilt'),
    ('1i2', 'NumWaves'),
    ('5i2', 'wave'),
    ('3f4', 'zxy0'),
    ('1i4', 'NumTitles'),
    ('10a80', 'Titles')
]

OTFs = {'10612': "Olympus_60X_142_10612.otf",
        '10205': "Olympus_20X_075_10205.otf",
        '10603': "Olympus_60X_120_10603.otf",
        '12003': "Nikon_100X_140_12003.otf",
        '2003': "Olympus_20X_085_2003.otf",
        '130': "Nikon_40X_130.otf",
        '10602': "Olympus_60X_140_10602.otf",
        '10404': "Olympus_40X_085_10404.otf",
        '12601': "Nikon_60X_140_12601.otf",
        '10410': "Olympus_40X_115_10410.otf",
        '14003': "Zeiss_100X_140_14003.otf",
        '10003': "Olympus_100X_135_10003.otf",
        #'2002': "Olympus_40X_125_2002kgs.otf",
        #'2002': "Olympus_40X_125_2002.otf",
        '14401': "Zeiss_40X_130_14401.otf",
        '10005': "Olympus_100X_135_10005.otf",
        '2002': "Olympus_40X_130_2002.otf",
        '14601': "Zeiss_63X_140_14601.otf",
        '10002': "Olympus_100X_140_10002.otf",
        '10403': "Olympus_40X_135_10403.otf",
        '10007': "Olympus_100X_140_10007.otf"}


def _dv_hdr_lookup(filepath, hdr_field_name):
    """Read a DeltaVision header field."""
    # TODO1, basic file validation before reading header? raise TypeError?
    with open(filepath, 'rb') as dv_file:
        formats = [fmt_lbl[0] for fmt_lbl in DV_HDR_STRUCT]
        labels = [fmt_lbl[1] for fmt_lbl in DV_HDR_STRUCT]
        hdr_arr = np.rec.fromfile(dv_file, formats=formats, shape=1)
        hdr = OrderedDict(zip(labels, hdr_arr[0]))
    return hdr[hdr_field_name]


def _lookup_otf(r3d_path):
    """Read OTF id from .dv file header and look up OTF file name."""
    lens_num = _dv_hdr_lookup(r3d_path, 'LensNum')
    if str(lens_num) not in OTFs:
        raise KeyError("OTF for LensNum=%d not found" % lens_num)
    return OTFs[str(lens_num)]


## test functions

def _test_gen_core2():
    path = os.path.join(os.getcwd(), 'test_data/BPAE_514_001.dv')
    com, log, dv = generate_core2_com(path, 1000, 0.5, 2)
    print com, log, dv


if __name__ == '__main__':
    # run module as script to test
    _test_gen_core2()
