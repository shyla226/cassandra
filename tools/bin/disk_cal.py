#
# Copyright DataStax, Inc.
#
# Please see the included license file for details.
#
# A utility for exploring AIO reads.
#
# It conducts a search for the ideal AIO queue depth using the fio utility. It examines values
# that range from 1 to 128 using a single read process. It reports IO ops per second and latency
# in both csv and graphical form. You should notice that at some point increasing the queue depth
# only increases the latency but not the IO ops per second. This is the ideal queue depth, which
# achieves the maximum number of IO ops with the smallest latency.
#
# You can also fix a queue depth with the -q parameter. In this case it will perform a search for
# the ideal number of parallel jobs, by examining from 1 to num cores jobs, or to io depth jobs if
# the io depth is less than the number of cores on the machine. Normally in this case there shouldn't be
# much of a difference, or at least none was seen from the cases analyzed so far.
#
# This script requires python 2 and fio, for plotting also matplotlib and numpy.

import json
import math
import multiprocessing
import subprocess
import sys
import time

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-d", "--directory", dest="directory",
                  help="The directory to test, default is the current directory", default=".")
parser.add_option("-b", "--buffer-size", dest="buffer_size",
                  help="The size of the buffer to read, default is 64k", default="64k")
parser.add_option("-s", "--size", dest="size",
                  help="The size of the file to read, default is 10G", default="10G")
parser.add_option("-j", "--job-name", dest="jobname",
                  help="The fio job name, only randread has been tested and only reads are supported",
                  default="randread")
parser.add_option("-q", "--io-queue-depth", dest="io_depth",
                  help="The io depth, leave unspecified to find the optimal io depth, "
                        "then use the optimal io depth to find the optimal number of parallel readers",
                  default=0, type="int")
parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                  help="Verbose mode, will also save fio results for each operation", default=False)

(options, args) = parser.parse_args()
if options.verbose:
    print options

num_cores = multiprocessing.cpu_count()
if options.verbose:
    print 'Detected {} cores'.format(num_cores)

try:
    import matplotlib
    matplotlib.use('svg')

    import matplotlib.pyplot as plt
    import numpy as np
    can_plot = True
except ImportError as e:
    can_plot = False
    print "{}, no graph will be generated".format(e.message)


def run_fio(jobs, result_file):
    """
    Executes fio for each job and writes the combined results in the result file.

    :param jobs: a list of tuples, where the left item is the number of workers and the right item is the io depth
    :param result_file: the file where to write the combined results
    """
    tot_workers = sum(j[0] for j in jobs)
    tot_depth = sum(j[0] * j[1] for j in jobs)
    if options.verbose:
        print 'Jobs distribution: {}'.format(jobs)
    sys.stdout.write('Testing with tot. {} workers and IO depth {}...'.format(tot_workers, tot_depth))
    sys.stdout.flush()

    procs = []
    for job in jobs:
        workers = job[0]
        depth = job[1]
        job_name = '{}.{}.{}'.format(options.jobname, workers, depth)
        args = ['fio',
                '--name={}'.format(job_name),
                '--directory={}'.format(options.directory),
                '--filename={}.fio.tmp'.format(options.jobname),
                '--runtime=10',
                '--size={}'.format(options.size),
                '--ioengine=libaio',
                '--iodepth={}'.format(depth),
                '--rw={}'.format(options.jobname),
                '--bs={}'.format(options.buffer_size),
                '--direct=1',
                '--buffered=0',
                '--output-format=json']
        if workers > 1:
            args.append('--numjobs={}'.format(workers))
            args.append('--group_reporting')

        procs.append(subprocess.Popen(args, stdout=subprocess.PIPE))

    iops = []
    lat_min = []
    lat_mean = []
    lat_max = []
    lat_stddev = []
    for proc, job in zip(procs, jobs):
        job_name = '{}.{}.{}'.format(options.jobname, job[0], job[1])
        out, err = proc.communicate()
        if err:
            print err

        if options.verbose:
            with open('{}.fio.json'.format(job_name), 'w') as rf:
                rf.write(out)

        result = json.loads(out)['jobs'][0]
        iops.append(float(result['read']['iops']))
        lat_min.append(float(result['read']['lat']['min']))
        lat_mean.append(float(result['read']['lat']['mean']))
        lat_max.append(float(result['read']['lat']['max']))
        lat_stddev.append(float(result['read']['lat']['stddev']))

    sys.stdout.write('... => {} iops, {} lat us\n'.format(sum(iops), float(sum(lat_mean)) / len(lat_mean)))
    result_file.write('{},{},{},{},{},{},{}\n'.format(tot_depth,
                                                      tot_workers,
                                                      sum(iops),
                                                      min(lat_min),
                                                      float(sum(lat_mean)) / len(lat_mean),
                                                      max(lat_max),
                                                      # TODO - we really want to combine stddev rather than avg.ing them
                                                      float(sum(lat_stddev)) / len(lat_stddev)))


def write_result_header(result_file):
    result_file.write('io depth,njobs,iops,lat usecs\n')
    result_file.write('io depth,njobs,iops,min,avg,max,stddev\n')


def run_depth_test():
    csv_output_file_name = 'disk_cal_depth.{}.csv'.format(time.strftime('%H%M%S'))
    print 'Testing IO depth from 1 to 128, saving results to {}'.format(csv_output_file_name)

    with open(csv_output_file_name, 'w') as result_file:
        write_result_header(result_file)

        io_depth = 1
        while io_depth <= 128:
            run_fio([(1, io_depth)], result_file)
            io_depth = int(max(math.ceil(io_depth * 1.1), io_depth + 1))

    return csv_output_file_name


def run_jobs_test():
    csv_output_file_name = 'disk_cal_{}_jobs.{}.csv'.format(options.io_depth, time.strftime('%H%M%S'))
    io_depth = options.io_depth
    nworkers = min(num_cores, io_depth)
    print 'Testing IO depth {} with 1 to {} workers, saving results to {}'\
        .format(io_depth, nworkers, csv_output_file_name)

    with open(csv_output_file_name, 'w') as result_file:
        write_result_header(result_file)

        for workers in range(1, nworkers + 1):
            depth = io_depth / workers
            remainder = io_depth % workers
            jobs = [(workers - remainder, depth)]
            if remainder > 0:
                jobs.append((remainder, depth + 1))

            run_fio(jobs, result_file)

    return csv_output_file_name


def plot(csv_output_file_name):
    svg_output_file_name = csv_output_file_name.replace('.csv', '.svg')
    print 'Print plotting data to {}...'.format(svg_output_file_name)

    data = np.genfromtxt(csv_output_file_name, delimiter=',', skip_header=2,
                         names=['depth', 'njobs', 'iops', 'minlat', 'lat', 'maxlat', 'std'])

    fig, ax1 = plt.subplots()
    x = 'depth' if options.io_depth <= 0 else 'njobs'
    ax1.set_xlabel(x)

    ax1.plot(data[x], data['iops'], 'b-+')
    ax1.tick_params('y', colors='b')
    ax1.set_ylabel('{} read ops/second'.format(options.buffer_size), color='b')

    # find the point where the delta iops start to become < 1
    if x == 'depth':
        deltas = np.diff(data['iops'])
        small_deltas = np.argwhere(deltas < 1)
        if small_deltas.size > 1:
            max_index = small_deltas[0]
            print 'Found first increment less than one at depth {}'.format(data[x][max_index])
            ax1.vlines(x=data[x][max_index], ymin=0, ymax=data['iops'][max_index], linestyle='--', color='k')

    ax2 = ax1.twinx()
    ax2.plot(data[x], data['lat'], 'r-+')
    ax2.errorbar(data[x], data['lat'], yerr=data['std'], color='r')
    ax2.tick_params('y', colors='r')
    ax2.set_ylabel('avg lat (us)', color='r')

    plt.savefig(svg_output_file_name, bbox_inches='tight', pad_inches=0.2)


csv_output = run_depth_test() if options.io_depth <= 0 else run_jobs_test()

if can_plot:
    plot(csv_output)
