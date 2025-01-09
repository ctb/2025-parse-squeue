#! /cvmfs/hpc.ucdavis.edu/sw/conda/root/bin/python
import subprocess
import argparse
import pprint


PARTITION_REPORT_ORDER = ['high2', 'med2', 'bmh', 'bmm']


def parse_squeue_output():
    """Parses the output of squeue and returns a list of dictionaries."""

    # Run the squeue command and capture the output
    result = subprocess.run(['squeue', '-A', 'ctbrowngrp', '--noconvert'], stdout=subprocess.PIPE, text=True)
    output = result.stdout.splitlines()

    # Parse the header line to get the column names
    header = output[0].split()

    # Parse the remaining lines as job records
    jobs = []
    for line in output[1:]:
        values = line.split()
        job = dict(zip(header, values))
        jobs.append(job)

    return jobs


def convert_mem_to_gb(job):
    mem = job['MIN_ME']
    if mem.endswith('M'): # mb -> gb
        mem = int(mem[:-1]) / 1024
    elif mem.endswith('G'): # gb
        mem = int(mem[:-1])
    else:               # gb
        mem = int(mem) / 1024

    return mem
    


def main():
    p = argparse.ArgumentParser()
    p.add_argument('-u', '--user', help='filter on user')
    p.add_argument('-p', '--partition', help='filter on partition')
    args = p.parse_args()

    if args.user:
        print(f'(filtering on user {args.user})')

    if args.partition:
        print(f'(filtering on partition {args.partition})')
    
    # Get the list of jobs
    jobs = parse_squeue_output()

    partitions = {}
    users = {}

    # Print the jobs
    for job in jobs:
        user = job['USER']
        partition = job['PARTITION']

        assert job['NODES'] == '1', job # simplify our lives
        if args.user and user != args.user:
            continue
        if args.partition and partition != args.partition:
            continue
            
        if job['ST'] == 'R':    # simplify our lives: running jobs only

            # update partition counts
            d = partitions.get(partition, dict(MEM=0, CPU=0))
            d['CPU'] = d['CPU'] + int(job['CPU'])
            d['MEM'] = d['MEM'] + convert_mem_to_gb(job)
            partitions[partition] = d

            # update user counts
            d = users.get(user, dict(MEM=0, CPU=0))
            d['CPU'] = d['CPU'] + int(job['CPU'])
            d['MEM'] = d['MEM'] + convert_mem_to_gb(job)
            users[user] = d

    print('PARTITION REPORT:')

    all_partitions = set(partitions) - set(PARTITION_REPORT_ORDER)
    for partition in PARTITION_REPORT_ORDER + list(all_partitions):
        values = partitions.get(partition, dict(MEM=0, CPU=0))
        mem = values['MEM']
        cpu = values['CPU']
        print(f"   partition {partition:8}: {mem:-6.1f} GB used, {cpu:-3} processors")

    print('')
    print('USER REPORT:')
    for user, values in sorted(users.items(),
                               key=lambda x: (x[1]['MEM'], x[1]['CPU']),
                               reverse=True):
        mem = values['MEM']
        cpu = values['CPU']
        print(f"   user {user:13}: {mem:-6.1f} GB used, {cpu:-3} processors")
        

if __name__ == '__main__':
    main()
