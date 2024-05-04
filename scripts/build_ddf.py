#!/usr/bin/env python

"""Creates dataset definition files."""

from __future__ import division, print_function
import argparse
from collections import namedtuple
import os
import re
import subprocess
import shutil
import sys
from uuid import uuid4
from multiprocessing import Pool, cpu_count
import uproot
import numpy as np

import ROOT
import yaml



WeightInfo = namedtuple(
    'WeightInfo', ['lhe_scale_present', 'ps_present']
)

def check_weights(files):
    """Check if generator weights of different types are present.

    Arguments:
        files:  Iterable with paths to NanoAOD files.

    Return value:
        An instance of WeightInfo.
    """

    lhe_scale_present = True
    ps_present = True
    all_empty = True

    # Loop over all files in case they come from different datasets with
    # different weight settings.  Mark the weights as present only if
    # they are found in all files.
    for path in files:
        input_file = ROOT.TFile(path)
        tree = input_file.Get('Events')
        if tree.GetEntries() == 0:
            continue

        tree.SetBranchStatus('*', False)
        for branch in ['nLHEScaleWeight', 'nPSWeight']:
            tree.SetBranchStatus(branch, True)

        # Only check the first event as the weight settings should be
        # the same for all events in a given dataset
        event = next(iter(tree))
        all_empty = False

        # In some cases these arrays contain single dummy entries.  For
        # this reason compare the lengths of the arrays against 1 and
        # not 0.
        if event.nLHEScaleWeight <= 1:
            lhe_scale_present = False
        if event.nPSWeight <= 1:
            ps_present = False

        input_file.Close()

    if all_empty:
        lhe_scale_present = False
        ps_present = False

    return WeightInfo(lhe_scale_present, ps_present)


def process_file(path):
    """Process a single ROOT file using uproot to count events and sum weights."""
    with uproot.open(path) as file:
        tree = file["Events"]
        weights = tree["Generator_weight"].array(library="np")
        sum_nominal_weight = np.sum(weights)
        num_selected = len(weights)
    return num_selected, sum_nominal_weight

def count_events_sim(files):
    """Count events and compute event weights in a set of files using multiprocessing and uproot.

    Arguments:
        files: Iterable with paths to NanoAOD files.

    Return value:
        Tuple consisting of the total number of processed events, the number of selected events,
        and the mean generator-level event weight.
    """
    with Pool(cpu_count()) as pool:
        results = pool.map(process_file, files)
        
    num_total = sum(result[0] for result in results)
    sum_nominal_weight = sum(result[1] for result in results)

    if num_total > 0:
        mean_weight = sum_nominal_weight / num_total
    else:
        mean_weight = 0

    print(num_total, num_total, mean_weight)
    return float(num_total), float(num_total), float(mean_weight)


def count_events(files):
    """Compute total number of events in a set of NanoAOD files.

    Arguments:
        files:  Iterable with paths to NanoAOD files.

    Return value:
        Number of events.
    """

    num_selected = 0
    for path in files:
        print(path)
        input_file = ROOT.TFile(path)

        event_tree = input_file.Get('Events')
        num_selected += event_tree.GetEntries()

        input_file.Close()
    return num_selected


def find_files(dataset_path):
    files = []

    with open(dataset_path, 'r') as datasets:
        for dataset in datasets:
            file_path = dataset.strip().split(',')[0]
            files.append(file_path)
        return files



if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument('config',
                            help='Configuration file with a list of datasets.')
    args = arg_parser.parse_args()

    #os.environ['LD_LIBRARY_PATH']=''
    #os.environ['PYTHONPATH']=''
    ddf_target_dir = 'ddf'
    subprocess.check_call(["mkdir", '-p', ddf_target_dir])
    if not os.path.exists(ddf_target_dir):
        os.mkdir(ddf_target_dir)
    files = find_files(args.config)
    dataset_name = os.path.abspath(args.config).split('/')[-1]
    dataset_name = dataset_name.split('.')[0]
    stem_name = dataset_name.split('SIM_')[-1]

    ddf = {
        'stem': stem_name,
        'files': files
    }

    if 'SIM' in dataset_name:
        num_total, num_selected, mean_weight = count_events_sim(files)
        weight_info = check_weights(files)
        ddf.update({
            'num_events': num_total,
            'num_selected_events': num_selected,
            'mean_weight': mean_weight,
            'weights': {
                'lhe_scale': weight_info.lhe_scale_present,
                'ps_scale': weight_info.ps_present
            }
        })
    else:
        ddf.update({
            'num_selected_events': count_events(files)
        })

    ddf_tmp_path = os.path.join(ddf_target_dir, stem_name + '.yaml')
    with open(ddf_tmp_path, 'w') as f:
        yaml.safe_dump(ddf, f, default_flow_style=False)
    print(4)


