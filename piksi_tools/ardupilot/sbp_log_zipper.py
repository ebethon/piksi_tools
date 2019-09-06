#!/usr/bin/env python
# Copyright (C) 2015 Swift Navigation Inc.
# Contact: Fergus Noble <fergus@swiftnav.com>
#
# This source is subject to the license found in the file 'LICENSE' which must
# be be distributed together with this source. All other rights reserved.
#
# THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR
"""
Combines a base and rover JSON SBP log file into a single JSON SBP log,
interleaving messages to produce a stream that increases monotonically in GPS time.
Further, sets the sender id of base log messages to zero.

This script only passes through a subset of all SBP messages. Only messages
necessary during post-processing observations to produce navigation solutions
are preserved. Specifically, observations, ephemeris, ionosphere, and base positions.

This script supports a post-processing use case for Piksi.
You can separately record a log file on a rover and on a base Piksi.
Use this script to create a single log file, which can be passed to
libswiftnav-private's run_filter command to produce a RTK baseline stream.

Requirements:

  pip install json
  sudo pip install sbp

"""
from __future__ import print_function

import argparse
import re

import sbp.client.loggers.json_logger as json_logger
import sbp.observation as ob

filename_regex = r"(?<=[\\\/])[^\\\/]*(?=$)"
dirname_regex = r"(?<=^).*[\\\/](?=[^\\\/]*$)"
filepath_split_ext_regex = r"(?<=^)(.*?)(\.[^\\\/]*)(?=$)"

msgs_filter = [
    ob.SBP_MSG_OBS,
    ob.SBP_MSG_OSR,
    ob.SBP_MSG_EPHEMERIS_GPS,
    ob.SBP_MSG_EPHEMERIS_GPS_DEP_E,
    ob.SBP_MSG_EPHEMERIS_GPS_DEP_F,
    ob.SBP_MSG_EPHEMERIS_BDS,
    ob.SBP_MSG_EPHEMERIS_GAL,
    ob.SBP_MSG_EPHEMERIS_SBAS_DEP_A,
    ob.SBP_MSG_EPHEMERIS_GLO_DEP_A,
    ob.SBP_MSG_EPHEMERIS_SBAS_DEP_B,
    ob.SBP_MSG_EPHEMERIS_SBAS,
    ob.SBP_MSG_EPHEMERIS_GLO_DEP_B,
    ob.SBP_MSG_EPHEMERIS_GLO_DEP_C,
    ob.SBP_MSG_EPHEMERIS_GLO_DEP_D,
    ob.SBP_MSG_EPHEMERIS_GLO,
    ob.SBP_MSG_EPHEMERIS_DEP_D,
    ob.SBP_MSG_EPHEMERIS_DEP_A,
    ob.SBP_MSG_EPHEMERIS_DEP_B,
    ob.SBP_MSG_EPHEMERIS_DEP_C,
    ob.SBP_MSG_BASE_POS_LLH,
    ob.SBP_MSG_BASE_POS_ECEF,
    ob.SBP_MSG_IONO,
    ob.SBP_MSG_GLO_BIASES
]

msgs_filter_eph = [
    ob.SBP_MSG_EPHEMERIS_GPS,
    ob.SBP_MSG_EPHEMERIS_GPS_DEP_E,
    ob.SBP_MSG_EPHEMERIS_GPS_DEP_F,
    ob.SBP_MSG_EPHEMERIS_BDS,
    ob.SBP_MSG_EPHEMERIS_GAL,
    ob.SBP_MSG_EPHEMERIS_SBAS,
    ob.SBP_MSG_EPHEMERIS_SBAS_DEP_A,
    ob.SBP_MSG_EPHEMERIS_SBAS_DEP_B,
    ob.SBP_MSG_EPHEMERIS_QZSS,
    ob.SBP_MSG_EPHEMERIS_GLO,
    ob.SBP_MSG_EPHEMERIS_GLO_DEP_A,
    ob.SBP_MSG_EPHEMERIS_GLO_DEP_B,
    ob.SBP_MSG_EPHEMERIS_GLO_DEP_C,
    ob.SBP_MSG_EPHEMERIS_GLO_DEP_D,
    ob.SBP_MSG_EPHEMERIS_DEP_A,
    ob.SBP_MSG_EPHEMERIS_DEP_B,
    ob.SBP_MSG_EPHEMERIS_DEP_C,
    ob.SBP_MSG_EPHEMERIS_DEP_D,
]

def extract_gpstime(msg, last_gpstime=(0, 0)):
    '''
    Returns (wn,tow) tuple. returns last_gpstime if none in this message
    '''
    if msg.msg_type == ob.SBP_MSG_OBS or msg.msg_type == ob.SBP_MSG_OSR:
        return (msg.header.t.wn, msg.header.t.tow)
    elif msg.msg_type in msgs_filter_eph:
        return (msg.common.toe.wn, msg.common.toe.tow)
    elif msg.msg_type == ob.SBP_MSG_IONO:
        return (msg.t_nmct.wn, msg.t_nmct.tow)
    elif msg.msg_type == ob.SBP_MSG_BASE_POS_ECEF or msg.msg_type == ob.SBP_MSG_BASE_POS_LLH or ob.SBP_MSG_GLO_BIASES:
        return last_gpstime


def compare_gpstime(g0, g1):
    '''
    Returns the index of the earlier GPSTIME (wn,tow) tow.
    '''
    if g0[0] < g1[0]:
        return 0
    elif g0[0] > g1[0]:
        return 1
    else:
        if g0[1] < g1[1]:
            return 0
        elif g0[1] > g1[1]:
            return 1
        else:
            return 0

def compare_gpstime_brate(g0, g1, brate):
    '''
    Returns 1 if g0=g1 or if g0>=g1+brate, 0 otherwise
    '''
    if g0[0] < g1[0]:
        return 0
    if g0[0] > g1[0]:
        return 1
    else:
        if g0[1] == g1[1]:
            return 1
        elif g0[1] >= g1[1]+brate:
            return 1
        else:
            return 0


def print_emit(msg):
    print(msg.to_json())

def zip_json_generators(base_gen, rove_gen, emit_fn, brate):
    '''
    Zips together two generators.
    Runs in constant space.
    Sends messages to the emit_fn

    Here's the algorithm:
      We assume we might have a message from every logfile
      For the logfiles we don't have a message, we retrieve one.
      We get timestamps for all our messages.
      We consume and discard the one with the oldest timestamp, keeping the other around
      Repeat!
    '''
    base_msg = None
    rove_msg = None

    last_gpstime = (0, 0)
    last_base_gpstime = (0, 0)
    while True:

        # Get a base_msg if we don't have one waiting
        while base_msg is None:
            try:
                base_msg = next(base_gen)[0]
                base_gpstime = extract_gpstime(base_msg, last_gpstime)
                if base_msg.msg_type in msgs_filter and compare_gpstime_brate(base_gpstime,last_base_gpstime, brate):
                    last_base_gpstime = base_gpstime
                    # Fix up base id
                    base_msg.sender = 0
                    break
                else:
                    base_msg = None
            except StopIteration:
                base_done = True
                break

        # Get a rove_msg if we don't have one waiting
        while rove_msg is None:
            try:
                rove_msg = next(rove_gen)[0]
                if rove_msg.sender and rove_msg.msg_type in msgs_filter:
                    break
                else:
                    rove_msg = None
            except StopIteration:
                rove_done = True
                break

        if base_msg is None and rove_msg is None:
            return  # We are done.

        if base_msg is None:
            emit_fn(rove_msg)
            rove_msg = None
            continue  # Loop!

        if rove_msg is None:
            emit_fn(base_msg)
            base_msg = None
            continue  # Loop!

        # We have a message from both. Which one do we emit?
        base_time = extract_gpstime(base_msg, last_gpstime)
        rove_time = extract_gpstime(rove_msg, last_gpstime)

        which = compare_gpstime(rove_time, base_time)
        if which == 1:
            emit_fn(base_msg)
            base_msg = None
            last_gpstime = base_time
        else:
            emit_fn(rove_msg)
            rove_msg = None
            last_gpstime = rove_time


def zip_json_files(base_log_handle, rove_log_handle, emit_fn, brate):
    with json_logger.JSONLogIterator(base_log_handle) as base_logger:
        with json_logger.JSONLogIterator(rove_log_handle) as rove_logger:

            base_gen = next(base_logger)
            rove_gen = next(rove_logger)

            zip_json_generators(base_gen, rove_gen, emit_fn, brate)


def main():
    parser = argparse.ArgumentParser(
        description="Swift Navigation SBP Rover-Base Log Zipper")
    parser.add_argument("rover_log", help="rover log")
    parser.add_argument(
        '-b',
        '--base',
        default=[''],
        nargs=1,
        help='base log')
    parser.add_argument(
        '-br',
        '--brate',
        default=[20],
        nargs=1,
        help='base log observation rate')
    parser.add_argument(
        '-o',
        '--output',
        default=[0],
        nargs=1,
        help='output filename (same directory as rover)')
    args = parser.parse_args()

    split = re.search(filepath_split_ext_regex, args.rover_log)
    if args.base[0] == '':
        base_log = split.groups()[0]+"_base"+split.groups()[1]
        rover_log = split.groups()[0]+"_rover"+split.groups()[1]

        with open(base_log, 'w+') as base_log_file:
            with open(rover_log, 'w+') as rover_log_file:
                with open(args.rover_log, 'r') as log_file:
                    with json_logger.JSONLogIterator(log_file) as rover_logger:
                        msg_gen = next(rover_logger)
                        while True:
                            try:
                                msg = next(msg_gen)[0]
                                if msg.sender == 0:
                                    base_log_file.write(msg.to_json() + '\n')
                                else:
                                    rover_log_file.write(msg.to_json() + '\n')
                            except StopIteration:
                                break

    else:
        base_log = args.base_log
        rover_log = args.rover_log

    with open(base_log, 'r') as base_log_handle:
        with open(rover_log, 'r') as rove_log_handle:
            if args.output[0]:
                zip_filename = split.groups()[0] + "_zip" + split.groups()[1]
                with open(zip_filename, 'w+') as zip_file:
                    def file_emit(msg):
                        zip_file.write(msg.to_json()+'\n')
                    zip_json_files(base_log_handle, rove_log_handle, file_emit, 1000 / float(args.brate[0]))
            else:
                zip_json_files(base_log_handle, rove_log_handle, print_emit, 1000/float(args.brate[0]))


if __name__ == "__main__":
    main()