#!/usr/bin/env python
# Copyright (C) 2016 Swift Navigation Inc.
# Contact: Dennis Zollo<dzollo@swift-nav.com>
#
# This source is subject to the license found in the file 'LICENSE' which must
# be be distributed together with this source. All other rights reserved.
#
# THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.

"""

This module provides a commandline interface for downloading and removing log files
from Swift Navigation devices.  In order for the relative imports to work
it is recommended that the script is installed and run as a module.

Example:
    to download all logs in the current directory:

        $ python -m piksi_tools.log_manager  -p /dev/ttyUSB0 list
        Output format: ['0001-00000.sbp', '0001-00010.sbp']

    to download ['0001-00000.sbp', '0001-00010.sbp'] in the current directory:

        $ python -m piksi_tools.log_manager  -p /dev/ttyUSB0 download ['0001-00000.sbp', '0001-00010.sbp'] .

    to download all logs in the current directory:

        $ python -m piksi_tools.log_manager  -p /dev/ttyUSB0 download all .

    to remove all logs:

        $ python -m piksi_tools.log_manager  -p /dev/ttyUSB0 remove all

    to remove ['0001-00000.sbp', '0001-00010.sbp'] logs:

        $ python -m piksi_tools.log_manager  -p /dev/ttyUSB0 remove ['0001-00000.sbp', '0001-00010.sbp']

"""

from __future__ import absolute_import, print_function

from sbp.client import Framer, Handler
from piksi_tools import serial_link
from piksi_tools.settings import Settings
from piksi_tools.fileio import FileIO

import ast


def download(fileio_link, log_path, log_list, dest_path):
    log_list = ast.literal_eval(log_list)
    logs = [n.strip() for n in log_list]
    sbp_files = []
    for f in logs:
        if f.split('.')[-1] == 'sbp':
            sbp_files.append(f)
    print("Download %s in %s" % (sbp_files, log_path))
    index = 1
    total = len(sbp_files)
    for f in sbp_files:
        with open(dest_path+"/"+f, "wb+") as log:
            print("Downloading %s... (%d/%d)" % (f, index, total))
            log.write(fileio_link.read(log_path+f))
            index += 1
    print("Done")
    return


def list(fileio_link, log_path):
    print("SBP files in %s:" % log_path)
    files = fileio_link.readdir(log_path)
    sbp_files = []
    for f in files:
        if f.split('.')[-1] == 'sbp':
            sbp_files.append(f)
    print(sbp_files)
    return str(sbp_files)


def remove(fileio_link, log_path, log_list):
    log_list = ast.literal_eval(log_list)
    logs = [n.strip() for n in log_list]
    sbp_files = []
    for f in logs:
        if f.split('.')[-1] == 'sbp':
            sbp_files.append(f)
    print("Remove %s in %s" % (sbp_files, log_path))
    index = 1
    total = len(sbp_files)
    for f in sbp_files:
        print("Removing %s... (%d/%d)" % (f, index, total))
        fileio_link.remove(log_path+"/"+f)
        index += 1
    print("Done")
    return


def get_args():
    """
    Get and parse arguments.
    """
    import argparse
    parser = serial_link.base_cl_options()
    parser.description = 'Log Manager Tool'
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.epilog = ("Returns:\n"
                     "  0: Upon success\n"
                     "  1: Runtime error or invalid settings request.\n"
                     "  2: Improper usage")

    subparsers = parser.add_subparsers(dest="command")

    remove = subparsers.add_parser('remove', help='remove all the logs.')
    remove.add_argument("list", help="List of files to remove")

    list = subparsers.add_parser('list', help='list all the logs.')

    download = subparsers.add_parser('download', help='download the logs from device.')
    download.add_argument("list", help="List of files to download")
    download.add_argument("dest", help="Destination path.")

    return parser.parse_args()


def main():
    """
    Get configuration, get driver, and build handler and start it.
    """
    args = get_args()
    command = args.command
    return_code = 0
    driver = serial_link.get_base_args_driver(args)
    with Handler(Framer(driver.read, driver.write)) as link:
        settings_link = Settings(link)
        with settings_link:
            log_path = settings_link.read("standalone_logging", "output_directory")
        fileio_link = FileIO(link)
        if command == 'download':
            if args.list == 'all':
                download(fileio_link, log_path, list(fileio_link, log_path), args.dest)
            else:
                download(fileio_link, log_path, args.list, args.dest)
        elif command == 'remove':
            if args.list == 'all':
                remove(fileio_link, log_path, list(fileio_link, log_path))
            else:
                remove(fileio_link, log_path, args.list)
        elif command == 'list':
            list(fileio_link, log_path)
    return return_code


if __name__ == "__main__":
    main()
