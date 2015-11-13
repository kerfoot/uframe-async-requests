#!/usr/bin/env python

import requests
import json
import re
import os
import sys
import argparse
import shutil
from netCDF4 import Dataset
from uframe_async.hyrax import *

def main(args):
    '''Download all NetCDF files located under the specified Hyrax url.  Individual
    NetCDF files correspond to the UFrame bin sizes.  The list of downloaded files
    is printed to STDOUT.'''
    
    hyrax_url = re.sub('8090/thredds/catalog/ooi/_nouser', '8080/opendap/hyrax/async_results/_nouser', args.hyrax_url)
    
    nc_files = download_hyrax_nc_files(hyrax_url, args.destdir, args.verbose)
    if not nc_files:
        sys.stderr.write('No Hyrax NetCDF files found at: {:s}\n'.format(args.hyrax_url))
        return
    
    if args.timestamp_files:
        nc_files = timestamp_nc_files(nc_files)
        
    if args.json:
        sys.stdout.write('{:s}'.format(json.dumps(nc_files)))
        return
    
    for nc_file in nc_files:
        sys.stdout.write('{:s}\n'.format(nc_file))
        
    return
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('hyrax_url',
        help='URL pointing to the HYRAX dataset destination.')
    arg_parser.add_argument('-d', '--destdir',
        dest='destdir',
        default=os.getcwd(),
        help='Destination directory for writing NetCDF files')
    arg_parser.add_argument('-j', '--json',
        dest='json',
        action='store_true',
        help='Dump the downloaded NetCDF files names as a json array.')
    arg_parser.add_argument('-v', '--verbose',
        dest='verbose',
        action='store_true',
        help='Print download progress to STDOUT.')
    arg_parser.add_argument('-t', '--timestamp',
        dest='timestamp_files',
        action='store_true',
        help='Rename each downloaded file to include the start and end timestamps.')
        
    parsed_args = arg_parser.parse_args()
    
    main(parsed_args)