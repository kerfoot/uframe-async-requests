#!/usr/bin/env python

import requests
import json
import re
import os
import sys
import argparse
import shutil
from netCDF4 import Dataset

def main(args):
    '''Download all NetCDF files located under the specified Hyrax url.  Individual
    NetCDF files correspond to the UFrame bin sizes.  The list of downloaded files
    is printed to STDOUT.'''
    
    nc_files = download_hyrax_nc_files(args.hyrax_url, args.destdir, args.verbose)
    
    if args.timestamp_files:
        nc_files = timestamp_nc_files(nc_files)
        
    if args.json:
        sys.stdout.write('{:s}'.format(json.dumps(nc_files)))
        return
    
    for nc_file in nc_files:
        sys.stdout.write('{:s}\n'.format(nc_file))
        
    return

def download_hyrax_nc_files(url, destdir, verbose):
    '''Download all NetCDF files located under the specified Hyrax url.  Individual
    NetCDF files correspond to the UFrame bin sizes.  File are downloaded to destdir
    under reference designator directories which are automatically created.'''
    
    # Retrieve the urls pointing to all NetCDF child directories located under
    # url
    parent_nc_dirs = parse_hyrax_parent_url(url)
    if not parent_nc_dirs:
        sys.stderr.write('No valid Hyrax parent directories found: {:s}\n'.format(url))
        sys.stderr.flush()
        return
    
    # Retrieve urls pointing to each NetCDF file in it's respective directory
    all_nc_file_urls = []    
    for parent_nc_dir in parent_nc_dirs:
        
        nc_file_urls = parse_hyrax_child_url(parent_nc_dir)
        
        for nc_file_url in nc_file_urls:
            all_nc_file_urls.append(nc_file_url)
            
    if not all_nc_file_urls:
        sys.stderr.write('No NetCDF urls found: {:s}\n'.format(url))
        return;
    
    # Download all NetCDF files to destdir
    nc_files = []    
    for nc_url in all_nc_file_urls:
        nc_file = download_hyrax_nc_from_url(nc_url, destdir, verbose=verbose)
        if not nc_file:
            continue
        
        nc_files.append(nc_file)
            
    return nc_files
        
def parse_hyrax_parent_url(url):
    
    r = requests.get(url)
    if r.status_code != 200:
        return []
        
    html = r.text
    r.close()
    
    # Regexp to find child directories containing .nc files
    hyrax_link_regexp = re.compile(r'<a href="(\w{10}\-\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}/contents.html)">')
    matches = hyrax_link_regexp.findall(html)
    
    if not matches:
        return matches
        
    url_tokens = os.path.split(url)
    hyrax_urls = []
    for m in matches:
        hyrax_urls.append(os.path.join(url_tokens[0], m))
        
    return hyrax_urls
    
def parse_hyrax_child_url(url):
    
    r = requests.get(url)
    if r.status_code != 200:
        return []
        
    html = r.text
    r.close()
    
    hyrax_nc_regexp = re.compile(r'<a href="(.*\.nc)">')
    matches = hyrax_nc_regexp.findall(html)

    if not matches:
        return matches
        
    url_tokens = os.path.split(url)
    nc_urls = []
    for m in matches:
        nc_urls.append(os.path.join(url_tokens[0], m))
        
    return nc_urls
    
def download_hyrax_nc_from_url(url, destdir, verbose=False):
    
    if not os.path.exists(destdir):
        sys.stderr.write('Invalid destination: {:s}\n'.format(destdir))
        return
        
    # Match uuid
    uuid_regex = re.compile(r'(\w{10}\-\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12})$')
    # match reference designator
    stream_regexp = re.compile(r'_(\w{1,}\-\w{1,}\-\w{1,}\-\w{1,}\-\w{1,}\-\w{1,})\.nc')

    url_tokens = os.path.split(url)
    if len(url_tokens) != 2:
        return
        
    match = uuid_regex.search(url_tokens[0])
    if not match:
        return
    uuid = match.groups()[0]
    
    match = stream_regexp.search(url_tokens[1])
    if not match:
        return
    stream = match.groups()[0]
    
    nc_dest = os.path.join(destdir, stream)
    if verbose:
        sys.stdout.write('NetCDF destination: {:s}\n'.format(nc_dest))
    if not os.path.exists(nc_dest):
        if verbose:
            sys.stdout.write('Creating destination: {:s}\n'.format(nc_dest))
        os.makedirs(nc_dest)
    
    local_nc = os.path.join(nc_dest, '-'.join([uuid, url_tokens[1]]))
    if verbose:
        sys.stderr.write('Downloading NetCDF file: {:s}\n'.format(url))
        
    try:
        with open(local_nc, 'wb') as fid:
            r = requests.get(url, stream=True)
            if r.status_code != 200:
                sys.stderr.write('Download failed: {:s}\n'.format(r.message))
                next
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    fid.write(chunk)
                    fid.flush()
    except IOError as e:
        sys.stderr.write('{:s}\n'.format(e.message))
        return
    
    return local_nc

def timestamp_nc_files(nc_files):
    
    ts_nc_files = []
    for nc_file in nc_files:
        
        file_tokens = os.path.split(nc_file)
        
        # match reference designator
        ref_des_regexp = re.compile(r'_(\w{1,}\-\w{1,}\-\w{1,}\-\w{1,}.*)\.nc')
        match = ref_des_regexp.search(file_tokens[1])
        if not match:
            sys.stderr.write('Failed to parse reference designator filename: {:s}\n'.format(nc_file))
            continue
            
        try:
            nci = Dataset(nc_file, 'r')
        except RuntimeError as e:
            sys.stderr.write('{:s}: {:s}\n'.format(e.message, nc_file))
            os.remove(nc_file)
            continue
        
        ts0 = re.sub('\-|:', '', nci.time_coverage_start[:19])
        ts1 = re.sub('\-|:', '', nci.time_coverage_end[:19])
        
        nc_filename = '{:s}-{:s}-{:s}.nc'.format(match.groups()[0], ts0, ts1)
        new_nc = os.path.join(file_tokens[0], nc_filename)
        
        try:
            shutil.move(nc_file, new_nc)
        except IOError as e:
            sys.stderr.write('{:s}: {:s}\n'.format(e.strerr, new_nc))
            continue
            
        ts_nc_files.append(new_nc)
        
    return ts_nc_files    
        
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('hyrax_url',
        help='URL pointing to the HYRAX dataset destination.')
    arg_parser.add_argument('-d', '--destdir',
        dest='destdir',
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
    