#!/usr/bin/env python

import csv
import argparse
import sys
import os
from uframe_async import *
from uframe_async.hyrax import *
    
def main(args):
    '''Check the availability of one or more asynchronous UFrame requests, contained in 
    a file.  Print the request responses to STDOUT'''
    
    responses = []
    
    fid = open(args.request_csv, 'r')
    csv_reader = csv.reader(fid)
    cols = csv_reader.next()
    col_range = range(0,len(cols))
    if 'completion_time' not in cols:
        cols.append('completion_time')
        
    for r in csv_reader:
        
        request_meta = {cols[i]:r[i] for i in col_range}
        
        if 'completion_time' not in request_meta.keys():
            request_meta['completion_time'] = None
        
        if not request_meta['completion_time']:
            completion_time = check_async_request_availability(request_meta)
            request_meta['completion_time'] = completion_time   
            hyrax_url = re.sub('8090/thredds/catalog/ooi/_nouser',
                '8080/opendap/hyrax/async_results/_nouser',
                request_meta['outputURL'])
            if completion_time:
                if args.debug:
                    sys.stdout.write('Request completed but skipping NetCDF downloads: {:s}\n'.format(hyrax_url))
                    sys.stdout.flush()
                    continue
                
                sys.stdout.write('Request completed, downloading NetCDF files: {:s}\n'.format(hyrax_url))    
                nc_files = download_hyrax_nc_files(hyrax_url, args.destdir, True)
                if not nc_files:
                    continue
                    
                nc_files = timestamp_nc_files(nc_files)
                for nc_file in nc_files:
                    sys.stdout.write('Downloaded: {:s}\n'.format(nc_file))
                    sys.stdout.flush()
        else:
            sys.stderr.write('Request already completed: {:s}\n'.format(request_meta['outputURL']))
            sys.stderr.flush()
            
        # Add the request to the output array
        responses.append(request_meta)
        
    fid.close()
        
    #csv_writer = csv.writer(sys.stdout)
    #csv_writer.writerow(cols)
    #for x in responses:
    #    x_keys = x.keys()
    #    csv_writer.writerow([x[k] for k in cols if k in x_keys])
        
    return 0
        
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('request_csv',
        help='CSV filename containing queued UFrame request.')
    arg_parser.add_argument('-d', '--destdir',
        dest='destdir',
        default=os.getcwd(),
        help='Destination directory for writing NetCDF files if they are available')
    arg_parser.add_argument('-v', '--verbose',
        dest='verbose',
        action='store_true',
        help='Print download progress to STDOUT.')
    arg_parser.add_argument('-x', '--debug',
        dest='debug',
        action='store_true',
        help='Print the outputUrls for completed requests, but do not download the NetCDF files.')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
