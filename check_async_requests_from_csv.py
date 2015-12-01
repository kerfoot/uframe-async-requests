#!/usr/bin/env python

import csv
import argparse
from uframe_async import *
    
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
        
        if r[0].startswith('#'):
            continue
            
        request_meta = {cols[i]:r[i] for i in col_range}
        
        if not request_meta['outputURL']:
            sys.stderr.write('No destination url specified: {:s}\n'.format(request_meta['request_url']))
            sys.stderr.flush()
            continue
            
        if 'completion_time' not in request_meta.keys():
            request_meta['completion_time'] = None
        
        if not request_meta['completion_time']:
            request_url = request_meta['outputURL']
            
            # Currently, we can only validate agains hyrax, not tds.
            if not args.tds:
                request_url = re.sub('8090/thredds/catalog/ooi/_nouser',
                    '8080/opendap/hyrax/async_results/_nouser',
                    request_meta['outputURL'])
                
            # Replace the url endpoint to point to the status.txt
            request_url = re.sub('catalog.html$', 'status.txt', request_url)
            completion_time = check_async_request_availability(request_url)
            request_meta['completion_time'] = completion_time   
        else:
            sys.stderr.write('Request already completed: {:s}\n'.format(request_meta['outputURL']))

        # Add the request to the output array
        responses.append(request_meta)
        
    fid.close()
        
    csv_writer = csv.writer(sys.stdout)
    csv_writer.writerow(cols)
    for x in responses:
        x_keys = x.keys()
        csv_writer.writerow([x[k] for k in cols if k in x_keys])
        
    return 0
        
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('request_csv',
            help='CSV filename containing queued UFrame request.')
    arg_parser.add_argument('--tds',
        help='Validate agains THREDDS, not hyrax (default)\n',
        dest='tds',
        action='store_true')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
