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
        
        request_meta = {cols[i]:r[i] for i in col_range}
        
        if 'completion_time' not in request_meta.keys():
            request_meta['completion_time'] = None
        
        if not request_meta['completion_time']:
            completion_time = check_async_request_availability(request_meta)
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

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
