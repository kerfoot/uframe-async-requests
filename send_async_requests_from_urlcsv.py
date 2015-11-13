#!/usr/bin/env python

import csv
import argparse
from uframe_async import *
    
def main(args):
    '''Validate and send one or more asynchronous UFrame requests, contained in 
    a file.  Print the request responses to STDOUT'''
    
    csv_writer = csv.writer(sys.stdout)
    cols = ['instrument',
        'beginDT',
        'endDT',
        'status_code',
        'reason',
        'stream_beginDT',
        'stream_endDT',
        'valid',
        'valid_time_interval',
        'request_time',
        'requestUUID',
        'outputURL',
        'request_url']
    csv_writer.writerow(cols)
        
    fid = open(args.request_csv, 'r')
    success = True
    for url in fid:
        
        if url.startswith('#'):
            continue
        
        #valid = validate_async_request(url.strip())
        #if not valid:
        #    continue
          
        #sys.stderr.write('Sending request: {:s}\n'.format(url.strip()))  
        status = send_async_request(url.strip())
        if not status:
            success = False
            continue
            
        status_keys = status.keys()
        csv_writer.writerow([status[k] for k in cols if k in status_keys])
            #responses.append(status)

    fid.close()
            
    #if not status:
    #    return 1
        
    #csv_writer = csv.writer(sys.stdout)
    #cols = ['instrument',
    #    'beginDT',
    #    'endDT',
    #    'status_code',
    #    'reason',
    #    'stream_beginDT',
    #    'stream_endDT',
    #    'valid',
    #    'valid_time_interval',
    #    'request_time',
    #    'requestUUID',
    #    'outputURL',
    #    'request_url']
    #csv_writer.writerow(cols)
    #for x in responses:
    #    x_keys = x.keys()
    #    csv_writer.writerow([x[k] for k in cols if k in x_keys])
        
    return success
        
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('request_csv',
            help='Filename containing valid asynchronous UFrame request urls')

    parsed_args = arg_parser.parse_args()

    success = main(parsed_args)
