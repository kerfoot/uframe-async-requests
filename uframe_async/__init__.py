#!/usr/bin/env python

import requests
import json
import sys
import re
import datetime
from dateutil import parser

_PARAMETER_REGEXPS = {'beginDT' : r'^\d{4}\-\d{2}\-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,}Z$',
    'endDT' : r'^\d{4}\-\d{2}\-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,}Z$',
    'format' : r'^application/(\w{1,})$',
    'limit' : r'^(\-\d{1,}|\d{1,})$',
    'execDPA' : r'^true|false$',
    'include_provenance' : r'^true|false$'}
_REQUIRED_PARAMETERS = ['beginDT',
    'endDT',
    'format',
    'limit']
    
#def main(args):
#    '''Validate and send one or more asynchronous UFrame requests, contained in 
#    a file.  Print the request responses to STDOUT'''
#    
#    responses = []
#    
#    fid = open(args.request_csv, 'r')
#    for url in fid:
#        #valid = validate_async_request(url.strip())
#        #if not valid:
#        #    continue
#            
#        status = send_async_request(url.strip())
#        if status:
#            responses.append(status)
#    fid.close()
#            
#    if not status:
#        return 1
#        
#    csv_writer = csv.writer(sys.stdout)
#    cols = ['instrument',
#        'beginDT',
#        'endDT',
#        'status_code',
#        'reason',
#        'stream_beginDT',
#        'stream_endDT',
#        'valid',
#        'valid_time_interval',
#        'request_time',
#        'requestUUID',
#        'outputURL',
#        'request_url']
#    csv_writer.writerow(cols)
#    for x in responses:
#        x_keys = x.keys()
#        csv_writer.writerow([x[k] for k in cols if k in x_keys])
#        
#    return 0
    
def send_async_request(url, time_check=True):
    '''Send an asynchronous UFrame data request and return the response and the
    request metadata from the url.
    '''
    
    status = {}
    
    # Make sure the url is formatted properly and check that the beginDT and 
    # endDT fall within the stream bounds contained in the metadata
    valid = validate_async_request(url, time_check=time_check)
    if not valid['valid']:
        sys.stderr.write('Invalid asynchronous data request: {:s} (Reason: {:s})\n'.format(url, valid.reason))
        return status
    
    # Parse url for status fields
    match = re.compile(r'beginDT=(\d{4}\-\d{2}\-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,}Z)').search(url)
    if not match:
        sys.stderr.write('Invalid beginDT specified: {:s}\n'.format(url))
        return status
    beginDT = match.groups()[0]
    
    match = re.compile(r'endDT=(\d{4}\-\d{2}\-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,}Z)').search(url)
    if not match:
        sys.stderr.write('Invalid endDT specified: {:s}\n'.format(url))
        return status
    endDT = match.groups()[0]
    
    # Instrument
    INSTRUMENT_REGEXP = re.compile('/(\w{8,}/\w{5,}/\d{2}\-\w{9,})/')
    match = INSTRUMENT_REGEXP.search(url)
    if not match:
        sys.stderr.write('Invalid instrument specified: {:s}\n'.format(url))
        return status
    instrument = '-'.join(match.groups()[0].split('/'))
    
    # Fill in the return value
    status['request_url'] = url
    status['instrument'] = instrument
    status['beginDT'] = beginDT
    status['endDT'] = endDT
    status['status_code'] = None
    status['reason'] = None
    status['requestUUID'] = None
    status['outputURL'] = None
    status['request_time'] = None
    
    # Add validation parameters
    for (k,v) in valid.items():
        status[k] = valid[k]
    
    # Time of request
    rt = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%sZ')
    
    # Send request
    r = requests.get(url)
    
    # Store the server status code of the request
    status['status_code'] = r.status_code
    
    if r.status_code == 400:
        sys.stderr.write('Request Failed (Reason={:s}): {:s}'.format(r.json()['message'], url))
        status['reason'] = r.json()['message']
        return status
    elif r.status_code != 200:
        sys.stderr.write('Request Failed (Reason={:s}): {:s}'.format(r.reason, url))
        status['reason'] = r.reason
        return status
        
    response = r.json()
    
    # Combine the request response and parsed url metadata
    status['requestUUID'] = response['requestUUID']
    status['outputURL'] = response['outputURL']
    status['request_time'] = rt

    return status
    
def validate_async_request(url, time_check=True):
    '''Validates url to ensure that the request is formatted properly.  Set
    time_check=True to check that the beginDT and endDT are within the specified
    stream time bounds.'''
    
    valid = {'valid' : False,
        'valid_time_interval' : None,
        'stream_beginDT' : None,
        'stream_endDT' : None,
        'reason' : ''}
        
    # Split the url and the parameters
    url_tokens = url.split('?')
    if len(url_tokens) != 2:
        sys.stderr.write('Request contains no parameters: {:s}\n'.format(url))
        return valid
        
    parameters = url_tokens[1].split('&')
    valid_parameters = _PARAMETER_REGEXPS.keys()
    # Make sure all specified parameters are in valid_parameters and that the
    # url has the _REQUIRED_PARAMETERS
    required_count = 0
    for parameter in parameters:
        try:
            (p,v) = parameter.split('=')
        except ValueError as e:
            sys.stderr.write('Malformed parameter: {:s}\n'.format(parameter))
            return valid
        
        if p not in valid_parameters:
            sys.stderr.write('Invalid parameter: {:s}\n'.format(p))
            return valid    
        
        if p in _REQUIRED_PARAMETERS:
            required_count += 1
            
        match = re.compile(_PARAMETER_REGEXPS[p]).search(v)
        if not match:
            sys.stderr.write('Invalid parameter/value: {:s}\n'.format(parameter))
            return valid
            
        # Special cases for checking values
        # limit
        if p == 'limit':
            try:
                val = int(v)
                if val >= 0:
                    sys.stderr.write('URL is requesting a synchronouse request: {:s}={:s}\n'.format(p,v))
                    return valid
                    
            except ValueError as e:
                sys.stderr.write('Invalid {:s} value: {:s}\n'.format(p, v))
                return valid
        
        # Convert beginDT to datetime
        if p == 'beginDT':
            beginDT = v
            dt0 = parser.parse(beginDT)
        
        # Convert endDT to datetime
        if p == 'endDT':
            endDT = v
            dt1 = parser.parse(endDT)
    
    # Make sure beginDT occurs before endDT    
    if dt0 > dt1:
        sys.stderr.write('Invalid time bounds (beginDT={:s} > endDT={:s}): {:s}\n'.format(beginDT, endDT, url))
        return valid
    
    # Request is valid if we've made it here
    valid['valid'] = True        
    
    # Fetch metadata to get stream beginTime and endTime    
    METADATA_REGEXP = re.compile(r'^(http://.*/sensor/inv/\w{8}/\w{5}/\d{2}\-\w{9}/)(\w+)/(\w+)')
    match = METADATA_REGEXP.search(url) 
    if not match:
        sys.stderr.write('Time check ERROR: invalid url {:s}\n'.format(url))
        return valid
        
    metadata_url = '{:s}metadata/times'.format(match.groups()[0])
    telemetry = match.groups()[1]
    stream = match.groups()[2]
    # Send the metadata request
    r = requests.get(metadata_url)
    if r.status_code != 200:
        valid['reason'] = 'Failed to fetch metadata: {:s}\n'.format(metadata_url)
        sys.stderr.flush()
        return valid
        
    metadata = r.json()
    
    # Find the stream
    streams = [m['stream'] for m in metadata]
    if stream not in streams:
        sys.stderr.write('Invalid stream: {:s}\n'.format(stream))
        return valid
        
    s_index = streams.index(stream)
    stream_beginDT = parser.parse(metadata[s_index]['beginTime'])
    stream_endDT = parser.parse(metadata[s_index]['endTime'])
    
    # Add stream begin/endDt to return value
    valid['stream_beginDT'] = metadata[s_index]['beginTime']
    valid['stream_endDT'] = metadata[s_index]['endTime']
    
    # Check beginDT and endDT agains the metadata request if time_check = True    
    if time_check:
        
        # dt0 must be >= stream_beginDT and dt1 must be <= stream_endDT
        if dt0 < stream_beginDT:
            sys.stderr.write('Time check ERROR: Specified request beginDT is earlier than metadata beginDT ({:s} < {:s})'.format(beginDT, metadata[s_index]['beginTime']))
            valid['valid_time_interval'] = False
            return valid
            
        if dt1 > stream_endDT:
            sys.stderr.write('Time check ERROR: Specified request endDT is later than metadata endDT ({:s} > {:s})'.format(endDT, metadata[s_index]['endTime']))
            valid['valid_time_interval'] = False
            return valid
        
        # Set request to valid
        valid['valid_time_interval'] = True
        
    return valid
    
def check_async_request_availability(async_request):
    
    time_available = None
    
    opendap_url_key = 'outputURL'
    if opendap_url_key not in async_request.keys():
        sys.stderr.write('async_request is missing the outputURL\n')
        return time_available
        
    #sys.stdout.write('outputURL: {:s}\n'.format(async_request[opendap_url_key]))
    #return time_available
    
    # Attempt to fetch the opendap url top-level directory page
    r = requests.get(async_request[opendap_url_key])
    if r.status_code != 200:
        sys.stderr.write('Invalid request: {:s} ({:s})\n'.format(async_request['outputURL'], r.reason))
        return time_available
        
    time_available = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%sZ')
    
    return time_available
