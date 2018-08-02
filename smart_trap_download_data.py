################
# Python 3.5.2 #
################

import json
import requests
import pprint
import argparse
import os
import collections
from datetime import datetime

def main():
    args = parse_args()
    
    api_key = args.api_key
    start_time = args.start_time
    end_time = args.end_time

    data = {
        'data[user_id]' : api_key,
        'data[startTime]' : start_time.isoformat(' '),
        'data[endTime]' : end_time.isoformat(' ')
    }

    response = requests.post('http://live.bg-counter.com/traps/exportTrapCapturesForTimeFrame.json', data)
    js = json.loads(response.text, object_pairs_hook=collections.OrderedDict)

    #pprint.pprint(js)

    for trap_wrapper in js['traps']:
        trap_id = trap_wrapper['Trap']['id']
        captures = trap_wrapper['Capture']
        print('Trap: ' + trap_id)
        print('Starting captures: ' + str(len(captures)))

        if len(captures) > 0:
            print('First capture: ' + captures[0]['timestamp_start'])
            print('Last capture: ' + captures[-1]['timestamp_end'])

            if len(captures) == 1000:
                last_timestamp_end = captures[-1]['timestamp_end']
                rest_of_captures = get_captures(api_key, trap_id, last_timestamp_end, end_time.isoformat(' '))
                captures.extend(rest_of_captures)

            final_start_time = captures[0]['timestamp_start'].replace(' ', 'T')
            final_end_time = captures[-1]['timestamp_end'].replace(' ', 'T')
            filename = '{}_{}_{}.json'.format(trap_id, final_start_time, final_end_time)
    
            if os.path.isdir('./trap-json'):
                key_dir = './trap-json/' + api_key

                if not os.path.exists(key_dir):
                    os.mkdir(key_dir)

                path = './trap-json/{}/{}'.format(api_key, filename)

            else:
                path = './' + filename
    
            with open(path, 'w') as f:
                trap_obj = {'traps' : [trap_wrapper]}
                json.dump(trap_obj, f, indent=args.pretty_print)

        print('Final captures: ' + str(len(captures)) + '\n')

def get_captures(api_key, trap_id, start_timestamp, end_timestamp):
    print('Entering get_captures()')
    data = {
        'data[user_id]' : api_key,
        'data[startTime]' : start_timestamp,
        'data[endTime]' : end_timestamp
    }

    response = requests.post('http://live.bg-counter.com/traps/exportTrapCapturesForTimeFrame.json', data)
    js = json.loads(response.text, object_pairs_hook=collections.OrderedDict)

    for trap_wrapper in js['traps']:
        if trap_wrapper['Trap']['id'] == trap_id:
            captures = trap_wrapper['Capture']
            print('New captures: ' + str(len(captures)))

            if len(captures) > 0:
                print('First: ' + captures[0]['timestamp_start'])
                print('Last: ' + captures[-1]['timestamp_end'])

            if len(captures) == 1000:
                last_timestamp_end = captures[-1]['timestamp_end']
                rest_of_captures = get_captures(api_key, trap_id, last_timestamp_end, end_timestamp)
                captures.extend(rest_of_captures)

            print('Leaving get_captures()')
            return captures

def parse_args():
    parser = argparse.ArgumentParser(description='Pulls smart trap data.')
    parser.add_argument('-k', '--api-key', required=True)
    parser.add_argument('-s', '--start-time', type=parse_date, required=True)
    parser.add_argument('-e', '--end-time', type=parse_date, required=True)
    parser.add_argument('-p', '--pretty-print', action='store_const', const=4, default=None)

    return parser.parse_args()

def parse_date(string):
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(string, fmt)
        except ValueError:
            pass

    raise argparse.ArgumentTypeError(
        'Acceptable time formats: "YYYY-MM-DD", "YYYY-MM-DD HH:MM", "YYYY-MM-DD HH:MM:SS"')

main()

