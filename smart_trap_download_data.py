################
# Python 3.5.2 #
################

import json
import requests
import pprint
import argparse
import os
import collections
import curses
import math
from datetime import datetime, timedelta
from time import sleep

def main(stdscr, args):
    api_key = args.api_key
    start_time = args.start_time
    end_time = args.end_time
    display = args.display

    LIMIT = 1000  # The limit of data points (captures) per trap the API can deliver

    if display:
        # Set up stuff related to curses
        curses.curs_set(False)
        graph_width = stdscr.getmaxyx()[1] - 35
        duration = end_time - start_time
        gradation = duration / graph_width
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_GREEN)

    # Perform a request on the full duration first
    js = request_data(stdscr, api_key, start_time, end_time)

    incomplete_traps = {}  # Keys will be traps with incomplete data and values will be their most recent timestamps
    trap_data = {}  # Will contain the JSON objects for each individual trap

    if display:
        trap_ys = {}  # Will contain the y-coordinate of each trap's line in the display

        # Draw vertical lines
        stdscr.vline(3, 20, '|', 2 * len(js['traps']) + 3)
        stdscr.vline(3, 21 + graph_width, '|', 2 * len(js['traps']) + 3)
        stdscr.addch(2, 20, '+')
        stdscr.addch(2 * len(js['traps']) + 6, 20, '+')
        stdscr.move(3, 2)

    # Get info about each trap
    for trap_wrapper in js['traps']:
        trap_id = trap_wrapper['Trap']['id']
        captures = trap_wrapper['Capture']
        num_captures = len(captures)
        trap_data[trap_id] = trap_wrapper
        #print('trap_id: {} - Starting captures: {}'.format(trap_id, len(captures)))

        if display:
            # Draw horizontal lines
            y, x = stdscr.getyx()
            y += 2
            stdscr.move(y, 2)
            trap_ys[trap_id] = y
            stdscr.addstr(trap_id)
            stdscr.hline(y, 21, '-', graph_width)

        # If we get LIMIT capture for this trap, most likely we hit the max
        # and the trap has more data that wasn't delivered
        if num_captures == LIMIT:
            # Store the most recent timestamp in incomplete_traps
            ending_timestamp = captures[-1]['timestamp_end']
            ending_datetime = datetime.strptime(ending_timestamp, '%Y-%m-%d %H:%M:%S')
            incomplete_traps[trap_id] = ending_datetime

            if display:
                # Draw a white line to show the data we currently have
                # and print the number of captures (LIMIT) to the right of the line
                position = date_to_position(ending_datetime, start_time, gradation)
                stdscr.hline(trap_ys[trap_id], 21, ' ', position, curses.color_pair(1))
                stdscr.addstr(y, 23 + graph_width, str(num_captures))

        # If the API is ever improved so that it can deliver more than LIMIT captures,
        # we'll have to modify this script
        elif num_captures > LIMIT:
            raise ValueError('More than {} (LIMIT) captures for a trap: {}.'.format(LIMIT, num_captures))

        # Else, assume that we got all captures
        elif display:
            stdscr.addstr(y, 23 + graph_width, 'Done')
            stdscr.hline(y, 21, ' ', graph_width, curses.color_pair(2))

    if display:
        stdscr.refresh()

    # While there are still traps with more data
    while incomplete_traps:
        # Find which ending timestamp is the earliest
        ending_dates = list(incomplete_traps.values())
        earliest_date = ending_dates[0]

        #stdscr.move(50, 5)
        for date in ending_dates:
            #y, x = stdscr.getyx()
            #stdscr.move(y + 1, 5)
            #stdscr.addstr(date.isoformat())
            if date < earliest_date:
                earliest_date = date

        #stdscr.getch()
        if display:
            # Move the tracking line to the earliest time
            position = date_to_position(earliest_date, start_time, gradation)
            #stdscr.addstr(55, 5, str(position))
            erase_tracking_line(stdscr, trap_ys, graph_width)
            draw_tracking_line(stdscr, position, trap_ys)

        # Perform a request from the earliest time to the original end time
        #stdscr.getch()
        new_js = request_data(stdscr, api_key, earliest_date, end_time)

        # Turn all previous data green
        if display:
            for trap_id, ending_date in incomplete_traps.items():
                position = date_to_position(ending_date, start_time, gradation)
                stdscr.hline(trap_ys[trap_id], 21, ' ', position, curses.color_pair(2))
                #stdscr.addstr(ending_date.isoformat() + ' ' + str(position))
    
            stdscr.refresh()

        for trap_wrapper in new_js['traps']:
            trap_id = trap_wrapper['Trap']['id']

            if trap_id in incomplete_traps:
                captures = trap_wrapper['Capture']
                num_captures = len(captures)
                num_new_captures = 0

                if display:
                    # Get the last timestamp and draw a white bar for all new data
                    y = trap_ys[trap_id]
                    last_ending_position = date_to_position(incomplete_traps[trap_id], start_time, gradation)
                    ending_timestamp = captures[-1]['timestamp_end']
                    ending_datetime = datetime.strptime(ending_timestamp, '%Y-%m-%d %H:%M:%S')
                    position = date_to_position(ending_datetime, start_time, gradation)
                    stdscr.hline(y, last_ending_position + 21, ' ', position - last_ending_position, curses.color_pair(1))
                    stdscr.refresh()

                # Check if there are more than LIMIT captures for the same reason as before
                if num_captures > LIMIT:
                    raise ValueError('More than {} (LIMIT) captures for a trap: {}.'.format(LIMIT, num_captures))

                # Loop through the captures
                for i in range(num_captures):
                    capture = captures[i]
                    timestamp_start = datetime.strptime(capture['timestamp_start'], '%Y-%m-%d %H:%M:%S')

                    # If this capture has a timestamp after the most recent timestamp for this trap,
                    # this and all following captures are new data
                    if timestamp_start >= incomplete_traps[trap_id]:
                        # Add the new data to the JSON object
                        new_captures = captures[i:]
                        num_new_captures = len(new_captures)
                        trap_data[trap_id]['Capture'].extend(new_captures)

                        # Update the most recent timestamp in incomplete_traps
                        ending_timestamp = new_captures[-1]['timestamp_end']
                        ending_datetime = datetime.strptime(ending_timestamp, '%Y-%m-%d %H:%M:%S')
                        incomplete_traps[trap_id] = ending_datetime

                        # No need to loop through the rest of the captures
                        break

                # If less than LIMIT captures were delivered, assume we now have all the data
                if num_captures < LIMIT:
                    del incomplete_traps[trap_id]

                    if display:
                        stdscr.hline(y, 23 + graph_width, ' ', 6)
                        stdscr.addstr(y, 23 + graph_width, 'Done')
                        stdscr.hline(y, 21, ' ', graph_width, curses.color_pair(2))

                elif display:
                    # Print the number of new captures to the right of the trap's line
                    stdscr.hline(y, 23 + graph_width, ' ', 6)
                    stdscr.addstr(y, 23 + graph_width, '+' + str(num_new_captures))

    if display:
        # Now that we're done, move the tracking line to the end
        erase_tracking_line(stdscr, trap_ys, graph_width)
        draw_tracking_line(stdscr, graph_width, trap_ys)

    # Unless we're doing a dry run, write the JSON objects to file
    if not args.dry:
        if display:
            print_status(stdscr, 'Writing to file...')

        i = 0

        if args.split_traps:
            for trap_id, trap_wrapper in trap_data.items():
                #print('trap_id: {} - Final captures: {}'.format(trap_id, len(captures)))

                # Don't write if we're skipping empty and there are no captures
                if not (args.skip_empty and not trap_wrapper['Capture']):
                    if args.nested_directories:
                        dir_path = './smart-trap-json/{}/{}'.format(api_key, trap_id)

                        if not os.path.exists(dir_path):
                            os.makedirs(dir_path)

                        filename = '{}_{}_{}.json'.format(trap_id, start_time.isoformat(), end_time.isoformat())
                        path = '{}/{}'.format(dir_path, filename)

                    else:
                        filename = '{}_{}'.format(args.output, i)
                        path = './' + filename

                    with open(path, 'w') as f:
                        trap_obj = {'traps' : [trap_wrapper]}
                        json.dump(trap_obj, f, indent=args.pretty_print)

                    i += 1

        else:
            trap_obj = {'traps' : []}

            for trap_wrapper in trap_data.values():
                if not (args.skip_empty and not trap_wrapper['Capture']):
                    trap_obj['traps'].append(trap_wrapper)

            if args.nested_directories:
                dir_path = './smart-trap-json/' + api_key

                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)

                filename = '{}_{}.json'.format(start_time.isoformat(), end_time.isoformat())
                path = '{}/{}'.format(dir_path, filename)

            else:
                path = './' + args.output

            with open(path, 'w') as f:
                json.dump(trap_obj, f, indent=args.pretty_print)

    if display:
        print_status(stdscr, 'Finished!')
        sleep(1.5)

def parse_args():
    parser = argparse.ArgumentParser(description='Pulls smart trap data.')

    group = parser.add_argument_group('arguments')
    group.add_argument('-k', '--api-key', required=True,
        help='The 32-character API key taken from the Biogents user dashboard. Keep dashes.')
    group.add_argument('-s', '--start-time', type=parse_date, required=True,
        help='Beginning of the target timeframe. Acceptable time formats ("T" is literal): '
        '"YYYY-MM-DD", "YYYY-MM-DDTHH:MM", "YYYY-MM-DDTHH:MM:SS"')
    group.add_argument('-e', '--end-time', type=parse_date, required=True,
        help='End of the target timeframe. Same acceptable formats as above.')
    group.add_argument('-p', '--pretty-print', action='store_const', const=4, default=None, help='Pretty print to file.')
    group.add_argument('--skip-empty', action='store_true', help='Don\'t write traps with no data to file.')
    group.add_argument('--no-display', dest='display', action='store_false', help='Don\'t show the graphical display.')
    group.add_argument('--split-traps', action='store_true', help='Write each trap into a separate file.')

    output_group_wrapper = parser.add_argument_group('output arguments', 'Must specify exactly one of the following.')
    output_group = output_group_wrapper.add_mutually_exclusive_group(required=True)
    output_group.add_argument('-d', '--dry', action='store_true', help='Don\'t write to file.')
    output_group.add_argument('-o', '--output',
        help='The name of the output file. If --split-traps is set, each separate file '
        'will end in "_X", where X is a unique integer.')
    output_group.add_argument('-n', '--nested-directories', action='store_true',
        help='Instead of writing to a file in the current directory, write to ./smart-trap-json/[API Key]/. '
        'The filename will be [Start Time]_[End Time].json. If --split-traps is set, write to '
        './smart-trap-json/[API Key]/[Trap ID]/. Each filename will be [Trap ID]_[Start Time]_[End Time].json')
 
    args = parser.parse_args()

    if not args.start_time < args.end_time:
        parser.error('End time must be after start time')

    if datetime.now() - args.end_time < timedelta(days=1):
        print('Warning: Attempting to get data from within the past 24 hours (or in the future)\n'
              '  may yield incomplete datasets. Continuing in 5 seconds.')
        sleep(5)

    return args

# Tries to create a datetime from a string and raises an argparse error if unsuccessful
def parse_date(string):
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(string, fmt)
        except ValueError:
            pass

    raise argparse.ArgumentTypeError(
        'Acceptable time formats ("T" is literal): "YYYY-MM-DD", "YYYY-MM-DDTHH:MM", "YYYY-MM-DDTHH:MM:SS"')

# Returns data in JSON format for a given key, start time, and end time
def request_data(stdscr, api_key, start_time, end_time):
    if stdscr:
        print_status(stdscr, 'Performing request...')

    data = {
        'data[user_id]' : api_key,
        'data[startTime]' : start_time.isoformat(' '),
        'data[endTime]' : end_time.isoformat(' ')
    }

    response = requests.post('http://live.bg-counter.com/traps/exportTrapCapturesForTimeFrame.json', data)
    response.raise_for_status()

    js = json.loads(response.text, object_pairs_hook=collections.OrderedDict)

    if stdscr:
        print_status(stdscr, 'Done.')

    return js

# Returns the graph position that a certain datetime maps to
def date_to_position(datetime, start_time, gradation):
    return math.floor((datetime - start_time) / gradation)

# Draws the tracking line at a certain position on the graph
def draw_tracking_line(stdscr, position, trap_ys):
    position += 21

    stdscr.addch(2, position, '+')
    stdscr.vline(3, position, '|', 2)

    for trap_id, y in trap_ys.items():
        stdscr.addch(y + 1, position, '|')

    stdscr.addch(2 * len(trap_ys) + 5, position, '|')
    stdscr.addch(2 * len(trap_ys) + 6, position, '+')
    stdscr.refresh()

# Erases the tracking line
def erase_tracking_line(stdscr, trap_ys, graph_width):
    stdscr.hline(2, 20, ' ', graph_width + 2)
    stdscr.hline(3, 21, ' ', graph_width)
    stdscr.hline(4, 21, ' ', graph_width)

    for trap_id, y in trap_ys.items():
        stdscr.hline(y + 1, 21, ' ', graph_width)

    stdscr.hline(2 * len(trap_ys) + 5, 21, ' ', graph_width)
    stdscr.hline(2 * len(trap_ys) + 6, 20, ' ', graph_width + 2)
    stdscr.refresh()

# Prints string at the top left of the window
def print_status(stdscr, string):
    stdscr.hline(1, 2, ' ', 30)
    stdscr.addstr(1, 2, string)
    stdscr.refresh()

args = parse_args()

if args.display:
    curses.wrapper(main, args)
else:
    main(None, args)
