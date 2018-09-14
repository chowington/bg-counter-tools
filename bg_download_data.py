################
# Python 3.5.2 #
################

import argparse
import collections
import curses
import json
import math
import os
import re
import time
import datetime as dt

import requests


# Parses the command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Pulls smart trap data.')

    parser.add_argument('api_key', help='The 32-character API key taken from the Biogents user dashboard. Keep dashes.')
    parser.add_argument('start_time', type=parse_date,
        help='Beginning of the target timeframe. Acceptable time formats ("T" is literal): '
        '"YYYY-MM-DD", "YYYY-MM-DDTHH-MM", "YYYY-MM-DDTHH-MM-SS"')
    parser.add_argument('end_time', type=parse_date, help='End of the target timeframe. Same acceptable formats as above.')

    parser.add_argument('-p', '--pretty-print', action='store_const', const=4, default=None, help='Pretty print to file.')
    parser.add_argument('-t', '--trap', metavar='TRAP_ID', nargs='*', type=valid_trap_id,
        help='Only get data for particular traps based on their trap IDs.')
    parser.add_argument('--skip-empty', action='store_true', help='Don\'t write traps with no data to file.')
    parser.add_argument('--no-display', dest='display', action='store_false', help='Don\'t show the graphical display.')
    parser.add_argument('--split-traps', action='store_true', help='Write each trap into a separate file.')

    output_group_wrapper = parser.add_argument_group('output arguments', 'Must specify exactly one of the following.')
    output_group = output_group_wrapper.add_mutually_exclusive_group(required=True)
    output_group.add_argument('-d', '--dry', action='store_true', help='Don\'t write to file.')
    output_group.add_argument('-o', '--output',
        help='The name of the output file. If --split-traps is set, each separate file '
        'will end in "_X", where X is a unique integer.')
    output_group.add_argument('-n', '--nested-directories', dest='directories', action='store_true',
        help='Instead of writing to a file in the current directory, write to ./smart-trap-json/[API Key]/. '
        'The filename will be [Start Time]_[End Time].json. If --split-traps is set, write to '
        './smart-trap-json/[API Key]/[Trap ID]/. Each filename will be [Trap ID]_[Start Time]_[End Time].json')
 
    args = parser.parse_args()

    if not args.start_time < args.end_time:
        parser.error('End time must be after start time')

    if dt.datetime.now() - args.end_time < dt.timedelta(days=1):
        print('Warning: Attempting to get data from within the past 24 hours (or in the future)\n'
              '  may yield incomplete datasets. Continuing in 5 seconds.')
        time.sleep(5)

    return args


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
        stdscr.move(3, 2)
        trap_ys = {}  # Will contain the y-coordinate of each trap's line in the display

    # Perform a request on the full duration first
    js = request_data(api_key, start_time, end_time, stdscr)

    incomplete_traps = {}  # Keys will be traps with incomplete data and values will be their most recent timestamps
    trap_data = {}  # Will contain the JSON objects for each individual trap

    if display:
        # Move cursor to prepare for horizontal line drawing
        stdscr.move(3, 2)

    # Loop over each trap
    for trap_wrapper in js['traps']:
        trap_id = trap_wrapper['Trap']['id']

        # If no trap was specified or this trap was specified, grab its data
        if not args.trap or trap_id in args.trap:
            captures = trap_wrapper['Capture']
            num_captures = len(captures)
            trap_data[trap_id] = trap_wrapper

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
                ending_datetime = dt.datetime.strptime(ending_timestamp, '%Y-%m-%d %H:%M:%S')
                incomplete_traps[trap_id] = ending_datetime

                if display:
                    # Draw a white line to show the data we currently have
                    # and print the number of captures (LIMIT) to the right of the line
                    position = date_to_position(ending_datetime, start_time, gradation)
                    stdscr.hline(trap_ys[trap_id], 21, ' ', position, curses.color_pair(1))
                    stdscr.addstr(y, 23 + graph_width, str(num_captures))
                else:
                    percentage = date_to_percentage(ending_datetime, start_time, end_time)
                    print('Trap {}: {}% complete.'.format(trap_id, percentage))

            # If the API is ever improved so that it can deliver more than LIMIT captures,
            # we'll have to modify this script
            elif num_captures > LIMIT:
                raise ValueError('More than {} (LIMIT) captures for a trap: {}.'.format(LIMIT, num_captures))

            # Else, assume that we got all captures
            else:
                if display:
                    stdscr.addstr(y, 23 + graph_width, 'Done')
                    stdscr.hline(y, 21, ' ', graph_width, curses.color_pair(2))
                else:
                    print('Trap {}: 100% complete.'.format(trap_id))

    # If traps were specified, check to see if they're all there
    if args.trap:
        diff = set(args.trap) - set(trap_data.keys())

        if diff:
            raise ValueError('Trap ID(s) not found in response: ' + ', '.join(diff))

    if display:
        # Draw vertical lines
        stdscr.vline(3, 20, '|', 2 * len(trap_ys) + 3)
        stdscr.vline(3, 21 + graph_width, '|', 2 * len(trap_ys) + 3)
        stdscr.addch(2, 20, '+')
        stdscr.addch(2 * len(trap_ys) + 6, 20, '+')

        stdscr.refresh()

    # While there are still traps with more data
    while incomplete_traps:
        # Find which ending timestamp is the earliest
        ending_dates = list(incomplete_traps.values())
        earliest_date = ending_dates[0]

        for date in ending_dates:
            if date < earliest_date:
                earliest_date = date

        if display:
            # Move the tracking line to the earliest time
            position = date_to_position(earliest_date, start_time, gradation)
            erase_tracking_line(graph_width, trap_ys, stdscr)
            draw_tracking_line(position, trap_ys, stdscr)

        # Perform a request from the earliest time to the original end time
        # with a half-second delay so we don't overload the server
        time.sleep(0.5)
        new_js = request_data(api_key, earliest_date, end_time, stdscr)

        # Turn all previous data green
        if display:
            for trap_id, ending_date in incomplete_traps.items():
                position = date_to_position(ending_date, start_time, gradation)
                stdscr.hline(trap_ys[trap_id], 21, ' ', position, curses.color_pair(2))
    
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
                    ending_datetime = dt.datetime.strptime(ending_timestamp, '%Y-%m-%d %H:%M:%S')
                    position = date_to_position(ending_datetime, start_time, gradation)
                    stdscr.hline(y, last_ending_position + 21, ' ', position - last_ending_position, curses.color_pair(1))
                    stdscr.refresh()

                # Check if there are more than LIMIT captures for the same reason as before
                if num_captures > LIMIT:
                    raise ValueError('More than {} (LIMIT) captures for a trap: {}.'.format(LIMIT, num_captures))

                # Loop through the captures
                for i in range(num_captures):
                    capture = captures[i]
                    timestamp_start = dt.datetime.strptime(capture['timestamp_start'], '%Y-%m-%d %H:%M:%S')

                    # If this capture has a timestamp after the most recent timestamp for this trap,
                    # this and all following captures are new data
                    if timestamp_start >= incomplete_traps[trap_id]:
                        # Add the new data to the JSON object
                        new_captures = captures[i:]
                        num_new_captures = len(new_captures)
                        trap_data[trap_id]['Capture'].extend(new_captures)

                        # Update the most recent timestamp in incomplete_traps
                        ending_timestamp = new_captures[-1]['timestamp_end']
                        ending_datetime = dt.datetime.strptime(ending_timestamp, '%Y-%m-%d %H:%M:%S')
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
                    else:
                        print('Trap {}: 100% complete.'.format(trap_id))

                else:
                    if display:
                        # Print the number of new captures to the right of the trap's line
                        stdscr.hline(y, 23 + graph_width, ' ', 6)
                        stdscr.addstr(y, 23 + graph_width, '+' + str(num_new_captures))
                    else:
                        percentage = date_to_percentage(incomplete_traps[trap_id], start_time, end_time)
                        print('Trap {}: {}% complete. ({} new captures)'.format(trap_id, percentage, num_new_captures))

    if display:
        # Now that we're done, move the tracking line to the end
        erase_tracking_line(graph_width, trap_ys, stdscr)
        draw_tracking_line(graph_width, trap_ys, stdscr)

    # Unless we're doing a dry run, write the JSON objects to file
    if not args.dry:
        write_to_file(trap_data, api_key, start_time, end_time, args.output, args.split_traps, args.skip_empty,
                      args.directories, args.pretty_print, stdscr)

    if display:
        print_status('Finished!', stdscr)
        time.sleep(1.5)
    else:
        print('Finished.')


# Writes trap data to file
def write_to_file(trap_data, api_key, start_time, end_time, output='captures', split_traps=False, skip_empty=False,
                  directories=False, pretty_print=False, stdscr=None):
    if stdscr:
        print_status('Writing to file...', stdscr)
    else:
        print('Writing to file...')

    date_fmt = '%Y-%m-%d'
    midnight = dt.time()  # Defaults to 00:00:00 (midnight)

    # If either time contains some non-zero value, we'll print both fully
    if start_time.time() != midnight or end_time.time() != midnight:
        date_fmt += 'T%H-%M-%S'

    if split_traps:
        i = 0

        for trap_id, trap_wrapper in trap_data.items():
            # Don't write if we're skipping empty and there are no captures
            if not (skip_empty and not trap_wrapper['Capture']):
                if directories:
                    dir_path = './smart-trap-json/{}/{}'.format(api_key, trap_id)

                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path)

                    filename = '{}_{}_{}.json'.format(trap_id, start_time.strftime(date_fmt), end_time.strftime(date_fmt))
                    path = '{}/{}'.format(dir_path, filename)

                else:
                    filename = '{}_{}'.format(output, i)
                    path = './' + filename

                with open(path, 'w') as f:
                    trap_obj = {'traps': [trap_wrapper]}
                    json.dump(trap_obj, f, indent=pretty_print)

                i += 1

    else:
        trap_obj = {'traps': []}

        for trap_wrapper in trap_data.values():
            if not (args.skip_empty and not trap_wrapper['Capture']):
                trap_obj['traps'].append(trap_wrapper)

        if directories:
            dir_path = './smart-trap-json/' + api_key

            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            filename = '{}_{}.json'.format(start_time.strftime(date_fmt), end_time.strftime(date_fmt))
            path = '{}/{}'.format(dir_path, filename)

        else:
            path = './' + args.output

        with open(path, 'w') as f:
            json.dump(trap_obj, f, indent=pretty_print)


# Tries to create a datetime from a string and raises an argparse error if unsuccessful
def parse_date(string):
    for fmt in ('%Y-%m-%dT%H-%M-%S', '%Y-%m-%dT%H-%M', '%Y-%m-%d'):
        try:
            return dt.datetime.strptime(string, fmt)
        except ValueError:
            pass

    raise argparse.ArgumentTypeError(
        'Acceptable time formats ("T" is literal): "YYYY-MM-DD", "YYYY-MM-DDTHH-MM", "YYYY-MM-DDTHH-MM-SS"')


# Checks a string to see if it's a valid trap ID
def valid_trap_id(string):
    if not re.match('^[0-9]{15}$', string):
        raise argparse.ArgumentTypeError('Invalid trap ID: ' + string)

    return string


# Returns data in JSON format for a given key, start time, and end time
def request_data(api_key, start_time, end_time, stdscr=None):
    if stdscr:
        print_status('Performing request...', stdscr)
    else:
        print('Performing request...')

    data = {
        'data[user_id]' : api_key,
        'data[startTime]' : start_time.isoformat(' '),
        'data[endTime]' : end_time.isoformat(' ')
    }

    response = requests.post('http://live.bg-counter.com/traps/exportTrapCapturesForTimeFrame.json', data)
    response.raise_for_status()

    js = json.loads(response.text, object_pairs_hook=collections.OrderedDict)

    if stdscr:
        print_status('Done.', stdscr)
    else:
        print('Done.')

    return js


# Returns the graph position that a certain datetime maps to
def date_to_position(datetime, start_time, gradation):
    return math.floor((datetime - start_time) / gradation)


# Returns the percentage that a certain datetime represents
def date_to_percentage(datetime, start_time, end_time):
    return math.floor(((datetime - start_time) / (end_time - start_time)) * 100)


# Draws the tracking line at a certain position on the graph
def draw_tracking_line(position, trap_ys, stdscr):
    position += 21

    stdscr.addch(2, position, '+')
    stdscr.vline(3, position, '|', 2)

    for trap_id, y in trap_ys.items():
        stdscr.addch(y + 1, position, '|')

    stdscr.addch(2 * len(trap_ys) + 5, position, '|')
    stdscr.addch(2 * len(trap_ys) + 6, position, '+')
    stdscr.refresh()


# Erases the tracking line
def erase_tracking_line(graph_width, trap_ys, stdscr):
    stdscr.hline(2, 20, ' ', graph_width + 2)
    stdscr.hline(3, 21, ' ', graph_width)
    stdscr.hline(4, 21, ' ', graph_width)

    for trap_id, y in trap_ys.items():
        stdscr.hline(y + 1, 21, ' ', graph_width)

    stdscr.hline(2 * len(trap_ys) + 5, 21, ' ', graph_width)
    stdscr.hline(2 * len(trap_ys) + 6, 20, ' ', graph_width + 2)
    stdscr.refresh()


# Prints string at the top left of the window
def print_status(string, stdscr):
    stdscr.hline(1, 2, ' ', 30)
    stdscr.addstr(1, 2, string)
    stdscr.refresh()


if __name__ == '__main__':
    args = parse_args()

    if args.display:
        curses.wrapper(main, args)
    else:
        main(None, args)
