"""
Pulls smart trap data from the Biogents API.

Pulls smart trap data in JSON format from the Biogents API given the API
key and the desired timeframe.  By default, this script shows a
graphical display of its progress.  This display shows a progress bar
for each trap detected in the initial request.  The left side of the
graph marks the beginning of the target timeframe and the right side the
end. Each trap's bar represents the most recent data that has been
collected for that trap.

For usage information, run with -h.

This script requires at least Python 3.5.
"""

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

import bg_common as com


def parse_args():
    """Parse the command line arguments and return an args namespace."""
    parser = argparse.ArgumentParser(description='Pulls smart trap data.')

    parser.add_argument('api_key', help='The 32-character API key taken from the Biogents user '
                                        'dashboard. Keep dashes.')
    parser.add_argument('start_time', type=com.parse_date,
                        help='Beginning of the target timeframe. Acceptable time formats ("T" is '
                             'literal): "YYYY-MM-DD", "YYYY-MM-DDTHH-MM", "YYYY-MM-DDTHH-MM-SS"')
    parser.add_argument('end_time', type=com.parse_date,
                        help='End of the target timeframe. Same acceptable formats as above.')

    parser.add_argument('-p', '--pretty-print', action='store_const', const=4, default=None,
                        help='Pretty print to file.')
    parser.add_argument('-t', '--trap', metavar='TRAP_ID', dest='target_traps', nargs='*',
                        type=valid_trap_id, help='Only get data for particular traps based on '
                                                 'their trap IDs.')
    parser.add_argument('--skip-empty', action='store_true',
                        help="Don't write traps with no data to file.")
    parser.add_argument('--no-display', dest='display', action='store_false',
                        help="Don't show the graphical display.")
    parser.add_argument('--split-traps', action='store_true',
                        help='Write each trap into a separate file.')

    output_group_wrapper = parser.add_argument_group('output arguments',
                                                     'Must specify exactly one of the following.')
    output_group = output_group_wrapper.add_mutually_exclusive_group(required=True)
    output_group.add_argument('-d', '--dry', action='store_true', help="Don't write to file.")
    output_group.add_argument('-o', '--output',
                              help='The name of the output file. If --split-traps is set, each '
                                   'separate file will end in "_X", where X is a unique integer.')
    output_group.add_argument('-n', '--nested-directories', action='store_true',
                              help='Instead of writing to a file in the current directory, write '
                                   'to ./smart-trap-json/[API Key]/. The filename will be '
                                   '[Start Time]_[End Time].json. If --split-traps is set, write '
                                   'to ./smart-trap-json/[API Key]/[Trap ID]/. Each filename '
                                   'will be [Trap ID]_[Start Time]_[End Time].json')

    args = parser.parse_args()

    if not args.start_time < args.end_time:
        parser.error('End time must be after start time')

    if dt.datetime.now() - args.end_time < dt.timedelta(days=1):
        print('Warning: Attempting to get data from within the past 24 hours (or in the future)\n'
              '  may yield incomplete datasets. Continuing in 5 seconds.')
        time.sleep(5)

    del args.nested_directories

    return args


def download_data(stdscr, api_key, start_time, end_time, output, target_traps=None,
                  split_traps=False, skip_empty=False, pretty_print=None, dry=False):
    """Download smart trap data over a specific timeframe.

    Required arguments:
    stdscr -- The curses screen object on which to show the graphical
        display.  'None' can be passed to indicate no graphical display.
    api_key -- The 32-character API key taken from the Biogents user
        dashboard.  Keep dashes.
    start_time -- A datetime object representing the beginning of the
        timeframe to get data over.
    end_time -- A datetime object representing the end of the timeframe
        to get data over.
    output -- A string containing the desired name of the output file.
        If 'None' is passed, the file will be placed within a directory
        structure described in the documentation for the
        --nested-directories option above.

    Optional arguments:
    target_traps -- A container holding one or more string trap IDs
        to exclusively get data for.  All other traps will be ignored.
        If not all target traps are found, a ValueError will be thrown.
    split_traps -- A boolean controlling whether to keep all traps'
        data in one file or to split each trap's data into a separate
        file.
    skip_empty -- A boolean controlling whether to list traps in the
        output file that did not provide any capture data.
    pretty_print -- A boolean controlling whether to add indentation
        to the final JSON output.
    dry -- Pass True to prevent the script from creating any files.
    """
    # The limit of data points (captures) per trap the API can deliver.
    limit = 1000

    if stdscr:
        # Get screen size.
        max_y, max_x = stdscr.getmaxyx()

        # Check whether the window is big enough.
        if max_x < 40:
            raise ValueError('Window is too narrow. Please widen to at least 40 columns '
                             'or use --no-display.')

        # Set up stuff related to curses.
        curses.curs_set(False)
        stdscr.nodelay(True)
        graph_width = max_x - 35
        duration = end_time - start_time
        gradation = duration / graph_width
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_GREEN)

        # Will contain the y-coordinate of each trap's line
        # in the display.
        trap_ys = {}

        # Create the pad we will draw on.
        pad = Pad(stdscr, max_y, max_x)
    else:
        pad = None

    # Perform a request on the full duration first.
    js = request_data(api_key, start_time, end_time, stdscr)

    # Keys will be traps with incomplete data and values will be
    # their most recent timestamps.
    incomplete_traps = {}

    # Will contain the JSON objects for each individual trap.
    trap_data = {}

    if stdscr:
        # Determine whether the pad needs to be resized.
        num_traps = len(js['traps'])
        min_height = 8 + 2*num_traps

        if min_height > max_y:
            pad.resize(min_height, max_x)

        # Move cursor to prepare for horizontal line drawing.
        pad.move(3, 2)

    # Loop over each trap.
    for trap_wrapper in js['traps']:
        trap_id = trap_wrapper['Trap']['id']

        # If no trap was specified or this trap was specified,
        # grab its data.
        if not target_traps or trap_id in target_traps:
            captures = trap_wrapper['Capture']
            num_captures = len(captures)
            trap_data[trap_id] = trap_wrapper

            if stdscr:
                # Draw horizontal lines.
                y, x = pad.getyx()
                y += 2
                pad.move(y, 2)
                trap_ys[trap_id] = y
                pad.addstr(trap_id)
                pad.hline(y, 21, '-', graph_width)

            # If we get limit capture for this trap, most likely we hit
            # the max and the trap has more data that wasn't delivered.
            if num_captures == limit:
                # Store the most recent timestamp in incomplete_traps.
                ending_timestamp = captures[-1]['timestamp_end']
                ending_datetime = dt.datetime.strptime(ending_timestamp, '%Y-%m-%d %H:%M:%S')
                incomplete_traps[trap_id] = ending_datetime

                if stdscr:
                    # Draw a white line to show the data
                    # we currently have and print the number of
                    # captures (limit) to the right of the line.
                    position = date_to_position(ending_datetime, start_time, gradation)
                    pad.hline(trap_ys[trap_id], 21, ' ', position, curses.color_pair(1))
                    pad.addstr(y, 23 + graph_width, str(num_captures))
                else:
                    percentage = date_to_percentage(ending_datetime, start_time, end_time)
                    print('Trap {}: {}% complete.'.format(trap_id, percentage))

            # If the API is ever improved so that it can deliver more
            # than limit captures, we'll have to modify this script.
            elif num_captures > limit:
                raise ValueError('More than {} (limit) captures for a trap: {}.'
                                 .format(limit, num_captures))

            # Else, assume that we got all captures.
            else:
                if stdscr:
                    pad.addstr(y, 23 + graph_width, 'Done')
                    pad.hline(y, 21, ' ', graph_width, curses.color_pair(2))
                else:
                    print('Trap {}: 100% complete.'.format(trap_id))

    # If traps were specified, check to see if they're all there.
    if target_traps:
        diff = set(target_traps) - set(trap_data.keys())

        if diff:
            raise ValueError('Trap ID(s) not found in response: ' + ', '.join(diff))

    if stdscr:
        # Draw vertical lines.
        pad.vline(3, 20, '|', 2*len(trap_ys) + 3)
        pad.vline(3, 21 + graph_width, '|', 2*len(trap_ys) + 3)
        pad.addch(2, 20, '+')
        pad.addch(2*len(trap_ys) + 6, 20, '+')

        pad.refresh()

    # Loop while there are still traps with more data.
    while incomplete_traps:
        # Find which ending timestamp is the earliest.
        ending_dates = list(incomplete_traps.values())
        earliest_date = ending_dates[0]

        for date in ending_dates:
            if date < earliest_date:
                earliest_date = date

        if stdscr:
            # Move the tracking line to the earliest time.
            position = date_to_position(earliest_date, start_time, gradation)
            erase_tracking_line(graph_width, trap_ys, pad)
            draw_tracking_line(position, trap_ys, pad)

        # Perform a request from the earliest time to the original
        # end time with a second delay so we don't overload the server.
        time.sleep(1)
        new_js = request_data(api_key, earliest_date, end_time, pad)

        # Turn all previous data green.
        if stdscr:
            for trap_id, ending_date in incomplete_traps.items():
                position = date_to_position(ending_date, start_time, gradation)
                pad.hline(trap_ys[trap_id], 21, ' ', position, curses.color_pair(2))

            pad.refresh()

        for trap_wrapper in new_js['traps']:
            trap_id = trap_wrapper['Trap']['id']

            if trap_id in incomplete_traps:
                captures = trap_wrapper['Capture']
                num_captures = len(captures)
                num_new_captures = 0

                if stdscr:
                    # Get the last timestamp and draw a white bar
                    # for all new data.
                    y = trap_ys[trap_id]
                    last_ending_position = date_to_position(incomplete_traps[trap_id], start_time,
                                                            gradation)
                    ending_timestamp = captures[-1]['timestamp_end']
                    ending_datetime = dt.datetime.strptime(ending_timestamp, '%Y-%m-%d %H:%M:%S')
                    position = date_to_position(ending_datetime, start_time, gradation)
                    pad.hline(y, last_ending_position + 21, ' ', position - last_ending_position,
                              curses.color_pair(1))
                    pad.refresh()

                # Check if there are more than limit captures
                # for the same reason as before.
                if num_captures > limit:
                    raise ValueError('More than {} (limit) captures for a trap: {}.'
                                     .format(limit, num_captures))

                # Loop through the captures.
                for i in range(num_captures):
                    capture = captures[i]
                    timestamp_start = com.make_datetime(capture['timestamp_start'])
                    timestamp_end = com.make_datetime(capture['timestamp_end'])

                    # If this capture has valid timestamps and has
                    # a timestamp after the most recent timestamp
                    # for this trap, this and all following captures
                    # are new data.
                    if (timestamp_start and timestamp_end
                            and timestamp_start >= incomplete_traps[trap_id]):
                        # Add the new data to the JSON object.
                        new_captures = captures[i:]
                        num_new_captures = len(new_captures)
                        trap_data[trap_id]['Capture'].extend(new_captures)

                        # Update the most recent timestamp
                        # in incomplete_traps.
                        ending_timestamp = new_captures[-1]['timestamp_end']
                        ending_datetime = com.make_datetime(ending_timestamp)

                        # If the last timestamp_end for a trap in
                        # a request is empty, this might mean that all
                        # its timestamp_ends are empty, which is
                        # a problem.
                        if not ending_datetime:
                            raise ValueError('Last ending timestamp is empty at capture ID: '
                                             + capture['id'])

                        incomplete_traps[trap_id] = ending_datetime

                        # No need to loop through the rest
                        # of the captures.
                        break

                # If less than limit captures were delivered, assume
                # we now have all the data.
                if num_captures < limit:
                    del incomplete_traps[trap_id]

                    if stdscr:
                        pad.hline(y, 23 + graph_width, ' ', 6)
                        pad.addstr(y, 23 + graph_width, 'Done')
                        pad.hline(y, 21, ' ', graph_width, curses.color_pair(2))
                    else:
                        print('Trap {}: 100% complete.'.format(trap_id))

                else:
                    if stdscr:
                        # Print the number of new captures
                        # to the right of the trap's line.
                        pad.hline(y, 23 + graph_width, ' ', 6)
                        pad.addstr(y, 23 + graph_width, '+' + str(num_new_captures))
                    else:
                        percentage = date_to_percentage(incomplete_traps[trap_id],
                                                        start_time, end_time)
                        print('Trap {}: {}% complete. ({} new captures)'
                              .format(trap_id, percentage, num_new_captures))

    if stdscr:
        # Now that we're done, move the tracking line to the end.
        erase_tracking_line(graph_width, trap_ys, pad)
        draw_tracking_line(graph_width, trap_ys, pad)

    # Unless we're doing a dry run, write the JSON objects to file.
    if not dry:
        write_to_file(trap_data, api_key, start_time, end_time, output,
                      split_traps, skip_empty, pretty_print, pad)

    if stdscr:
        print_status('Finished!', pad)
        time.sleep(1.5)
    else:
        print('Finished.')


def write_to_file(trap_data, api_key, start_time, end_time, output,
                  split_traps=False, skip_empty=False, pretty_print=None, screen=None):
    """Write smart trap JSON data to file.

    Required arguments:
    trap_data -- The dict containing the smart trap data.
    api_key -- The 32-character API key taken from the Biogents user
        dashboard.  Used only if the output option isn't provided.
    start_time -- A datetime object representing the beginning of the
        timeframe that contains this data.
    end_time -- A datetime object representing the end of the timeframe
        that contains this data.
    output -- A string containing the desired name of the output file.
        If 'None' is passed, the file will be placed within a directory
        structure described in the documentation for the
        --nested-directories option above.

    Optional arguments:
    split_traps -- A boolean controlling whether to keep all traps'
        data in one file or to split each trap's data into a separate
        file.
    skip_empty -- A boolean controlling whether to list traps in the
        output file that did not provide any capture data.
    pretty_print -- A boolean controlling whether to add indentation
        to the output files.
    screen -- The curses screen object on which to print the status.
        'None' indicates that a curses screen is not being used.
    """
    # Write trap data to file.
    if screen:
        print_status('Writing to file...', screen)
    else:
        print('Writing to file...')

    date_fmt = '%Y-%m-%d'
    midnight = dt.time()  # dt.time() defaults to 00:00:00.

    # If either time contains some non-zero value,
    # we'll print both fully.
    if start_time.time() != midnight or end_time.time() != midnight:
        date_fmt += 'T%H-%M-%S'

    if split_traps:
        i = 0

        for trap_id, trap_wrapper in trap_data.items():
            # Don't write if we're skipping empty
            # and there are no captures.
            if not (skip_empty and not trap_wrapper['Capture']):
                if output:
                    filename = '{}_{}'.format(output, i)
                    path = './' + filename

                else:
                    dir_path = './smart-trap-json/{}/{}'.format(api_key, trap_id)

                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path)

                    filename = '{}_{}_{}.json'.format(trap_id, start_time.strftime(date_fmt),
                                                      end_time.strftime(date_fmt))
                    path = '{}/{}'.format(dir_path, filename)

                with open(path, 'w') as f:
                    trap_obj = {'traps': [trap_wrapper]}
                    json.dump(trap_obj, f, indent=pretty_print)

                i += 1

    else:
        trap_obj = {'traps': []}

        for trap_wrapper in trap_data.values():
            if not (skip_empty and not trap_wrapper['Capture']):
                trap_obj['traps'].append(trap_wrapper)

        if output:
            path = './' + output

        else:
            dir_path = './smart-trap-json/' + api_key

            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            filename = '{}_{}.json'.format(start_time.strftime(date_fmt),
                                           end_time.strftime(date_fmt))
            path = '{}/{}'.format(dir_path, filename)

        with open(path, 'w') as f:
            json.dump(trap_obj, f, indent=pretty_print)


def valid_trap_id(string):
    """Check a string to see if it's a valid trap ID."""
    if not re.match('^[0-9]{15}$', string):
        raise argparse.ArgumentTypeError('Invalid trap ID: ' + string)

    return string


def request_data(api_key, start_time, end_time, screen):
    """Send a request for smart trap data and return data as a dict."""
    if screen:
        print_status('Performing request...', screen)
    else:
        print('Performing request...')

    url = 'http://live.bg-counter.com/traps/exportTrapCapturesForTimeFrame.json'

    data = {
        'data[user_id]': api_key,
        'data[startTime]': start_time.isoformat(' '),
        'data[endTime]': end_time.isoformat(' ')
    }

    response = requests.post(url, data)
    response.raise_for_status()

    try:
        js = json.loads(response.text, object_pairs_hook=collections.OrderedDict)
    except json.JSONDecodeError:
        if screen:
            curses.endwin()

        print("Response text:\n'" + response.text + "'\n")
        raise

    if screen:
        print_status('Done.', screen)
    else:
        print('Done.')

    return js


def date_to_position(datetime, start_time, gradation):
    """Return the graph position that a datetime maps to."""
    return math.floor((datetime-start_time) / gradation)


def date_to_percentage(datetime, start_time, end_time):
    """Return the percentage of the timeframe a datetime represents."""
    return math.floor(((datetime-start_time) / (end_time-start_time)) * 100)


def draw_tracking_line(position, trap_ys, screen):
    """Draw the tracking line at the given position on the graph."""
    position += 21

    screen.addch(2, position, '+')
    screen.vline(3, position, '|', 2)

    for trap_id, y in trap_ys.items():
        screen.addch(y + 1, position, '|')

    screen.addch(2*len(trap_ys) + 5, position, '|')
    screen.addch(2*len(trap_ys) + 6, position, '+')
    screen.refresh()


def erase_tracking_line(graph_width, trap_ys, screen):
    """Erase the tracking line."""
    screen.hline(2, 20, ' ', graph_width + 2)
    screen.hline(3, 21, ' ', graph_width)
    screen.hline(4, 21, ' ', graph_width)

    for trap_id, y in trap_ys.items():
        screen.hline(y + 1, 21, ' ', graph_width)

    screen.hline(2*len(trap_ys) + 5, 21, ' ', graph_width)
    screen.hline(2*len(trap_ys) + 6, 20, ' ', graph_width + 2)
    screen.refresh()


def print_status(string, screen):
    """Print the given string at the top left of the screen."""
    screen.hline(1, 2, ' ', 30)
    screen.addstr(1, 2, string)
    screen.refresh()


class Pad:
    """Create and manage a pad potentially larger than the screen size.

    This class encapsulates curses' window class to create a pad that
    behaves well even if it's larger than the screen size, including
    providing basic scrolling functionality.

    Public methods:
        resize
        check_input
        scroll
        refresh
    """

    def __init__(self, stdscr, nlines, ncols):
        """Initialize the instance and create the main pad object."""
        self.stdscr = stdscr
        self.pad = curses.newpad(nlines, ncols)
        self.scr_height, self.scr_width = stdscr.getmaxyx()
        self.top_shown = self.max_top_shown = 0

    def __getattr__(self, item):
        """Implement encapsulation.

        This method implements the encapsulation of the curses window
        object by routing all undefined attributes to the internal pad.

        item -- The attribute that was called.
        """
        return getattr(self.pad, item)

    def resize(self, nlines, ncols):
        """Resize the pad."""
        self.pad.resize(nlines, ncols)
        self.max_top_shown = nlines - self.scr_height

    def check_input(self):
        """Read keyboard input and perform the appropriate action."""
        pages = 0
        code = self.stdscr.getch()

        # Find the cumulative result of the key presses.
        while code >= 0:
            if code == curses.KEY_UP:
                pages -= 1
            elif code == curses.KEY_DOWN:
                pages += 1

            code = self.stdscr.getch()

        if pages != 0:
            self.scroll(pages)

    def scroll(self, num_pages):
        """Scroll the pad some number of pages.

        Note that a positive num_pages scrolls down while a negative
        scrolls up.
        """
        self.top_shown = self.top_shown + num_pages*self.scr_height

        self.top_shown = max(self.top_shown, 0)
        self.top_shown = min(self.top_shown, self.max_top_shown)

    def refresh(self):
        """Refresh the pad after checking for input."""
        self.check_input()
        self.pad.refresh(self.top_shown, 0, 0, 0, self.scr_height - 1, self.scr_width)


if __name__ == '__main__':
    args = vars(parse_args())

    display = args['display']
    del args['display']

    if display:
        curses.wrapper(download_data, **args)
    else:
        download_data(None, **args)
