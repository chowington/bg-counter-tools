################
# Python 3.5.2 #
################

import argparse
import csv
import json
import math
import datetime as dt
import os

import bg_common as com


def parse_args():
    parser = argparse.ArgumentParser(description='Parses JSON delivered by the Biogents smart trap API '
        'and creates an interchange format file from the data.')

    parser.add_argument('files', nargs='+', metavar='file', help='The JSON file(s) to parse.')
    parser.add_argument('--preserve-metadata', action='store_true', help='Don\'t change the metadata in the database in any way')

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument('-o', '--output', help='The name of the output file.')
    output_group.add_argument('-y', '--split-years', action='store_true',
        help='Split data from different years into separate output files. The files will be named [prefix]_[year].pop.')

    args = parser.parse_args()

    return args


def parse_json(files, output='interchange.pop', split_years=False, preserve_metadata=False):
    out_csv = None

    try:
        metadata = {}

        if split_years:
            out_csv = {}
        else:
            out_csv = CSVWriter(output)

        for filename in files:
            with open(filename, 'r') as json_f:
                js = json.load(json_f)

                print("Processing file " + filename)

                for trap_wrapper in js['traps']:
                    trap_id = trap_wrapper['Trap']['id']
                    captures = trap_wrapper['Capture']

                    if len(captures) != 0:
                        # Get metadata for this trap
                        for prefix, trapset in metadata.items():
                            if trap_id in trapset['traps']:
                                curr_prefix = prefix
                                curr_trapset = trapset
                                break
                        else:
                            new_metadata = get_metadata(trap_id=trap_id)
                            metadata.update(new_metadata)
                            curr_prefix = list(new_metadata.keys())[0]
                            curr_trapset = new_metadata[curr_prefix]

                        trap_metadata = {'prefix': curr_prefix, 'locations': curr_trapset['traps'][trap_id],
                                         'ordinals': curr_trapset['ordinals']}

                        # Process captures
                        total_captures, good_captures = process_captures(captures, trap_metadata, out_csv)

                        # Update master metadata
                        curr_trapset['traps'][trap_id] = trap_metadata['locations']
                        curr_trapset['ordinals'] = trap_metadata['ordinals']

                        print('Trap {}: Total captures: {} - Good captures: {} ({}%)'
                              .format(trap_id, total_captures, good_captures, math.floor((good_captures / total_captures) * 100)))

                    # Warn if a trap is showing no captures. We should reasonably expect data from each trap,
                    # and if we aren't getting any, it might be worth looking into
                    else:
                        print('Warning: 0 captures at trap_id: ' + trap_id)

        if not preserve_metadata:
            update_metadata(metadata=metadata)

    finally:
        # Close all output files
        if out_csv:
            if type(out_csv) is dict:
                for prefix, years in out_csv.items():
                    for year, csv_writer in years.items():
                        csv_writer.close()
            else:
                out_csv.close()


# Takes a set of captures from a single trap and bins them into days
# before sending them off to be collected and written to file
def process_captures(captures, metadata, out_csv):
    total_captures = 0  # The total number of unique captures (some are duplicates)
    good_captures = 0  # The number of captures that end up being collated into a collection
    day_captures = []  # The captures within a single day
    prev_end_timestamp = dt.datetime.min  # Will hold the timestamp_end of the last capture
    num_captures = len(captures)

    for i in range(num_captures):
        capture = captures[i]

        # We use this to do some sanity checking.
        # The ending timestamp is more consistent than the starting one
        curr_end_timestamp = com.make_datetime(capture['timestamp_end'])
        curr_start_timestamp = com.make_datetime(capture['timestamp_start'])
        curr_date = curr_start_timestamp.date()

        valid_dates = curr_end_timestamp and curr_start_timestamp

        # Count a capture if it's not a duplicate or it has invalid dates
        if not valid_dates or curr_end_timestamp != prev_end_timestamp:
            total_captures += 1

        if valid_dates:
            # If this end timestamp is later than the previous one, store the capture.
            # We ignore the capture if it's identical to the previous one
            # or if its timeframe is much less than 15 minutes
            if curr_end_timestamp > prev_end_timestamp and curr_end_timestamp - curr_start_timestamp >= dt.timedelta(minutes=12):
                day_captures.append({
                    'trap_id': capture['trap_id'],
                    'timestamp_start': capture['timestamp_start'],
                    'co2_status': capture['co2_status'],
                    'counter_status': capture['counter_status'],
                    'medium': capture['medium'],
                    'trap_latitude': capture['trap_latitude'],
                    'trap_longitude': capture['trap_longitude'],
                })

                prev_end_timestamp = curr_end_timestamp

            # Else if this timestamp is earlier than the previous one, error out.
            # We rely on the captures being delivered in forward chronological order
            elif curr_end_timestamp < prev_end_timestamp:
                raise ValueError('Capture has earlier ending timestamp than preceding capture. Capture ID: ' + capture['id'])

            # If we're at the last capture or the next capture is from a different day, end this day
            if i == num_captures - 1 or com.make_date(captures[i + 1]['timestamp_start']) != curr_date:
                trap_id = capture['trap_id']

                # Our current assumption is that there are no more than 96 unique captures
                # in a day (4 per hour) - if this changes, we'll need to edit this script
                if len(day_captures) > 96:
                    raise ValueError('More than 96 captures in a day at trap_id: {} - date: {}'.format(trap_id, curr_date))

                # Try to make a collection from this set of captures
                collection = make_collection(day_captures, metadata['locations'])

                # Get the correct output file
                if type(out_csv) is dict:
                    prefix = metadata['prefix']
                    year = curr_date.year

                    # If the correct output file doesn't exist, make it
                    if prefix not in out_csv:
                        out_csv[prefix] = {}

                    if year not in out_csv[prefix]:
                        filename = '{}_{}.pop'.format(prefix, year)
                        out_csv[prefix][year] = CSVWriter(filename)

                    curr_csv = out_csv[prefix][year]

                else:
                    curr_csv = out_csv

                # If a collection could be made, write it to file and count its captures as good
                if collection and write_collection(collection, metadata, curr_csv):
                    good_captures += len(collection['captures'])

                day_captures = []

    return total_captures, good_captures


# Takes a set of captures within the same day, bins them based on location,
# and returns the collection that is big enough, returning None if there is none.
# It also adds new locations to the metadata dict if there are any.
def make_collection(captures, locations):
    # Add a new key that will hold the captures that map to each location
    # and a key that will let us know that these locations are not new (used later)
    for location in locations:
        location['captures'] = []
        location['new'] = False

    # First, loop through the captures to pinpoint any possible new locations.
    # We're looping backwards so we can delete captures if necessary
    for i in range(len(captures) - 1, -1, -1):
        capture = captures[i]
        curr_lat = float(capture['trap_latitude'])
        curr_lon = float(capture['trap_longitude'])

        # If a trap can't get correct GPS data, it will either report a coordinate that is exactly 0
        # or report its location as (51.4778, 0.0014), which is in Greenwich near the prime meridian.
        # Either way, drop the capture
        if curr_lat == 0 or curr_lon == 0 or (curr_lat == 51.4778 and curr_lon == 0.0014):
            del captures[i]

        else:
            # Determine whether this capture is close to any existing locations
            for location in locations:
                distance = calculate_distance(curr_lat, curr_lon, location['latitude'], location['longitude'])

                if distance < 0.111:  # 111 meters - arbitrary, but shouldn't be too small
                    break

            # If it's not close to any known location, add a new location at its coordinates.
            # This bubbles up to the metadata dict as well
            else:
                locations.append({
                    'latitude': curr_lat,
                    'longitude': curr_lon,
                    'captures': [],
                    'new': True
                })

    # Next, loop through the captures and assign them to the closest locations
    for capture in captures:
        curr_lat = float(capture['trap_latitude'])
        curr_lon = float(capture['trap_longitude'])

        closest_location = None
        closest_distance = math.inf

        for location in locations:
            distance = calculate_distance(curr_lat, curr_lon, location['latitude'], location['longitude'])

            if distance < closest_distance:
                closest_location = location
                closest_distance = distance

        # Because of the previous loop, each capture will be within 111 meters of some location
        closest_location['captures'].append(capture)

    collection = None  # This will hold the final collection if there is one

    # Once again, loop backwards so we can remove items
    for i in range(len(locations) - 1, -1, -1):
        location = locations[i]
        num_captures = len(location['captures'])

        # Allow at most one cumulative hour of missing data in a day
        if num_captures >= 92:
            # If the location is new, average its captures' coordinates to get a more accurate lat/lon
            if location['new']:
                lats, lons = [], []

                for capture in location['captures']:
                    lats.append(float(capture['trap_latitude']))
                    lons.append(float(capture['trap_longitude']))

                location['latitude'] = round(sum(lats) / len(lats), 6)
                location['longitude'] = round(sum(lons) / len(lons), 6)

            collection = dict(location)

        # If there weren't enough captures for a full collection and the location was new,
        # remove it so it doesn't get added to the metadata
        elif location['new']:
            del locations[i]

        # Remove 'captures' and 'new'
        del location['captures']
        del location['new']

    return collection


# Takes a collection containing a day's worth of captures and aggregates and writes it to file
# if the counter was on at some point during the day. Returns True if the collection was written
# and False if it wasn't.
def write_collection(collection, metadata, out_csv):
    captures = collection['captures']
    mos_count = 0
    counter_on = False  # Stores whether the counter was turned on at some point in the day
    used_co2 = False  # Stores whether CO2 was turned on at some point in the day

    trap_id = captures[0]['trap_id']
    date = com.make_date(captures[0]['timestamp_start'])

    # Sum all of the mosquitoes captured throughout the day and check to see whether counter and CO2 were used
    for capture in captures:
        mos_count += int(capture['medium'])  # Mosquito counts are stored in the 'medium' field

        if not used_co2 and capture['co2_status']:
            used_co2 = True

        if not counter_on and capture['counter_status']:
            counter_on = True

    # Only write the collection if the counter was on
    if counter_on:
        if used_co2:
            attractant = 'carbon dioxide'
        else:
            attractant = ''

        year = date.year
        prefix = metadata['prefix']

        # If no ordinal exists for this year, make a new one
        if year not in metadata['ordinals']:
            metadata['ordinals'][year] = 0

        # Increment the ordinal and store it
        ordinal = metadata['ordinals'][year] = metadata['ordinals'][year] + 1

        # The ordinal string must have a leading zero, so we're giving it a length that probably won't
        # be exceeded for a year's worth of data. If it is exceeded, make sure it has at least
        # one leading zero and warn us that the ordinal is getting large
        digits = 8
        len_ordinal = len(str(ordinal))
        min_digits = len_ordinal + 1

        if min_digits > digits:
            digits = min_digits
            print('Warning: Large ordinal at trap_id: {} - year: {} - ordinal: {}'.format(trap_id, year, ordinal))

        ordinal_string = str(ordinal).zfill(digits)

        collection_id = '{}_{}_collection_{}'.format(prefix, year, ordinal_string)
        sample_id = '{}_{}_sample_{}'.format(prefix, year, ordinal_string)

        # Write the collection to file
        out_csv.writerow([collection_id, sample_id, date, date, trap_id,
                          collection['latitude'], collection['longitude'], '', 'BG-Counter trap catch', attractant,
                          1, 1, 'Culicidae', 'by size', 'adult',
                          'unknown sex', mos_count])

        return True

    else:
        # If the counter was never on, print a warning. If this is the case for
        # a decent number of days, it might be worth looking into
        print('Warning: Counter never on at date: {} - trap_id: {}'.format(date, trap_id))
        return False


# Gets metadata for the trapset containing the given trap from the database
# Note: Call as 'get_metadata(trap_id)'; 'cur' is added by the decorator
@com.run_with_connection
def get_metadata(cur, trap_id):
    # Get the prefix associated with the trap to check if the trap exists in the database
    sql = 'SELECT prefix FROM traps WHERE trap_id = %s'
    cur.execute(sql, (trap_id,))
    row = cur.fetchone()

    if not row:
        raise ValueError('No database entry for trap ID: ' + trap_id)

    prefix = row['prefix']
    metadata = {prefix: {'traps': {}, 'ordinals': {}}}

    # Get the locations associated with the traps
    sql = ('SELECT t.trap_id, latitude, longitude ' 
           'FROM traps as t LEFT OUTER JOIN locations as l ON t.trap_id = l.trap_id '
           'WHERE t.prefix = %s')
    cur.execute(sql, (prefix,))

    rows = cur.fetchall()
    for row in rows:
        trap_id = row['trap_id']

        if trap_id not in metadata[prefix]['traps']:
            metadata[prefix]['traps'][trap_id] = []

        if row['latitude'] and row['longitude']:
            metadata[prefix]['traps'][trap_id].append({'latitude': row['latitude'], 'longitude': row['longitude']})

    # Get the ordinals associated with the prefix
    sql = 'SELECT year, ordinal FROM ordinals WHERE prefix = %s'
    cur.execute(sql, (prefix,))

    metadata[prefix]['ordinals'] = {row['year']: row['ordinal'] for row in cur.fetchall()}

    return metadata


# Updates metadata in database
# Note: Call as 'update_metadata(metadata)'; 'cur' is added by the decorator
@com.run_with_connection
def update_metadata(cur, metadata):
    for prefix, trapset in metadata.items():
        # Update the ordinals associated with the prefix
        sql = ('INSERT INTO ordinals VALUES (%s, %s, %s) '
               'ON CONFLICT (prefix, year) DO UPDATE SET ordinal = EXCLUDED.ordinal')
        for year, ordinal in trapset['ordinals'].items():
            cur.execute(sql, (prefix, year, ordinal))

        for trap_id, locations in trapset['traps'].items():
            # Check the trap to make sure it still exists and has the same prefix
            sql = 'SELECT prefix FROM traps WHERE trap_id = %s'
            cur.execute(sql, (trap_id,))
            row = cur.fetchone()

            if not row:
                raise ValueError('Metadata update failed - trap no longer exists: ' + trap_id)
            elif row['prefix'] != prefix:
                raise ValueError('Metadata update failed - prefix has changed for trap: ' + trap_id)

            # Add new locations if there are any
            sql = 'INSERT INTO locations VALUES (%s, %s, %s) ON CONFLICT DO NOTHING'
            for location in locations:
                cur.execute(sql, (trap_id, location['latitude'], location['longitude']))


# Calculate the distance in kilometers between two sets of decimal coordinates
def calculate_distance(lat1, lon1, lat2, lon2):
    # Approximate radius of earth in km
    r = 6373.0

    arguments = (lat1, lon1, lat2, lon2)
    lat1, lon1, lat2, lon2 = map(math.radians, arguments)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    distance = r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return distance


# Handles CSV file operations
class CSVWriter:
    def __init__(self, filename):
        self.filename = filename
        self.file = open(filename, 'w')
        self.writer = csv.writer(self.file, lineterminator='\n')

        self.writer.writerow(['collection_ID', 'sample_ID', 'collection_start_date', 'collection_end_date', 'trap_ID',
                              'GPS_latitude', 'GPS_longitude', 'location_description', 'trap_type', 'attractant',
                              'trap_number', 'trap_duration', 'species', 'species_identification_method', 'developmental_stage',
                              'sex', 'sample_count'])

        self.empty_pos = self.file.tell()

    # Wrapper for the CSV writer object's writerow function
    def writerow(self, *args, **kwargs):
        self.writer.writerow(*args, **kwargs)

    # Closes the object's file and deletes the file if it has no data
    def close(self):
        if self.file.tell() == self.empty_pos:
            remove = True
        else:
            remove = False

        self.file.close()

        if remove:
            os.remove(self.filename)


if __name__ == '__main__':
    args = vars(parse_args())
    parse_json(**args)
