################
# Python 3.5.2 #
################

import argparse
import csv
import json
import math
from datetime import datetime

from bg_common import run_with_connection


def parse_args():
    parser = argparse.ArgumentParser(description='Parses JSON delivered by the Biogents smart trap API '
        'and creates an interchange format file from the data.')

    parser.add_argument('file', nargs='+', help='The JSON file(s) to parse.')
    parser.add_argument('-o', '--output', default='interchange.out', help='The name of the output file.')
    parser.add_argument('--preserve-metadata', action='store_true',
        help='Don\'t change the metadata file in any way, neglecting to update any ordinals or locations. '
        'Note: This feature currently isn\'t perfect, as it can also affect the CSV output.')

    args = parser.parse_args()

    return args


def main():
    args = parse_args()

    files = args.file
    out_name = args.output
    
    with open(out_name, 'w') as out_file:
        out_csv = csv.writer(out_file)

        # Write the header row
        out_csv.writerow(['collection_ID', 'sample_ID', 'collection_start_date', 'collection_end_date', 'trap_ID',
                          'GPS_latitude', 'GPS_longitude', 'location_description', 'trap_type', 'attractant',
                          'trap_number', 'trap_duration', 'species', 'species_identification_method', 'developmental_stage',
                          'sex', 'sample_count'])
    
        for filename in files:
            with open(filename, 'r') as json_f:
                js = json.load(json_f)

                print("Processing file " + filename)

                for trap_wrapper in js['traps']:
                    trap_id = trap_wrapper['Trap']['id']
                    captures = trap_wrapper['Capture']
                    metadata = get_metadata(trap_id)

                    if len(captures) != 0:
                        total_captures, good_captures = process_captures(captures, metadata, out_csv)

                        print('Trap {}: Total captures: {} - Good captures: {} ({}%)'
                              .format(trap_id, total_captures, good_captures, math.floor((good_captures / total_captures) * 100)))

                    # Warn if a trap is showing no captures. We should reasonably expect data from each trap,
                    # and if we aren't getting any, it might be worth looking into
                    else:
                        print('Warning: 0 captures at trap_id: ' + trap_id)

                    if not args.preserve_metadata:
                        update_metadata(trap_id, metadata)


# Takes a set of captures from a single trap and bins them into days
# before sending them off to be collected and written to file
def process_captures(captures, metadata, out_csv):
    total_captures = 0  # The total number of unique captures (some are duplicates)
    good_captures = 0  # The number of captures that end up being collated into a collection
    day_captures = []  # The captures within a single day
    prev_end_timestamp = datetime.min  # Will hold the timestamp_end of the last capture
    num_captures = len(captures)

    for i in range(num_captures):
        capture = captures[i]
        curr_date = make_date(capture['timestamp_start'])

        # We use this to do some sanity checking.
        # The ending timestamp is more consistent than the starting one
        curr_end_timestamp = make_datetime(capture['timestamp_end'])

        # If this end timestamp is later than the previous one, store the capture.
        # We ignore the capture if it's identical to the previous one
        if curr_end_timestamp > prev_end_timestamp:
            day_captures.append({
                'trap_id': capture['trap_id'],
                'timestamp_start': capture['timestamp_start'],
                'co2_status': capture['co2_status'],
                'counter_status': capture['counter_status'],
                'medium': capture['medium'],
                'trap_latitude': capture['trap_latitude'],
                'trap_longitude': capture['trap_longitude'],
            })

            total_captures += 1
            prev_end_timestamp = curr_end_timestamp

        # Else if this timestamp is earlier than the previous one, error out.
        # We rely on the captures being delivered in forward chronological order
        elif curr_end_timestamp < prev_end_timestamp:
            raise ValueError('Capture has earlier ending timestamp than preceding capture. Capture ID: ' + capture['id'])

        # If we're at the last capture or the next capture is from a different day, end this day
        if i == num_captures - 1 or make_date(captures[i + 1]['timestamp_start']) != curr_date:
            trap_id = capture['trap_id']

            # Our current assumption is that there are no more than 96 unique captures
            # in a day (4 per hour) - if this changes, we'll need to edit this script
            if len(day_captures) > 96:
                raise ValueError('More than 96 captures in a day at trap_id: {} - date: {}'.format(trap_id, curr_date))

            # Try to make a collection from this set of captures
            collection = make_collection(day_captures, metadata['locations'])

            # If a collection could be made, write it to file and count its captures as good
            if collection and write_collection(collection, metadata, out_csv):
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

    # First, loop through the captures to pinpoint any possible new locations
    for capture in captures:
        curr_lat = float(capture['trap_latitude'])
        curr_lon = float(capture['trap_longitude'])

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

    # Loop backwards so we can remove items from location_groups
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
def write_collection(collection, metadata, csv):
    captures = collection['captures']
    mos_count = 0
    counter_on = False  # Stores whether the counter was turned on at some point in the day
    used_co2 = False  # Stores whether CO2 was turned on at some point in the day

    trap_id = captures[0]['trap_id']
    date = make_date(captures[0]['timestamp_start'])

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
        csv.writerow([collection_id, sample_id, date, date, trap_id,
                      collection['latitude'], collection['longitude'], '', 'BG-Counter trap catch', attractant,
                      1, 1, 'Culicidae', 'by size', 'adult',
                      'unknown sex', mos_count])

        return True

    else:
        # If the counter was never on, print a warning. If this is the case for
        # a decent number of days, it might be worth looking into
        print('Warning: Counter never on at date: {} - trap_id: {}'.format(date, trap_id))
        return False


# Gets metadata for a given trap_id from the database
# Note: Call as 'get_metadata(trap_id)'; 'cur' is added by the decorator
@run_with_connection
def get_metadata(cur, trap_id):
    metadata = {}

    # Get the prefix associated with the trap to check if the trap exists in the database
    sql = 'SELECT prefix FROM traps WHERE trap_id = %s'
    cur.execute(sql, (trap_id,))
    metadata = cur.fetchone()

    if not metadata:
        raise ValueError('No database entry for trap ID: ' + trap_id)

    # Get the locations associated with the trap
    sql = 'SELECT latitude, longitude FROM locations WHERE trap_id = %s'
    cur.execute(sql, (trap_id,))

    metadata['locations'] = cur.fetchall()

    # Get the ordinals associated with the prefix
    sql = 'SELECT year, ordinal FROM ordinals WHERE prefix = %s'
    cur.execute(sql, (metadata['prefix'],))

    metadata['ordinals'] = {row['year']: row['ordinal'] for row in cur.fetchall()}

    return metadata


# Updates metadata in database for a given trap_id
# Note: Call as 'update_metadata(trap_id, metadata)'; 'cur' is added by the decorator
@run_with_connection
def update_metadata(cur, trap_id, metadata):
    # Check the trap_id and prefix to make sure they still exist
    sql = 'SELECT prefix FROM traps WHERE trap_id = %s'
    cur.execute(sql, (trap_id,))
    row = cur.fetchone()

    if not row:
        raise ValueError('Metadata update failed - trap no longer exists: ' + trap_id)
    elif row['prefix'] != metadata['prefix']:
        raise ValueError('Metadata update failed - prefix has changed for trap: ' + trap_id)

    # Update the locations associated with the trap
    sql = 'INSERT INTO locations VALUES (%s, %s, %s) ON CONFLICT DO NOTHING'
    for location in metadata['locations']:
        cur.execute(sql, (trap_id, location['latitude'], location['longitude']))

    # Update the ordinals associated with the prefix
    sql = ('INSERT INTO ordinals VALUES (%s, %s, %s) '
           'ON CONFLICT (prefix, year) DO UPDATE SET ordinal = EXCLUDED.ordinal')
    for year, ordinal in metadata['ordinals'].items():
        cur.execute(sql, (metadata['prefix'], year, ordinal))


# Attempts to create a datetime object from a string
def make_datetime(string):
    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S')


# Attempts to create a date object from a string
def make_date(string):
    return make_datetime(string).date()


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


if __name__ == '__main__':
    main()
