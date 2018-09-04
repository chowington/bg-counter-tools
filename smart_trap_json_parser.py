################
# Python 3.5.2 #
################

import argparse
import csv
import json
import math
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(description='Parses JSON delivered by the Biogents smart trap API '
        'and creates Darwin Core formatted files from the data.')

    parser.add_argument('file', nargs='+', help='The JSON file(s) to parse.')

    args = parser.parse_args()

    return args


def main():
    args = parse_args()

    files = args.file
    events_filename = 'sampling_events.csv'
    occurrences_filename = 'associated_occurrences.csv'
    
    with open(events_filename, 'w') as events_file, open(occurrences_filename, 'w') as occurrences_file:
        events_csv = csv.writer(events_file)
        occurrences_csv = csv.writer(occurrences_file)

        # Write the header rows
        events_csv.writerow(['eventID', 'samplingProtocol', 'samplingEffort', 'sampleSizeValue', 'sampleSizeUnit',
                             'eventDate', 'locationID', 'decimalLatitude', 'decimalLongitude', 'geodeticDatum'])
        occurrences_csv.writerow(['eventID', 'occurrenceID', 'basisOfRecord', 'individualCount', 'organismQuantity',
                                  'organismQuantityType', 'occurrenceStatus', 'scientificName', 'identificationQualifier', 'kingdom',
                                  'phylum', 'class', 'order', 'family', 'taxonRank'])
    
        for filename in files:
            with open(filename, 'r') as json_f:
                js = json.load(json_f)

                print("Processing file " + filename)

                for trap_wrapper in js['traps']:
                    trap_id = trap_wrapper['Trap']['id']
                    captures = trap_wrapper['Capture']
                    metadata = get_metadata(trap_id)

                    if len(captures) != 0:
                        total_captures, good_captures = process_captures(captures, metadata, events_csv, occurrences_csv)

                        print('Trap {}: Total captures: {} - Good captures: {} ({}%)'
                              .format(trap_id, total_captures, good_captures, math.floor((good_captures / total_captures) * 100)))

                    # Warn if a trap is showing no captures. We should reasonably expect data from each trap,
                    # and if we aren't getting any, it might be worth looking into
                    else:
                        print('Warning: 0 captures at trap_id: ' + trap_id)

                    write_metadata(trap_id, metadata)


# Takes a set of captures from a single trap and bins them into days
# before sending them off to be collected and written to file
def process_captures(captures, metadata, events_csv, occurrences_csv):
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

            # Get the locations for the current trap.
            # get_metadata() guarantees that the trap exists within metadata
            for trap in metadata['traps']:
                if trap['id'] == trap_id:
                    locations = trap['locations']
                    break

            # Try to make a collection from this set of captures
            collection = make_collection(day_captures, locations)

            # If a collection could be made, write it to file and count its captures as good
            if collection and write_collection(collection, metadata, events_csv, occurrences_csv):
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
            location_id = '{}-{}'.format(capture['trap_id'], len(locations))

            locations.append({
                'location_id': location_id,
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
def write_collection(collection, metadata, events_csv, occurrences_csv):
    captures = collection['captures']
    mos_count = 0
    counter_on = False  # Stores whether the counter was turned on at some point in the day

    trap_id = captures[0]['trap_id']
    date = make_date(captures[0]['timestamp_start'])

    # Sum all of the mosquitoes captured throughout the day and check to see whether counter and CO2 were used
    for capture in captures:
        mos_count += int(capture['medium'])  # Mosquito counts are stored in the 'medium' field

        if not counter_on and capture['counter_status']:
            counter_on = True

    # Only write the collection if the counter was on
    if counter_on:
        if mos_count:
            occurrence_status = 'present'
        else:
            occurrence_status = 'absent'

        year_string = str(date.year)
        prefix = metadata['prefix']

        # If no ordinal exists for this year, make a new one
        if year_string not in metadata['ordinals']:
            metadata['ordinals'][year_string] = 0

        # Increment the ordinal and store it
        ordinal = metadata['ordinals'][year_string] = metadata['ordinals'][year_string] + 1

        # The ordinal string must have a leading zero, so we're giving it a length that probably won't
        # be exceeded for a year's worth of data. If it is exceeded, make sure it has at least
        # one leading zero and warn us that the ordinal is getting large
        digits = 8
        len_ordinal = len(str(ordinal))
        min_digits = len_ordinal + 1

        if min_digits > digits:
            digits = min_digits
            print('Warning: Large ordinal at trap_id: {} - year: {} - ordinal: {}'.format(trap_id, year_string, ordinal))

        ordinal_string = str(ordinal).zfill(digits)

        event_id = '{}_{}_event_{}'.format(prefix, year_string, ordinal_string)
        occurrence_id = '{}_{}_occurrence_{}'.format(prefix, year_string, ordinal_string)

        # Write the collection to file
        events_csv.writerow([event_id, 'BG Counter', '1 trap-day', 1, 'day',
                             date, collection['location_id'], collection['latitude'], collection['longitude'], 'WGS84'])
        occurrences_csv.writerow([event_id, occurrence_id, 'MachineObservation', mos_count, mos_count,
                                 'individuals', occurrence_status, 'Culicidae', 'Machine determination based on size', 'Animalia',
                                 'Arthropoda', 'Insecta', 'Diptera', 'Culicidae', 'family'])

        return True

    else:
        # If the counter was never on, print a warning. If this is the case for
        # a decent number of days, it might be worth looking into
        print('Warning: Counter never on at date: {} - trap_id: {}'.format(date, trap_id))
        return False


# Get the metadata for a given trap_id
def get_metadata(trap_id):
    with open('smart-trap-metadata.json', 'r') as f:
        js = json.load(f)

        for trapset in js.values():
            for trap in trapset['traps']:
                if trap['id'] == trap_id:
                    return trapset

    raise ValueError('No metadata for trap ID: ' + trap_id)


# Write the updated metadata for a given trap_id
def write_metadata(trap_id, metadata):
    with open('smart-trap-metadata.json', 'r+') as f:
        js = json.load(f)

        for api_key in js:
            trap_ids = [trap['id'] for trap in js[api_key]['traps']]

            if trap_id in trap_ids:
                js[api_key] = metadata
                break
        else:
            raise ValueError('No metadata for trap ID: ' + trap_id)

        f.seek(0)
        f.truncate()
        json.dump(js, f, indent=4)


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
