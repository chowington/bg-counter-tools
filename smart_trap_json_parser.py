################
# Python 3.5.2 #
################

import sys
import csv
import json
import math
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Parses JSON delivered by the Biogents smart trap API '
        'and creates an interchange format file from the data.')

    parser.add_argument('file', nargs='+', help='The JSON file(s) to parse.')
    parser.add_argument('-o', '--output', default='interchange.out', help='The name of the output file.')
    parser.add_argument('--preserve-metadata', action='store_true',
        help='Don\'t change the metadata file in any way, neglecting to update any ordinals or locations.')

    args = parser.parse_args()

    return args

def main():
    args = parse_args()

    files = args.file
    out_name = args.output
    
    with open(out_name, 'w') as out_file:
        out_csv = csv.writer(out_file)
        out_csv.writerow(['collection_ID', 'sample_ID', 'collection_start_date', 'collection_end_date', 'trap_ID',
                          'GPS_latitude', 'GPS_longitude', 'location_description', 'trap_type', 'attractant',
                          'trap_number', 'trap_duration', 'species', 'species_identification_method', 'developmental_stage',
                          'sex', 'sample_count'])
    
        for filename in files:
            with open(filename, 'r') as json_f:
                js = json.load(json_f)
                total_captures = 0
                good_captures = 0

                print("Processing file " + filename)

                for trap_wrapper in js['traps']:
                    trap = trap_wrapper['Trap']
                    trap_id = trap['id']
                    captures = trap_wrapper['Capture']
                    metadata = get_metadata(trap_id)

                    if len(captures) != 0:
                        total_count, good_count = process_captures(captures, metadata, out_csv)
                        total_captures += total_count
                        good_captures += good_count
                    else:
                        print('Warning: 0 captures at trap_id: ' + trap['id'])

                    if not args.preserve_metadata:
                        write_metadata(trap_id, metadata)

                print('End of file. Total captures: {} - Good captures: {} - Tossed captures: {}'
                      .format(total_captures, good_captures, total_captures - good_captures))

def process_captures(captures, metadata, out_csv):
    total_captures = 0
    good_captures = 0
    day_captures = []
    prev_end_timestamp = None
    num_captures = len(captures)

    for i in range(num_captures):
        capture = captures[i]
        curr_end_timestamp = capture['timestamp_end']
        curr_date = capture['timestamp_start'][:10]

        if curr_end_timestamp != prev_end_timestamp:
            day_captures.append({
                'trap_id' : capture['trap_id'],
                'timestamp_start' : capture['timestamp_start'],
                'co2_status' : capture['co2_status'],
                'medium' : capture['medium'],
                'trap_latitude' : capture['trap_latitude'],
                'trap_longitude' : capture['trap_longitude'],
            })

            total_captures += 1
            prev_end_timestamp = curr_end_timestamp

        if i == num_captures - 1 or captures[i + 1]['timestamp_start'][:10] != curr_date:
            if len(day_captures) > 96:
                raise ValueError('More than 96 captures in a day at trap_id: {} - date: {}'.format(capture['trap_id'], curr_date))

            collection = make_collection(day_captures, metadata)

            if collection:
                write_collection(collection, metadata, out_csv)
                good_captures += len(collection['captures'])

            day_captures = []

    return total_captures, good_captures

def make_collection(captures, metadata):
    location_groups = metadata['locations']
    collection = None

    for location_group in location_groups:
        location_group['captures'] = []
        location_group['new'] = False

    for capture in captures:
        curr_lat = float(capture['trap_latitude'])
        curr_lon = float(capture['trap_longitude'])

        for location_group in location_groups:
            distance = calculate_distance(curr_lat, curr_lon, location_group['latitude'], location_group['longitude'])

            if distance < 0.111:
                location_group['captures'].append(capture)
                break
        else:
            location_groups.append({
                'latitude': curr_lat,
                'longitude': curr_lon,
                'captures': [capture],
                'new': True
            })

    for i in range(len(location_groups) - 1, -1, -1):
        location_group = location_groups[i]
        num_captures = len(location_group['captures'])

        if num_captures >= 92:
            collection = dict(location_group)

            del location_group['captures']
            del location_group['new']

        else:
            if location_group['new']:
                del location_groups[i]
            else:
                del location_group['captures']
                del location_group['new']

    return collection

def write_collection(collection, metadata, csv):
    captures = collection['captures']
    mos_count = 0
    used_co2 = False

    for capture in captures:
        mos_count += int(capture['medium'])

        if not used_co2 and capture['co2_status']:
            used_co2 = True

    if used_co2:
        attractant = 'carbon dioxide'
    else:
        attractant = ''

    trap_id = captures[0]['trap_id']
    date = captures[0]['timestamp_start'][:10]
    year = date[:4]
    prefix = metadata['prefix']

    if year not in metadata['ordinals']:
        metadata['ordinals'][year] = 0

    ordinal = metadata['ordinals'][year] = metadata['ordinals'][year] + 1
    ordinal_string = str(ordinal).zfill(8)

    collection_ID = '{}_{}_collection_{}'.format(prefix, year, ordinal_string)
    sample_ID = '{}_{}_sample_{}'.format(prefix, year, ordinal_string)

    csv.writerow([collection_ID, sample_ID, date, date, trap_id,
                  collection['latitude'], collection['longitude'], '', 'BG-Counter trap catch', attractant,
                  1, 1, 'Culicidae', 'by size', 'adult',
                  'unknown sex', mos_count])

def get_metadata(trap_id):
    with open('smart-trap-metadata.json', 'r') as f:
        js = json.load(f)

        for trapset in js.values():
            if trap_id in trapset['traps']:
                return trapset

        raise ValueError('No metadata for trap ID: ' + trap_id)

def write_metadata(trap_id, metadata):
    with open('smart-trap-metadata.json', 'r+') as f:
        js = json.load(f)

        for api_key in js:
            if trap_id in js[api_key]['traps']:
                js[api_key] = metadata
                break
        else:
            raise ValueError('No metadata for trap ID: ' + trap_id)

        f.seek(0)
        f.truncate()
        json.dump(js, f, indent=4)

def calculate_distance(lat1, lon1, lat2, lon2):
    # Approximate radius of earth in kilometers
    R = 6373.0

    arguments = (lat1, lon1, lat2, lon2)
    lat1, lon1, lat2, lon2 = map(math.radians, arguments)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    distance = R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return distance

if __name__ == '__main__':
    main()

