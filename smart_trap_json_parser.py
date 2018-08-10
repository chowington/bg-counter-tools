################
# Python 3.5.2 #
################

import sys
import csv
import json
import pprint
from datetime import datetime

def main():
    files = sys.argv[1:]
    
    out_name = 'interchange.out'
    
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

                for trap_wrapper in js['traps']:
                    trap = trap_wrapper['Trap']
                    trap_id = trap['id']
                    captures = trap_wrapper['Capture']
                    metadata = get_metadata(trap_id)

                    if len(captures) != 0:
                        day_captures = []
                        prev_timestamp = None
                        date = captures[0]['timestamp_start'][:10]

                        for capture in captures:
                            curr_timestamp = capture['timestamp_start']
                            '''print([trap['id'], curr_timestamp, capture['latitude'], capture['longitude'], capture['isTrapExactLocation']])
                            print([trap['id'], curr_timestamp, capture['trap_latitude'], capture['trap_longitude']])
                            print()'''

                            if curr_timestamp != prev_timestamp:
                                total_captures += 1
                                prev_timestamp = curr_timestamp
                                curr_date = curr_timestamp[:10]

                                if not curr_date == date:
                                    good_captures += write_collections(day_captures, metadata, out_csv)
                                    day_captures = []
                                    date = curr_date

                                day_captures.append({
                                    'trap_id' : capture['trap_id'],
                                    'timestamp_start' : capture['timestamp_start'],
                                    'co2_status' : capture['co2_status'],
                                    'medium' : capture['medium'],
                                    'trap_latitude' : capture['trap_latitude'],
                                    'trap_longitude' : capture['trap_longitude'],
                                })

                    else:
                        print('Warning: 0 captures at file: {} - trap_id: {}'.format(filename, trap['id']))

                    write_metadata(trap_id, metadata)

                print('End of file: {} - Total captures: {} - Good captures: {} - Tossed captures: {}'
                      .format(filename, total_captures, good_captures, total_captures - good_captures))

def write_collections(captures, metadata, csv):
    collections = []
    good_captures = 0

    for capture in captures:
        binned = False

        for collection in collections:
            same_lat = abs(float(capture['trap_latitude']) - float(collection[0]['trap_latitude'])) < 0.001
            same_lon = abs(float(capture['trap_longitude']) - float(collection[0]['trap_longitude'])) < 0.001

            if same_lat and same_lon:
                collection.append(capture)
                binned = True
                break

        if not binned:
            collections.append([capture])

    for collection in collections:
        #print([collection[0]['trap_id'], collection[0]['timestamp_start'],
        #       collection[0]['trap_latitude'], collection[0]['trap_longitude'], len(collection)])

        if len(collection) > 96:
            raise ValueError('More than 96 captures in a day.')

        if len(collection) >= 92:
            mos_count = 0
            used_co2 = False

            for capture in collection:
                mos_count += int(capture['medium'])

                if not used_co2 and capture['co2_status']:
                    used_co2 = True

            if used_co2:
                attractant = 'carbon dioxide'
            else:
                attractant = ''

            trap_id = collection[0]['trap_id']
            date = collection[0]['timestamp_start'][:10]
            year = date[:4]
            prefix = metadata['prefix']

            if year not in metadata['ordinals']:
                metadata['ordinals'][year] = 0

            ordinal = metadata['ordinals'][year] = metadata['ordinals'][year] + 1
            ordinal_string = str(ordinal).zfill(8)

            collection_ID = '{}_{}_collection_{}'.format(prefix, year, ordinal_string)
            sample_ID = '{}_{}_sample_{}'.format(prefix, year, ordinal_string)

            csv.writerow([collection_ID, sample_ID, date, date, trap_id,
                          collection[0]['trap_latitude'], collection[0]['trap_longitude'], '', 'BG-COUNTER trap catch', attractant,
                          1, 1, 'Culicidae', 'by size', 'adult',
                          'unknown sex', mos_count])

            good_captures += len(collection)

    return good_captures

def get_metadata(trap_id):
    with open('smart-trap-metadata.json', 'r') as f:
        js = json.load(f)

        metadata = None

        for trapset in js.values():
            if trap_id in trapset['traps']:
                metadata = trapset['metadata']
                break

        if not metadata:
            raise ValueError('No metadata for trap ID: ' + trap_id)

        return metadata

def write_metadata(trap_id, metadata):
    with open('smart-trap-metadata.json', 'r+') as f:
        js = json.load(f)

        for api_key in js:
            if trap_id in js[api_key]['traps']:
                js[api_key]['metadata'] = metadata
                break

        f.seek(0)
        f.truncate()
        json.dump(js, f, indent=4)

main()

