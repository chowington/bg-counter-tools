################
# Python 3.5.2 #
################

import sys
import csv
import json
from datetime import datetime

def main():
    files = sys.argv[1:]
    
    out_name = 'interchange.out'
    
    with open(out_name, 'w') as out_f:
        out_csv = csv.writer(out_f)
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
                    captures = trap_wrapper['Capture']
    
                    if len(captures) == 0:
                        print('Warning: 0 captures at file: {} - trap_id: {}'.format(filename, trap['id']))
                        continue
    
                    else:
                        total_captures += len(captures)
                        day_captures = []
                        date = captures[0]['timestamp_start'][:10]
    
                        for capture in captures:
                            curr_date = capture['timestamp_start'][:10]
    
                            if not curr_date == date:
                                good_captures += write_collections(day_captures, out_csv)
                                day_captures = []
                                date = curr_date

                            day_captures.append(capture)
    
                print('End of file: {} - Total captures: {} - Good captures: {} - Tossed captures: {}'
                      .format(filename, total_captures, good_captures, total_captures - good_captures))


def write_collections(captures, csv):
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

            collection_ID = '{}_{}_collection_{}'.format('VB', trap_id, date)
            sample_ID = '{}_{}_sample_{}'.format('VB', trap_id, date)

            csv.writerow([collection_ID, sample_ID, date, date, trap_id,
                          collection[0]['trap_latitude'], collection[0]['trap_longitude'], '', 'BG-COUNTER trap catch', attractant,
                          1, 1, 'Culicidae', 'by size', 'adult',
                          'unknown sex', mos_count])

            good_captures += len(collection)

    return good_captures

main()

