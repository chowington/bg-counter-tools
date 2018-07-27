################
# Python 3.5.2 #
################

import sys
import csv
import json
from datetime import datetime
from collections import defaultdict

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
            tossed_captures = 0

            for trap_wrapper in js['traps']:
                trap = trap_wrapper['Trap']
                bins = defaultdict(lambda: defaultdict(int))

                captures = trap_wrapper['Capture']

                if len(captures) == 0: # Maybe don't need error here - warning?
                    print('ERROR: 0 captures at file: {} - trap_id: {}'.format(filename, trap['id']))
                    exit(1)

                else:
                    date = captures[0]['timestamp_start'][:10]

                    '''if not captures[0]['isGPSDecimal']:    # Only accept decimal until we know what the degrees/minutes form looks like
                        print('ERROR: GPS coordinates not in decimal format at file: {} - trap_id: {} - id: {} - GPS: {}'
                              .format(filename, captures[0]['trap_id'], captures[0]['id'], captures[0]['trap_latitude']))
                        exit(1)

                    else:'''
                    lat = float(captures[0]['trap_latitude'])
                    lon = float(captures[0]['trap_longitude'])

                    mos_count = 0
                    capture_count = 0
                    used_co2 = False

                    for capture in captures:
                        curr_date = capture['timestamp_start'][:10]

                        '''if capture['isGPSDecimal']:    # Only accept decimal until we know what the degrees/minutes form looks like
                            print('WARNING: GPS coordinates not in decimal format at file: {} - trap_id: {} - capture_id: {} - GPS: {}'
                                  .format(filename, capture['trap_id'], capture['id'], capture['trap_latitude']))
                            continue

                        else:'''
                        curr_lat = float(capture['trap_latitude'])
                        curr_lon = float(capture['trap_longitude'])

                        same_date = (curr_date == date)
                        same_lat = abs(curr_lat - lat) < 0.001
                        same_lon = abs(curr_lon - lon) < 0.001

                        if not (same_date and same_lat and same_lon):
                            if capture_count >= 92:  # If no more than an hour is missing
                                collection_ID = '{}_{}_collection_{}'.format('VB', trap['id'], date)
                                sample_ID = '{}_{}_sample_{}'.format('VB', trap['id'], date)

                                if used_co2:
                                    attractant = 'carbon dioxide'
                                else:
                                    attractant = ''

                                out_csv.writerow([collection_ID, sample_ID, date, date, trap['id'],
                                                  lat, lon, '', 'BG-COUNTER trap catch', attractant,
                                                  1, 1, 'Culicidae', 'by size', 'adult',
                                                  'unknown sex', mos_count])

                            else:
                                print('Notice: Incomplete collection at file: {} - trap id: {} - date: {} - capture_id: {} - captures: {}'
                                      .format(filename, trap['id'], date, capture['id'], capture_count))
                                tossed_captures += capture_count

                            total_captures += capture_count

                            date = curr_date
                            lat = curr_lat
                            lon = curr_lon
                            mos_count = 0
                            capture_count = 0
                            used_co2 = False

                        capture_count += 1
                        mos_count += int(capture['medium'])

                        if not used_co2 and capture['co2_status']:
                            used_co2 = True

                    if capture_count >= 92:  # If no more than an hour is missing
                        collection_ID = '{}_{}_collection_{}'.format('VB', trap['id'], date)
                        sample_ID = '{}_{}_sample_{}'.format('VB', trap['id'], date)

                        if used_co2:
                            attractant = 'carbon dioxide'
                        else:
                            attractant = ''

                        out_csv.writerow([collection_ID, sample_ID, date, date, trap['id'],
                                          lat, lon, '', 'BG-COUNTER trap catch', attractant,
                                          1, 1, 'Culicidae', 'by size', 'adult',
                                          'unknown sex', mos_count])

                    else:
                        print('Notice: Incomplete collection at file: {} - trap id: {} - date: {} - capture_id: {} - captures: {}'
                              .format(filename, trap['id'], date, capture['id'], capture_count))
                        tossed_captures += capture_count


            print('End of file: {} - Total captures: {} - Good captures: {} - Tossed captures: {}'
                  .format(filename, total_captures, total_captures - tossed_captures, tossed_captures))

