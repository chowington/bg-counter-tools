"""
Aggregates and converts smart trap JSON files into interchange files.

This script takes a set of smart trap JSON data files and aggregates
their data into days before writing the data to interchange-format
files.  The script can also automatically generate project files
necessary for the full BG-Counter Tools pipeline, splitting data into
years along the way, using the --split-years option.

For usage information, run with -h.

This script requires at least Python 3.5.
"""

import argparse
import csv
import json
import math
import os
import random
import datetime as dt
from string import Template

import bg_common as com


def parse_args():
    """Parse the command line arguments and return an args namespace."""
    parser = argparse.ArgumentParser(description='Parses JSON delivered by the Biogents smart trap'
                                                 ' API and creates an interchange format file '
                                                 'from the data.')

    parser.add_argument('files', nargs='+', metavar='file', help='The JSON file(s) to parse.')
    parser.add_argument('--preserve-metadata', action='store_true',
                        help="Don't change the metadata in the database in any way")

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument('-o', '--output', help='The name of the output file.')
    output_group.add_argument('-y', '--split-years', action='store_true',
                              help='Split data from different years into separate output files. '
                                   'The files will be named [prefix]_[year].pop.')

    args = parser.parse_args()

    return args


def parse_json(files, output='interchange.pop', split_years=False, preserve_metadata=False):
    """Parse JSON files and create interchange format files from them.

    Required arguments:
    files -- A list of filenames to parse.

    Optional arguments:
    output -- The name of the output file.  Ignored if split_years is
        True.
    split_years -- A boolean signalling whether to split data from
        different years into separate output files.  The files will be
        named [prefix]_[year].pop.  Also creates project files necessary
        for the rest of the BG-Counter Tools pipeline.
    preserve_metadata -- A boolean signalling whether to preserve the
        metadata within the database, skipping all database update
        operations.
    """
    random.seed()
    out_csv = None
    metadata = {}

    try:
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
                            new_metadata = get_trap_metadata(trap_id=trap_id)
                            metadata.update(new_metadata)
                            curr_prefix = list(new_metadata.keys())[0]
                            curr_trapset = new_metadata[curr_prefix]

                        trap_metadata = {
                            'prefix': curr_prefix,
                            'locations': curr_trapset['traps'][trap_id],
                            'ordinals': curr_trapset['ordinals'],
                            'obfuscate': curr_trapset['obfuscate']
                        }

                        # Process captures.
                        total_captures, good_captures = process_captures(captures, trap_metadata,
                                                                         out_csv)

                        # Update master metadata.
                        curr_trapset['traps'][trap_id] = trap_metadata['locations']
                        curr_trapset['ordinals'] = trap_metadata['ordinals']

                        print('Trap {}: Total captures: {} - Good captures: {} ({}%)'
                              .format(trap_id, total_captures, good_captures,
                                      math.floor((good_captures/total_captures) * 100)))

                    # Warn if a trap is showing no captures.
                    # We should reasonably expect data from each trap,
                    # and if we aren't getting any, it might be
                    # worth looking into.
                    else:
                        print('Warning: 0 captures at trap_id: ' + trap_id)

    finally:
        # Close all output files.
        if out_csv:
            if type(out_csv) is dict:
                projects = []

                for prefix, years in out_csv.items():
                    for year, csv_writer in years.items():
                        project_info = csv_writer.close()

                        # Store the info for the valid projects
                        # that were created.
                        if project_info:
                            projects.append(project_info)
            else:
                projects = None
                out_csv.close()

            if not preserve_metadata:
                update_metadata(metadata=metadata)

            return projects


def process_captures(captures, metadata, out_csv):
    """Bin captures into days, then write them to file.

    Arguments:
    captures -- A dict containing the captures to process.
    metadata -- A dict containing the metadata for the trap and provider
        that the captures originate from.
    out_csv -- A CSV writer object to write the binned captures to.
    """
    # The total number of unique captures (some are duplicates).
    total_captures = 0

    # The number of captures that end up being collated.
    # into a collection.
    good_captures = 0

    # The captures within a single day.
    day_captures = []

    # Will hold the timestamp_end of the last capture.
    prev_end_timestamp = dt.datetime.min

    num_captures = len(captures)

    for i in range(num_captures):
        capture = captures[i]

        # We use this to do some sanity checking.
        # The ending timestamp is more consistent than the starting one.
        curr_end_timestamp = com.make_datetime(capture['timestamp_end'])
        curr_start_timestamp = com.make_datetime(capture['timestamp_start'])
        curr_date = curr_start_timestamp.date()

        valid_dates = curr_end_timestamp and curr_start_timestamp

        # Count a capture if it's not a duplicate
        # or it has invalid dates.
        if not valid_dates or curr_end_timestamp != prev_end_timestamp:
            total_captures += 1

        if valid_dates:
            # If this end timestamp is later than the previous one,
            # store the capture.  We ignore the capture if it's
            # identical to the previous one or if its timeframe is
            # much less than 15 minutes.
            if (curr_end_timestamp > prev_end_timestamp
                    and curr_end_timestamp - curr_start_timestamp >= dt.timedelta(minutes=12)):
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

            # Else if this timestamp is earlier than the previous one,
            # error out.  We rely on the captures being delivered in
            # forward chronological order.
            elif curr_end_timestamp < prev_end_timestamp:
                raise ValueError('Capture has earlier ending timestamp than preceding capture. '
                                 'Capture ID: ' + capture['id'])

            # If we're at the last capture or the next capture is
            # from a different day, end this day.
            if (i == num_captures - 1
                    or com.make_date(captures[i + 1]['timestamp_start']) != curr_date):
                trap_id = capture['trap_id']

                # Our current assumption is that there are no more
                # than 96 unique captures in a day (4 per hour).
                # If this changes, we'll need to edit this script.
                if len(day_captures) > 96:
                    raise ValueError('More than 96 captures in a day at trap_id: {} - date: {}'
                                     .format(trap_id, curr_date))

                # Try to make a collection from this set of captures.
                collection = make_collection(day_captures, metadata['locations'],
                                             metadata['obfuscate'])

                # Get the correct output file.
                if isinstance(out_csv, dict):
                    prefix = metadata['prefix']
                    year = curr_date.year

                    # If the correct output file doesn't exist, make it.
                    if prefix not in out_csv:
                        out_csv[prefix] = {}

                    if year not in out_csv[prefix]:
                        out_csv[prefix][year] = ProjectFileManager(prefix, year)

                    curr_csv = out_csv[prefix][year]

                else:
                    curr_csv = out_csv

                # If a collection could be made, write it to file
                # and count its captures as good.
                if collection and write_collection(collection, metadata, curr_csv):
                    good_captures += len(collection['captures'])

                    if isinstance(curr_csv, ProjectFileManager):
                        curr_csv.update_dates(curr_date)

                day_captures = []

    return total_captures, good_captures


def make_collection(captures, locations, obfuscate):
    """Bin captures into collections based on location.

    Takes a set of captures within the same day, bins them based on
    location, and returns the collection that is big enough, returning
    None if there is none.  Adds new locations to the metadata dict if
    there are any.

    Arguments:
    captures -- A dict containing the captures to process.
    locations -- A dict containing the locations that have been recorded
        previously for the trap that the captures came from.
    obfuscate -- A boolean determining whether to obfuscate the new
        locations before adding them to the metadata.
    """
    # Add a new key that will hold the captures that map
    # to each location and a key that will let us know that these
    # locations are not new (used later).
    for location in locations:
        location['captures'] = []
        location['new'] = False

    # First, loop through the captures to pinpoint any possible
    # new locations.  We're looping backwards so we can delete captures
    # if necessary.
    for i in range(len(captures) - 1, -1, -1):
        capture = captures[i]
        curr_lat = float(capture['trap_latitude'])
        curr_lon = float(capture['trap_longitude'])

        # If a trap can't get correct GPS data, it will either report
        # a coordinate that is exactly 0 or report its location as
        # (51.4778, 0.0014), which is in Greenwich near the prime
        # meridian.  Either way, drop the capture.
        if curr_lat == 0 or curr_lon == 0 or (curr_lat == 51.4778 and curr_lon == 0.0014):
            del captures[i]

        else:
            # Determine whether this capture is close to
            # any existing locations.
            for location in locations:
                distance = calculate_distance(curr_lat, curr_lon, location['true_latitude'],
                                              location['true_longitude'])

                # 111 meters - arbitrary, but shouldn't be too small.
                if distance < 111:
                    break

            # If it's not close to any known location, add
            # a new location at its coordinates.  This bubbles up
            # to the metadata dict as well.
            else:
                locations.append({
                    'true_latitude': curr_lat,
                    'true_longitude': curr_lon,
                    'captures': [],
                    'new': True
                })

    # Next, loop through the captures and assign them
    # to the closest locations.
    for capture in captures:
        curr_lat = float(capture['trap_latitude'])
        curr_lon = float(capture['trap_longitude'])

        closest_location = None
        closest_distance = math.inf

        for location in locations:
            distance = calculate_distance(curr_lat, curr_lon,
                                          location['true_latitude'], location['true_longitude'])

            if distance < closest_distance:
                closest_location = location
                closest_distance = distance

        # Because of the previous loop, each capture will be
        # within a reasonable distance of some location.
        closest_location['captures'].append(capture)

    # This will hold the final collection if there is one.
    collection = None

    # Once again, loop backwards so we can remove items.
    for i in range(len(locations) - 1, -1, -1):
        location = locations[i]
        num_captures = len(location['captures'])

        # Allow at most one cumulative hour of missing data in a day.
        if num_captures >= 92:
            # If the location is new, average its captures' coordinates
            # to get a more accurate lat/lon, then obfuscate
            # if necessary.
            if location['new']:
                lats, lons = [], []

                for capture in location['captures']:
                    lats.append(float(capture['trap_latitude']))
                    lons.append(float(capture['trap_longitude']))

                location['true_latitude'] = round(sum(lats) / len(lats), 6)
                location['true_longitude'] = round(sum(lons) / len(lons), 6)

                if obfuscate:
                    new_lat, new_lon = obfuscate_coordinates(location['true_latitude'],
                                                             location['true_longitude'], 200, 400)
                    location['offset_latitude'] = round(new_lat, 6)
                    location['offset_longitude'] = round(new_lon, 6)
                else:
                    location['offset_latitude'] = location['true_latitude']
                    location['offset_longitude'] = location['true_longitude']

            collection = dict(location)

        # If there weren't enough captures for a full collection
        # and the location was new, remove it so it doesn't get
        # added to the metadata.
        elif location['new']:
            del locations[i]

        # Remove 'captures' and 'new'.
        del location['captures']
        del location['new']

    return collection


def write_collection(collection, metadata, out_csv):
    """Write a collection of captures to file.

    Take a collection containing a day's worth of captures and
    aggregate and write it to file if the counter was on at some point
    during the day.  Return True if the collection was written
    and False if it wasn't.

    Arguments:
    collection -- A dict containing the collection to write to file.
    metadata -- A dict containing the metadata for the trap and provider
        that the captures originate from.
    out_csv -- A CSV writer object to write the collection to.
    """
    captures = collection['captures']
    mos_count = 0

    # Stores whether the counter was turned on at some point in the day.
    counter_on = False

    # Stores whether CO2 was turned on at some point in the day.
    used_co2 = False

    trap_id = captures[0]['trap_id']
    date = com.make_date(captures[0]['timestamp_start'])

    # Sum all of the mosquitoes captured throughout the day
    # and check to see whether counter and CO2 were used.
    for capture in captures:
        # Mosquito counts are stored in the 'medium' field.
        mos_count += int(capture['medium'])

        if not used_co2 and capture['co2_status']:
            used_co2 = True

        if not counter_on and capture['counter_status']:
            counter_on = True

    # Only write the collection if the counter was on.
    if counter_on:
        if used_co2:
            attractant = 'carbon dioxide'
        else:
            attractant = ''

        year = date.year
        prefix = metadata['prefix']

        # If no ordinal exists for this year, make a new one.
        if year not in metadata['ordinals']:
            metadata['ordinals'][year] = 0

        # Increment the ordinal and store it.
        ordinal = metadata['ordinals'][year] = metadata['ordinals'][year] + 1

        # The ordinal string must have a leading zero, so we're giving
        # it a length that probably won't be exceeded for a year's worth
        # of data.  If it is exceeded, make sure it has at least one
        # leading zero and warn us that the ordinal is getting large.
        digits = 8
        len_ordinal = len(str(ordinal))
        min_digits = len_ordinal + 1

        if min_digits > digits:
            digits = min_digits
            print('Warning: Large ordinal at trap_id: {} - year: {} - ordinal: {}'
                  .format(trap_id, year, ordinal))

        ordinal_string = str(ordinal).zfill(digits)

        collection_id = '{}_{}_collection_{}'.format(prefix, year, ordinal_string)
        sample_id = '{}_{}_sample_{}'.format(prefix, year, ordinal_string)

        # Write the collection to file.
        out_csv.writerow([collection_id, sample_id, date, date, trap_id,
                          str(collection['offset_latitude']).zfill(6),
                          str(collection['offset_longitude']).zfill(6),
                          '', 'BGCT', attractant, 1, 1, 'Culicidae', 'by size', 'adult',
                          'unknown sex', mos_count])

        return True

    else:
        # If the counter was never on, print a warning.  If this is
        # the case for a decent number of days, it might be worth
        # looking into.
        print('Warning: Counter never on at date: {} - trap_id: {}'.format(date, trap_id))
        return False


@com.run_with_connection
def get_trap_metadata(cur, trap_id):
    """Get metadata for the trapset containing the given trap.

    trap_id -- A string representing a BG-Counter trap ID.

    Note: Omit the 'cur' argument when calling and provide other
    arguments as keyword args.
    """
    # Get the prefix associated with the trap
    # to check if the trap exists in the database.
    sql = ('SELECT p.prefix, p.obfuscate FROM traps as t, providers as p '
           'WHERE t.prefix = p.prefix AND t.trap_id = %s')
    cur.execute(sql, (trap_id,))
    row = cur.fetchone()

    if not row:
        raise ValueError('No database entry for trap ID: ' + trap_id)

    prefix = row['prefix']
    metadata = {prefix: {'traps': {}, 'ordinals': {}, 'obfuscate': row['obfuscate']}}

    # Get the locations associated with the traps.
    sql = ('SELECT t.trap_id, true_latitude, true_longitude, offset_latitude, offset_longitude '
           'FROM traps as t LEFT OUTER JOIN locations as l ON t.trap_id = l.trap_id '
           'WHERE t.prefix = %s')
    cur.execute(sql, (prefix,))

    rows = cur.fetchall()
    for row in rows:
        trap_id = row['trap_id']

        if trap_id not in metadata[prefix]['traps']:
            metadata[prefix]['traps'][trap_id] = []

        if row['true_latitude'] and row['true_longitude']:
            metadata[prefix]['traps'][trap_id].append({
                'true_latitude': row['true_latitude'],
                'true_longitude': row['true_longitude'],
                'offset_latitude': row['offset_latitude'],
                'offset_longitude': row['offset_longitude'],
            })

    # Get the ordinals associated with the prefix.
    sql = 'SELECT year, ordinal FROM ordinals WHERE prefix = %s'
    cur.execute(sql, (prefix,))

    metadata[prefix]['ordinals'] = {row['year']: row['ordinal'] for row in cur.fetchall()}

    return metadata


@com.run_with_connection
def get_provider_metadata(cur, prefix):
    """Get metadata for a particular data provider.

    prefix -- A string corresponding to the prefix of the desired
        provider.

    Note: Omit the 'cur' argument when calling and provide other
    arguments as keyword args.
    """
    sql = ('SELECT org_name, org_email, org_url, contact_first_name, contact_last_name,'
           'contact_email, study_tag, study_tag_number '
           'FROM providers WHERE prefix = %s')
    cur.execute(sql, (prefix,))
    row = cur.fetchone()

    return row


@com.run_with_connection
def update_metadata(cur, metadata):
    """Update metadata in the database.

    metadata -- A dict containing the metadata to update the database
        with.  Can contain metadata on multiple providers.

    Note: Omit the 'cur' argument when calling and provide other
    arguments as keyword args.
    """
    for prefix, trapset in metadata.items():
        # Update the ordinals associated with the prefix.
        sql = ('INSERT INTO ordinals VALUES (%s, %s, %s) '
               'ON CONFLICT (prefix, year) DO UPDATE SET ordinal = EXCLUDED.ordinal')
        for year, ordinal in trapset['ordinals'].items():
            cur.execute(sql, (prefix, year, ordinal))

        for trap_id, locations in trapset['traps'].items():
            # Check the trap to make sure it still exists
            # and has the same prefix.
            sql = 'SELECT prefix FROM traps WHERE trap_id = %s'
            cur.execute(sql, (trap_id,))
            row = cur.fetchone()

            if not row:
                raise ValueError('Metadata update failed - trap no longer exists: ' + trap_id)
            elif row['prefix'] != prefix:
                raise ValueError('Metadata update failed - prefix has changed for trap: '
                                 + trap_id)

            # Add new locations if there are any.
            sql = 'INSERT INTO locations VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING'
            for location in locations:
                cur.execute(sql, (trap_id, location['true_latitude'], location['true_longitude'],
                                  location['offset_latitude'], location['offset_longitude']))


def calculate_distance(lat1, lon1, lat2, lon2):
    """Get distance in meters between two sets of decimal coordinates.

    Note that this code is based on the Haversine formula for spheres,
    giving it an error of up to about 0.5%.
    """
    # Approximate radius of earth in km.
    r = 6373.0

    # Convert the parameters to radians.
    arguments = (lat1, lon1, lat2, lon2)
    lat1, lon1, lat2, lon2 = map(math.radians, arguments)

    # Get their deltas.
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Calculate distance in meters.
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    distance = r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance *= 1000

    return distance


def obfuscate_coordinates(lat, lon, min_distance, max_distance):
    """Obfuscate a set of GPS coordinates.

    Obfuscates a set of GPS coordinates by translating the represented
    location in a random direction by a random distance between two
    bounds.

    Arguments:
    lat -- The decimal latitude.
    lon -- The decimal longitude.
    min_distance -- The minimum distance in meters by which to displace
        the point.
    max_distance -- The maximum distance in meters by which to displace
        the point.

    Formula source: http://www.edwilliams.org/avform.htm#LL
    """
    # Randomly choose a distance within the parameters.
    d_m = random.uniform(min_distance, max_distance)

    # Convert this distance to nautical miles, then to radians.
    d_nm = d_m / 1852
    d_r = (math.pi / (180*60)) * d_nm

    # Randomly choose a true course (direction) in radians.
    tc = random.uniform(0, 2 * math.pi)

    # Convert the lat/lon to radians.
    lat = math.radians(lat)
    lon = math.radians(lon)

    # Calculate new coordinates.
    new_lat = math.asin(math.sin(lat)*math.cos(d_r) + math.cos(lat)*math.sin(d_r)*math.cos(tc))
    dlon = math.atan2(math.sin(tc)*math.sin(d_r)*math.cos(lat),
                      math.cos(d_r) - math.sin(lat)*math.sin(new_lat))
    new_lon = ((lon-dlon + math.pi) % (2*math.pi)) - math.pi

    # Convert new coordinates to degrees.
    new_lat = math.degrees(new_lat)
    new_lon = math.degrees(new_lon)

    return new_lat, new_lon


class ProjectFileManager:
    """Handle the formation of all files related to a project.

    Public methods:
        update_dates
        write_config
        write_investigation
        close
    """

    def __init__(self, prefix, year):
        """Initialize the instance.

        prefix -- The prefix of the provider that this project's data
            comes from.
        year -- The year that this project's data was collected.
        """
        self.prefix = prefix
        self.year = year

        csv_filename = '{}_{}.pop'.format(prefix, year)
        self.writer = CSVWriter(csv_filename)
        self.writerow = self.writer.writerow

        self.first_date = dt.date.max
        self.last_date = dt.date.min
        self.month = None

    def update_dates(self, date):
        """Update the first and/or last date, as appropriate.

        date -- The date of a capture that is being grouped into this
            project.
        """
        if date < self.first_date:
            self.first_date = date
            self.month = date.month
        if date > self.last_date:
            self.last_date = date

    def write_config(self):
        """Write the config file from a template.

        The config file is intended to be passed to PopBioWizard.pl
        further down the pipeline.
        """
        template_path = 'bg_config.tpl'
        config_path = '{}_{}.config'.format(self.prefix, self.year)

        with open(template_path) as template_f, open(config_path, 'w') as config_f:
            template = Template(template_f.read())
            config_text = template.substitute(prefix=self.prefix, year=self.year,
                                              month=str(self.month).zfill(2))
            config_f.write(config_text)

    def write_investigation(self):
        """Write the investigation sheet from a template.

        The investigation sheet is one of the four ISA-Tab sheets.  The
        other three are created by PopBioWizard.pl.
        """
        data = get_provider_metadata(prefix=self.prefix)

        template_path = 'bg_investigation.tpl'
        inv_path = '{}_{}.inv'.format(self.prefix, self.year)

        with open(template_path) as template_f, open(inv_path, 'w') as inv_f:
            template = Template(template_f.read())

            inv_text = template.substitute(
                prefix=self.prefix, year=self.year, month=str(self.month).zfill(2),
                start_date=self.first_date, end_date=self.last_date, org_name=data['org_name'],
                org_email=data['org_email'], org_url=data['org_url'],
                contact_first_name=data['contact_first_name'],
                contact_last_name=data['contact_last_name'], contact_email=data['contact_email'],
                study_tag=data['study_tag'], study_tag_number=data['study_tag_number']
            )

            inv_f.write(inv_text)

    def close(self):
        """Close the data file and write other files if necessary.

        This function calls self.writer's close function, which checks
        to see whether any data was written to the data file and deletes
        the file if not.  If data was written, this function then writes
        the config and investigation files and returns a dict containing
        the project information.  Otherwise it returns None.
        """
        if self.writer.close():
            self.write_config()
            self.write_investigation()

            return {'prefix': self.prefix, 'year': self.year}

        else:
            return None


class CSVWriter:
    """Handle CSV data file operations.

    Public methods:
        is_empty
        close
    """

    def __init__(self, filename):
        """Initialize the instance."""
        self.filename = filename
        self.file = open(filename, 'w')
        self.writer = csv.writer(self.file, lineterminator='\n')
        self.writerow = self.writer.writerow

        self.writerow([
            'collection_ID', 'sample_ID', 'collection_start_date', 'collection_end_date',
            'trap_ID', 'GPS_latitude', 'GPS_longitude', 'location_description', 'trap_type',
            'attractant', 'trap_number', 'trap_duration', 'species',
            'species_identification_method', 'developmental_stage', 'sex', 'sample_count',
        ])

        self.empty_pos = self.file.tell()

    def is_empty(self):
        """Return whether the CSV file is empty of any data rows."""
        return self.file.tell() == self.empty_pos

    def close(self):
        """Close the CSV file.

        Closes the object's file and deletes the file if it has
        no data.  Returns True if there was data and False if not.
        """
        if self.is_empty():
            remove = True
        else:
            remove = False

        self.file.close()

        if remove:
            os.remove(self.filename)

        return not remove


if __name__ == '__main__':
    args = vars(parse_args())
    parse_json(**args)
