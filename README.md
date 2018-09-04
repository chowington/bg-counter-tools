# bg-counter-tools
BG-Counter Tools is set of tools that can pull data from Biogents BG-Counter smart mosquito traps and convert them into useful formats.

## Usage
### Downloading Data
To download smart trap data, use **smart_trap_download_data.py**. You'll need to provide the API key associated with your (or someone else's) Biogents account.
```
python3 smart_trap_download_data.py -k [API key] -s [start time] -e [end time] -o [output filename]
```
Dates are entered in one of the following formats, where “T” is literal: `YYYY-MM-DD`, `YYYY-MM-DDTHH-MM`, or `YYYY-MM-DDTHH-MM-SS`. This dumps the data into one or more JSON files, depending on the options used.

### Converting to Darwin Core
#### Creating and Updating Metadata
To generate Darwin Core files from the JSON, first you need to create a metadata file and add your API key to it. Use **smart_trap_update_metadata.py** for this.
```
python3 smart_trap_update_metadata.py add-key [API key] [entity name] [prefix]
```
**Entity name** is the name of the person or organization that owns the traps, and **prefix** is the string that will form the beginning of the event and occurrence IDs.  Note that you only need to add an API key once.

Next, you'll need to add the new traps in the JSON output file to the API key's metadata.
```
python3 smart_trap_update_metadata.py update-traps [API key] [output filename]
```
You should do this each time you have new data, just in case new traps were added.

####Generating Darwin Core files
Finally, to generate the Darwin Core files, use **smart_trap_json_parser.py**.
```
python3 smart_trap_json_parser.py [output filename]
```
This will create two files: `sampling_events.csv` and `associated_occurrences.csv`. These files use Darwin Core terms and are adapted from [the GBIF format outlined here](https://www.gbif.org/news/82852/new-darwin-core-spreadsheet-templates-simplify-data-preparation-and-publishing).
