import pandas as pd

STATUS_OK = 200

BASE_URL = "https://en.wikipedia.org"

FT_TO_M = 0.3048
IN_TO_M = FT_TO_M / 12

LTONS_TO_MTONS = 1.016047
STONS_TO_MTONS = 0.90718

MPH_TO_KNTS = 1 / 1.15078

DATA_DIR = "../data/"

# Data frame columns for holding the links to each vessel article
VL_COLS = ["group_type",
           "group_type_url",
           "vessel_url"]

# Vessel general characteristic data items
# Linear measurements e.g. feet, metres
LNMES_GC_COLS = ["Beam",
                 "Draft",
                 "Draught",
                 "Length"]

# Weight measurements e.g. tons, long tons, short tons
WTMES_GC_COLS = ["Displacement",
                 "Tonnage"]

# Speed measurements e.g. knots
SPMES_GC_COLS = ["Speed"]

# All other non-measurement columns
NONMES_GC_COLS = ["Class and type",
                  "Identification",
                  "Type"]


# Vessel service history data items
# Data items from the vessel article infobox that are dates
DT_SH_COLS = ["Acquired",
              "Christened",
              "Commissioned",
              "Completed",
              "Decommissioned",
              "In service",
              "Laid down",
              "Launched",
              "Maiden voyage",
              "Ordered",
              "Out of service",
              "Recommissioned",
              "Renamed",
              "Stricken"]

# All other non-measurement columns
NONMES_SH_COLS = ["Builder",
                  "Builders",
                  "Built",
                  "Fate",
                  "Identification",
                  "In commission",
                  "Name",
                  "Operator",
                  "Owner",
                  "Reclassified",
                  "Status",
                  "Yard number"]

# Combined column names
GC_COLS = LNMES_GC_COLS + WTMES_GC_COLS + SPMES_GC_COLS + NONMES_GC_COLS
SH_COLS = DT_SH_COLS + NONMES_SH_COLS

# Regex patterns to search for fate
PAT_SCRAPPED = r'(?P<Scrapped>' \
    + r'(?:Scrap(?:ping)?)' \
    + r'|(?:Scrapped)' \
    + r'|(?:Broke(?:n)? up)' \
    + r'|(?:Dismant(?:(?:led)|(?:ling)))' \
    + r'|(?:Disposed)' \
    + r'|(?:Retired)' \
    + r'|(?:Laid up)' \
    + r'|(?:Abandoned)' \
    + r'|(?:Discarded)' \
    + r'|(?:Recycling)' \
    + r'|(?:Recycled)' \
    + r'|(?:Condemned)' \
    + r'|(?:Hulked)' \
    + r'|(?:Stricken)' \
    + r'|(?:Damaged beyond)' \
    + r')'

PAT_TRANS = r'(?P<Transferred>' \
    + r'(?:Transfer(?:red)?)' \
    + r'|(?:Loaned)' \
    + r'|(?:Lent to)' \
    + r'|(?:Donated)' \
    + r'|(?:Returned)' \
    + r'|(?:Interned)' \
    + r'|(?:Reflagged)' \
    + r'|(?:Expropriated)' \
    + r'|(?:Requisitioned)' \
    + r'|(?:Taken over)' \
    + r'|(?:Turned over)' \
    + r'|(?:Delivered to)' \
    + r'|(?:Museum)' \
    + r'|(?:Preserved)' \
    + r'|(?:^To Morocco)' \
    + r'|(?:^To Brazil)' \
    + r'|(?:^To Chile)' \
    + r'|(?:^To Turkey)' \
    + r'|(?:^To Argentina)' \
    + r'|(?:^To Colombia)' \
    + r'|(?:^To Taiwan)' \
    + r'|(?:^To Iran)' \
    + r'|(?:^To Japan)' \
    + r'|(?:^To Venezuela)' \
    + r')'

PAT_SUNK = r'(?P<Sunk>' \
    + r'(?:Sunk)' \
    + r'|(?:Sank)' \
    + r'|(?:Scuttled)' \
    + r'|(?:Foundered)' \
    + r'|(?:Burn(?:(?:ed)|(?:t)))' \
    + r'|(?:Wrecked)' \
    + r'|(?:capsized)' \
    + r'|(?:R(?:a|u)n aground)' \
    + r'|(?:Grounded)' \
    + r'|(?:Expended)' \
    + r'|(?:Disappeared)' \
    + r'|(?:Destroyed)' \
    + r'|(?:Target)' \
    + r'|(?:Struck mine)' \
    + r'|(?:Mined)' \
    + r'|(?:Missing)' \
    + r'|(?:Crashed)' \
    + r'|(?:Torpedo)' \
    + r'|(?:Lost (?:(?:at)|(?:by)|(?:with)|(?:to)|(?:off)|(?:after)|(?:during)|(?:while)|(?:in)))' \
    + r'|(?:Declared lost)' \
    + r'|(?:Exploded)' \
    + r'|(?:Stranded)' \
    + r')'

PAT_SOLD = r'(?P<Sold>' \
    + r'(?:Sold)' \
    + r'|(?:Acquired)' \
    + r'|(?:Leased)' \
    + r'|(?:Purchased)' \
    + r'|(?:Chartered)' \
    + r')'

PAT_CAPTURED = r'(?P<Captured>' \
    + r'(?:Captured)' \
    + r'|(?:Seized)' \
    + r'|(?:Escaped)' \
    + r')'

PAT_CANCELLED = r'(?P<Cancelled>' \
    + r'(?:Cancelled)' \
    + r')'

PAT_UNKNOWN = r'(?P<Unknown>' \
    + r'(?:Unknown)' \
    + r')'


# Regex patterns for date extraction and correction
START_PAT = r'(?P<Date>'
DAYS_PAT = r'(?P<Day>[1-3]?\d)'
MONTHS_PAT = r'\b(?P<Month>' + \
    'Jan(?:uary)?' + \
    '|Feb(?:ruary)?' + \
    '|Mar(?:ch)?' + \
    '|Apr(?:il)?' + \
    '|May(?:)?' + \
    '|Jun(?:e)?' + \
    '|Jul(?:y)?' + \
    '|Aug(?:ust)?' + \
    '|Sep(?:tember)?' + \
    '|Oct(?:ober)?' + \
    '|Nov(?:ember)?' + \
    '|Dec(?:ember)?' + ')'
YEAR_PAT = r'(?P<Year>[1-2]\d{3})'
END_PAT = r')'

REF_PAT = r'(?:\s?\[\d\])*'  # For Wikiedia references of the form [x]
ORD_PAT = r'(?:st|nd|rd|th)?'

DATE_PATS = {
    'DD MMMMMM YYYY': START_PAT + DAYS_PAT + REF_PAT + ' ' + MONTHS_PAT
    + REF_PAT + ' ' + YEAR_PAT + END_PAT,
    'MMMMMM DD, YYYY': START_PAT + MONTHS_PAT + ' ' + DAYS_PAT + ORD_PAT
    + REF_PAT + ', ' + YEAR_PAT + END_PAT,
    'MMM. DD, YYYY': START_PAT + MONTHS_PAT + r'\. ' + DAYS_PAT + ', '
    + YEAR_PAT + END_PAT,
    'YYYY-MM-DD': START_PAT + YEAR_PAT + r'-(?P<Month>\d{2})-(?P<Day>\d{2})'
    + END_PAT,
    'DD/MM/YYYY': START_PAT + DAYS_PAT + r'\/(?P<Month>\d{2})\/' + YEAR_PAT
    + END_PAT,
    'MMMMMM YYYY': START_PAT + MONTHS_PAT + r'(?:\,)?(?: of)? ' + YEAR_PAT
    + END_PAT,
    'Winter YYYY': START_PAT + r'(?P<Month>(?:W|w)inter)' + r'(?:\,)?(?: of)? '
    + YEAR_PAT + END_PAT,
    'Early YYYY': START_PAT + r'(?P<Month>(?:E|e)arly)' + r'(?:\,)?(?: of)? '
    + YEAR_PAT + END_PAT,
    'Spring YYYY': START_PAT + r'(?P<Month>(?:S|s)pring)' + r'(?:\,)?(?: of)? '
    + YEAR_PAT + END_PAT,
    'Mid YYYY': START_PAT + r'(?P<Month>(?:M|m)id)' + r'(?:\-| )'
    + YEAR_PAT + END_PAT,
    'Summer YYYY': START_PAT + r'(?P<Month>(?:S|s)ummer)' + r'(?:\,)?(?: of)? '
    + YEAR_PAT + END_PAT,
    'Late YYYY': START_PAT + r'(?P<Month>(?:L|l)ate)' + r'(?:\,)?(?: of)? '
    + YEAR_PAT + END_PAT,
    'End YYYY': START_PAT + r'(?P<Month>(?:E|e)nd)' + r'(?:\,)?(?: of)? '
    + YEAR_PAT + END_PAT,
    'YYYY': START_PAT + YEAR_PAT + END_PAT,
}

DATE_PAT_REPL = pd.DataFrame(
            [['DD MMMMMM YYYY', ('D', 'M', 'Y')],
             ['MMMMMM DD, YYYY', ('D', 'M', 'Y')],
             ['MMM. DD, YYYY', ('D', 'M', 'Y')],
             ['YYYY-MM-DD', ('D', 'M', 'Y')],
             ['DD/MM/YYYY', ('D', 'M', 'Y')],
             ['MMMMMM YYYY', ('1', 'M', 'Y')],
             ['Winter YYYY', ('1', '1', 'Y')],
             ['Early YYYY', ('1', '2', 'Y')],
             ['Spring YYYY', ('1', '4', 'Y')],
             ['Mid YYYY', ('1', '7', 'Y')],
             ['Summer YYYY', ('1', '7', 'Y')],
             ['Late YYYY', ('1', '11', 'Y')],
             ['End YYYY', ('1', '12', 'Y')],
             ['YYYY', ('1', '1', 'Y')], ], columns=['pat', 'repl'])
