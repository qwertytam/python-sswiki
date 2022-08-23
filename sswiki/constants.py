
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

# Data items from the vessel article infobox that are dates
DT_SH_COLS = ["Acquired",
              "Active",
              "Cancelled",
              "Christened",
              "Commissioned",
              "Completed",
              "Decommissioned",
              "Laid down",
              "Launched",
              "Lost",
              "Maiden voyage",
              "Ordered",
              "Out of service",
              "Recommissioned",
              "Renamed",
              "Retired",
              "Stricken"]

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

NONMES_GC_COLS = ["Class and type",
                  "Identification",
                  "Type"]

NONMES_SH_COLS = ["Builder",
                  "Builders",
                  "Built",
                  "Fate",
                  "In commission",
                  "In service",
                  "Name",
                  "Preserved",
                  "Reclassified",
                  "Status"]

GC_COLS = LNMES_GC_COLS + WTMES_GC_COLS + SPMES_GC_COLS + NONMES_GC_COLS
SH_COLS = DT_SH_COLS + NONMES_SH_COLS
