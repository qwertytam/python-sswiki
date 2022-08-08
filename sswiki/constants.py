
STATUS_OK = 200

BASE_URL = "https://en.wikipedia.org/"

FT_TO_M = 0.3048
IN_TO_M = FT_TO_M / 12

LTONS_TO_MTONS = 1.016047
STONS_TO_MTONS = 0.90718

DATA_DIR = "../data/"

# Data frame columns for holding the links to each vessel article
VL_COLS = ["group_type",
           "group_type_url",
           "vessel_url"]


# Data items from the vessel article infobox that are dates
DT_COLS = ["Acquired",
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
LN_MES = ["Beam",
          "Draft",
          "Draught",
          "Length",
          ]

# Weight measurements e.g. tons, long tons, short tons
WT_MES = ["Displacement",
          "Tonnage"]

# Speed measurements e.g. knots
SP_MES = ["Speed"]

# Data items from the vessel article infobox to keep
VD_COLS = DT_COLS + LN_MES + WT_MES + SP_MES + \
    ["Builder",
     "Builders",
     "Built",
     "Class and type",
     "Fate",
     "Identification",
     "In commission",
     "In service",
     "Name",
     "Preserved",
     "Reclassified",
     "Status",
     "Type"]
