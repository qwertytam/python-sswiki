# Hacky way to import modules instead of using setup.py or similiar
from pathlib import Path
import sys
path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)

import sswiki.constants as const
import sswiki.sswiki as sswiki
import sswiki.usswiki as usswiki
import sswiki.utils as utils

# Script static variables
ARTICLE_PATTERN = "/wiki/USS"
LISTS_OF_LISTS_URL = const.BASE_URL + "/wiki/List_of_United_States_Navy_ships"

# First find a list of vessels by type (e.g. battleship)
# The lists of lists url points to an article that has a list of the Navy ships
# by type (e.g. list of aircraft carriers, list of battleships)
# group_lists = usswiki.scrapeForGroupListsURLs(LISTS_OF_LISTS_URL)

# Now find the links to each vessel article,
# then scrape the data from each article
# vessel_links = sswiki.getVesselLinks(group_lists, ARTICLE_PATTERN)
# vessel_data = sswiki.getVesselData(vessel_links,
#                                    'uss_data.csv',
#                                    'uss_errors.csv')

vd = utils.loadVesselData(const.DATA_DIR + 'uss_data.csv')
vd = sswiki.convertDates(vd)
vd = sswiki.convertLinearMeasures(vd)
vd = sswiki.convertWeightMeasures(vd)
vd = sswiki.convertSpeedMeasures(vd)
vd = sswiki.convertHullNo(vd)
vd.to_csv(const.DATA_DIR + 'uss_data.csv')
