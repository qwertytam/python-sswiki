# Hacky way to import modules instead of using setup.py or similiar
from pathlib import Path
# import pandas as pd
import sys
path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)

import sswiki.sswiki as sswiki
import sswiki.constants as const

# Script static variables
ARTICLE_PATTERN = "wiki/USS_"
LISTS_OF_LISTS_URL = const.BASE_URL + "/wiki/List_of_United_States_Navy_ships"

TEST_VLINKS = ["https://en.wikipedia.org/wiki/HMS_Holmes_(K581)",
               "https://en.wikipedia.org/wiki/USS_William_R._Rush_(DD-714)",
               "https://en.wikipedia.org/wiki/USS_Wiltsie",
               "https://en.wikipedia.org/wiki/USS_Lloyd_Thomas_(DD-764)",
               "https://en.wikipedia.org/wiki/HMS_Holmes_(K581)",
               "https://en.wikipedia.org/wiki/USS_William_R._Rush_(DD-714)",
               "https://en.wikipedia.org/wiki/USS_Iowa_(BB-61)",
               "https://en.wikipedia.org/wiki/USS_Philippine_Sea_(CV-47)",
               "https://en.wikipedia.org/wiki/USS_Gwin_(TB-16)",
               "https://en.wikipedia.org/wiki/USS_Wilkes_(TB-35)",
               "https://en.wikipedia.org/wiki/USS_Kingfish",
               "https://en.wikipedia.org/wiki/USS_Monongahela_(AO-42)",
               "https://en.wikipedia.org/wiki/USNS_Mission_San_Miguel",
               "https://en.wikipedia.org/wiki/USS_Fidelity_(AM-96)",
               "https://en.wikipedia.org/wiki/USS_Garnet_(PYc-15)"]

# First find a list of vessels by type (e.g. battleship)
# The lists of lists url points to an article that has a list of the Navy ships
# by type (e.g. list of aircraft carriers, list of battleships)
group_lists = sswiki.scrapeForGroupListsURLs(LISTS_OF_LISTS_URL)

# Now find the links to each vessel article,
# then scrape the data from each article
vessel_links = sswiki.getVesselLinks(group_lists, ARTICLE_PATTERN)
# vessel_links.to_csv("../tmp/vessel_links.csv")

# vl = pd.read_csv("../tmp/vessel_links.csv")
# vessel_links = vl.apply(lambda row: row[vl['vessel_url'].isin(TEST_VLINKS)])
# vessel_links = vessel_links.sample(1000)
# print(vessel_links)

gc, sh = sswiki.getVesselData(vessel_links,
                              'gc_data.csv',
                              'sh_data.csv',
                              'errors.csv')

# vd = utils.loadVesselData(const.DATA_DIR + 'uss_data.csv')
# vd = sswiki.convertDates(vd)
# vd = sswiki.convertLinearMeasures(vd)
# vd = sswiki.convertWeightMeasures(vd)
# vd = sswiki.convertSpeedMeasures(vd)
# vd = sswiki.convertHullNo(vd)
# vd.to_csv(const.DATA_DIR + 'uss_data.csv')
