# Hacky way to import modules instead of using setup.py or similiar
from pathlib import Path
import sys
path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)

import sswiki.utils as utils
import sswiki.constants as const
import sswiki.sswiki as sswiki

# For GitBash on Windows 10
# sys.stdin.reconfigure(encoding='utf-8')
# sys.stdout.reconfigure(encoding='utf-8')

# Script static variables
ARTICLE_PATTERN = "wiki/USS_"
LISTS_OF_LISTS_URL = const.BASE_URL + "/wiki/List_of_United_States_Navy_ships"

# TEST_VLINKS = ["https://en.wikipedia.org/wiki/HMS_Holmes_(K581)",
#                "https://en.wikipedia.org/wiki/USS_William_R._Rush_(DD-714)",
#                "https://en.wikipedia.org/wiki/USS_Wiltsie",
#                "https://en.wikipedia.org/wiki/USS_Lloyd_Thomas_(DD-764)",
#                "https://en.wikipedia.org/wiki/HMS_Holmes_(K581)",
#                "https://en.wikipedia.org/wiki/USS_William_R._Rush_(DD-714)",
#                "https://en.wikipedia.org/wiki/USS_Iowa_(BB-61)",
#                "https://en.wikipedia.org/wiki/USS_Philippine_Sea_(CV-47)",
#                "https://en.wikipedia.org/wiki/USS_Gwin_(TB-16)",
#                "https://en.wikipedia.org/wiki/USS_Wilkes_(TB-35)",
#                "https://en.wikipedia.org/wiki/USS_Kingfish",
#                "https://en.wikipedia.org/wiki/USS_Monongahela_(AO-42)",
#                "https://en.wikipedia.org/wiki/USNS_Mission_San_Miguel",
#                "https://en.wikipedia.org/wiki/USS_Fidelity_(AM-96)",
#                "https://en.wikipedia.org/wiki/USS_Garnet_(PYc-15)"]

# First find a list of vessels by type (e.g. battleship)
# The lists of lists url points to an article that has a list of the Navy ships
# by type (e.g. list of aircraft carriers, list of battleships)
# group_lists = sswiki.scrapeForGroupListsURLs(LISTS_OF_LISTS_URL)

# Now find the links to each vessel article,
# then scrape the data from each article
# vessel_links = sswiki.getVesselLinks(group_lists, ARTICLE_PATTERN)
#
# gc, sh = sswiki.getVesselData(vessel_links,
#                               'gc_data.csv',
#                               'sh_data.csv',
#                               'errors.csv')

# Format general characteristics
gc = utils.loadVesselData(const.DATA_DIR + 'gc_data.csv', index_col='uuid')
gc = utils.dfStrNormalize(gc)
gc = sswiki.convertLinearMeasures(gc, const.LNMES_GC_COLS)
gc = sswiki.convertWeightMeasures(gc, const.WTMES_GC_COLS)
gc = sswiki.convertSpeedMeasures(gc, const.SPMES_GC_COLS)
# gc.to_csv(const.DATA_DIR + 'gc_data.csv')
gc.to_csv('../tmp/' + 'gc_data.csv')
print("finished gc\n")
# Format service history
sh = utils.loadVesselData(const.DATA_DIR + 'sh_data.csv', index_col='uuid')
sh = utils.dfStrNormalize(sh)
sh = sswiki.convertDates(sh, const.DT_SH_COLS)
sh = sswiki.convertHullNo(sh)
sh.to_csv('../tmp/' + 'sh_data.csv')
