# Hacky way to import modules instead of using setup.py or similiar
from pathlib import Path
import sys

path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)

import sswiki.utils as utils
import sswiki.constants as const
import sswiki.sswiki as sswiki
import tmp.testing as tmp