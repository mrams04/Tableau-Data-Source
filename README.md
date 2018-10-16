# Tableau-Data-Source

1. Tab_ds.py Python script is written in Python 2.7 version

2. To make sure tableausdk to work

Edit the file /lib/site-packages/tableausdk/__init__.py and add the following lines

from .HyperExtract import *

from .Server import *

from .Extract import *

3. This Script takes 2 different types of Parameters. 
For Extracts, it takes JSON object as input.

For Live Connection with Data Sources, it takes SQL query as input

4. tab_json.py has the details of how to execute the tab_ds.py
