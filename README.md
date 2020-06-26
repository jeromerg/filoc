Filoc File Locator
==================

This tiny library eases the saving, the reading and the analysis of files within a structured folder tree.

Example
-------

```python
import os
from filoc import Filoc

loc = Filoc('/data/simid={simid:d}/epid={epid:d}/settings.json') 

#--------------------------------------------------------------
# Build the path for simid=0 and epid=1, then write to the file
#--------------------------------------------------------------
path1 = loc.build_path(simid=0, epid=1)  # /data/simid=0/epid=1/settings.json
os.makedirs(os.path.dirname(path1))
with open(path1, 'w') as f:
    f.write('Coucou')

#--------------------------------------------------------------
# Same for simid=0 and epid=1 with a more compact syntax
#--------------------------------------------------------------
with loc.open(dict(simid=0, epid=2), 'w') as f:  # file handle to /data/simid=0/epid=2/settings.json
    f.write('Salut')

#--------------------------------------------------------------
# Find all files on file system having simid=0
#--------------------------------------------------------------
paths = loc.find_paths(simid=0)          # ['/data/simid=0/epid=1/settings.json', '/data/simid=0/epid=2/settings.json']

#--------------------------------------------------------------
# Extract properties for path paths[0]
#--------------------------------------------------------------
props = loc.extract_properties(paths[0]) # { 'simid': 0, 'epid': 1 }

#--------------------------------------------------------------
# More compact form to get both the paths and their related properties
#--------------------------------------------------------------
for path, props in loc.find_paths_and_properties(simid=0):
    print(path, props)

#--------------------------------------------------------------
# Analyse all files and build a report
#--------------------------------------------------------------
def get_file_report(f):
    content = f.read()
    return { 'content_length' : len(content) }

report = loc.report(dict(simid=0), get_file_report, 'r')

print(report) #-> [{"simid":0, "epid":1, "content_length":6}, {"simid":0, "epid":2, "content_length":5}]

from pandas import DataFrame
print(DataFrame(report)) #-> convert report to pandas DataFrame 

```    

Install
-------

    pip install filoc

Syntax
------

The Filoc constructor accepts a file path/url, which will finally be interpreted by [fsspec](https://pypi.org/project/fsspec). 
That way, it is possible to access ftp, HDFS or any other file repository supported by fsspec. 
The path is at the same time a format string with named placeholder, which will be parsed by the [parse library](https://pypi.org/project/parse/).
Each placeholder defines a *property* associated to the files to save, retrieve or analyse.
 
