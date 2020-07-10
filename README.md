![Tests](https://github.com/jeromerg/filoc/workflows/Tests/badge.svg)

FiLoc File Locator
==================

Filoc makes the saving, reading and analysis of files easy: You files are the database. 

Machine Learning Swiss Knife
----------------------------
In machine learning, we use it to schedule, record, and analyze hundreds of machine learning experiences 
without the need of any service.

### Schedule Simulations

Prepare hyperparameters for all your simulations at one place in a pandas dataframe or in an excel sheet. 
Save the simulation configurations into the folder structure of your choice, along with the processing state.

### Record Simulations

In the algorithm script, you do not need to introduce any dependency to *filoc*. Just open the files previously
written in the simulation folder. Nevertheless, you can also use *filoc* to harmonize your file handling. 

### Analyze Simulations

While the simulation are running or after, you can get insight on any information about the current results.
 Filoc doesn't do any difference between the simulation state, the hyperparameters and the result files of your 
 simulations: it handles all in the same way and displays statistical analysis along with the hyperparameters.


Basic
-----

```python
import os
from filoc import Filoc

loc = Filoc('/data/simid={simid:d}/epid={epid:d}/settings.json') 

#--------------------------------------------------------------
# Build the path for simid=0 and epid=1, then write to the file
#--------------------------------------------------------------
path1 = loc.get_path(simid=0, epid=1)  # /data/simid=0/epid=1/settings.json
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
props = loc.get_path_properties(paths[0]) # { 'simid': 0, 'epid': 1 }

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

#--------------------------------------------------------------
# Analyse all files and build a report
#--------------------------------------------------------------
def my_analysis(f):
    content = f.read()
    return { 'content_length' : len(content) }

fimap = Filoc(loc, my_analysis)

report = fimap.read_contents(simid=0)

print(report) #-> [{"simid":0, "epid":1, "content_length":6}, {"simid":0, "epid":2, "content_length":5}]

from pandas import DataFrame
print(DataFrame(report)) #-> convert report to pandas DataFrame 

```    

Install
-------

    pip install filoc

Syntax
------

The FiLoc constructor accepts a file path/url, which will finally be interpreted by [fsspec](https://pypi.org/project/fsspec). 
That way, it is possible to access ftp, HDFS or any other file repository supported by fsspec. 
The path is at the same time a format string with named placeholder, which will be parsed by the [parse library](https://pypi.org/project/parse/).
Each placeholder defines a *property* associated to the files to save, retrieve or analyse.
 
ToDos
-----

- Readme Doc
- API Doc 
- File locking
- Unit Test 
    - Multiloc with different aggregation layers (snowflake)
    - File locking
- Dev: Logging of edited files
- Dev: File watcher
- Dev: Excel File watcher / binder
