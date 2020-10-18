![Tests](https://github.com/jeromerg/filoc/workflows/Tests/badge.svg)

# <img src="./filoc.svg" alt="filoc" width="150"/>


The Filoc library enables you to:

- Visualize the content of a *set of files* as a pandas DataFrame
- Save a DataFrame into a *set of files*

The *set of files* is defined by a [format string] where the placeholders are part of the data. Consider the following format string:

```
/data/{country}/{company}/info.json
```

You see two placeholders, namely `country` and `company`. Both are part of the data read and saved by filoc. Let's say that the `info.json` files contain two additional attributes `address` and `phone`, then filoc binds the files to a DataFrame with the following columns:

| <br> | country | company | address | phone |
| ---- | ------- | ------- | ------- | ----- |
| ...  | ...     | ...     | ...     | ...   |


This is the key feature of filoc, which enables you to choose the best path structure for **your needs** and at the same time to manipulate the whole data set in a single DataFrame!

## Basic example

### Read all files
Let's see concretely how to use filoc to simply read the whole *set of files*:

```python
# import the filoc factory
from filoc import filoc

# create a Filoc instance via the filoc factory
loc = filoc('/data/{country}/{company}/info.json')

# load the whole data set into a dataframe
df = loc.read_contents()

# let's see the resulting DataFrame:
print(df)

# OUTPUT
#        country  company   address  phone          
#  ----  -------  --------  -------  -------------- 
#  0     France   OVH       Roubaix  +33681906730   
#  1     Germany  Strato    Berlin   +49303001460   
#  2     Germany  DF        Munich   +4989998288026 
```

### Read a subset of files
Instead of reading all the files, you can restrict the reading to a subset of files by adding conditions as follows:

```python
df = loc.read_contents(country='Germany')
print(df)

# OUTPUT
#        country  company  address  phone          
#  ----  -------  --------  -------  -------------- 
#  0     Germany  Strato    Berlin   +49303001460   
#  1     Germany  DF        Munich   +4989998288026 
```

### Save the DataFrame
Conversely, you can save the dataframe as a *set of files*. Let's fix the address of the DF company:

```python
# The filoc need to be initialized as writable
loc = filoc('/data/{country}/{company}/info.json', writable=True)

# Change the address
df.loc[df['company'] == 'DF', 'address'] = 'Ismaning (by Munich)' 

# Save the change
loc.write_contents(df)
```

You can verify in a shell, that the file has changed correspondingly:
```shell
> cat /data/Germany/DF/info.json

{
  "address": "Ismaning (by Munich)",
  "phone": "+4989998288026"
}
```

### Working with one entry only

Sometimes, you want to work on a single row of the data set. In that case, a DataFrame is not convenient.
Filoc allows you to work with a pandas Series:

| cardinality | read                | write                | frontend class |
| ----------- | ------------------- | -------------------- | -------------- |
| 1           | loc.read_content()  | loc.write_content()  | Series         |
| *           | loc.read_contents() | loc.write_contents() | DataFrame      |

Here is how it works:

```python
# ---- read ----
series = loc.read_content(country='Germany', company='DF')
print(series)

# OUTPUT
# country               Germany
# company                    DF
# address  Ismaning (by Munich)
# phone          +4989998288026
# dtype: object

print(f'The company address is: {series.phone}')

# OUTPUT
# The company address is: Munich

series.phone = "+49 (0)89/998288026"

loc.write_content(series)
```

## Typed placeholders

A format placeholder can be typed to map to a specific python type, by using the conventional [format string] syntax. Useful and tested are mappings to integer and float:

```python
'{value:d}'  # '123' maps value to integer 123
'{value:g}'  # '3.5' maps value to float 3.5
```

**Local and remote files**

Filoc works out-of-the box with various file systems. Filoc roots on the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/index.html) library, which currently supports the following protocols:

| Protocol                   | File system                          | Additional requirements                            |
| -------------------------- | ------------------------------------ | -------------------------------------------------- |
| (none) or file://          | [local]                              |                                                    |
| memory://                  | [memory]                             |                                                    |
| zip://                     | [zip]                                |                                                    |
| ftp://                     | [ftp]                                |                                                    |
| cached:// or blockcache:// | [blockwise caching pseudo]           |                                                    |
| filecache://               | [whole file caching pseudo]          |                                                    |
| simplecache://             | [simple caching pseudo]              |                                                    |
| dropbox://                 | [dropbox]                            | dropboxdrivefs, requests, dropbox                  |
| http:// or https://        | [http]                               | requests, aiohttp                                  |
| gcs:// or gs://            | [google storage]                     | gcsfs                                              |
| gdrive://                  | [google drive]                       | gdrivefs                                           |
| sftp:// or ssh://          | [ssh]                                | paramiko                                           |
| hdfs://                    | [hadoop]                             | pyarrow and local java libraries required for HDFS |
| webhdfs://                 | [hadoop over HTTP]                   | requests                                           |
| s3://                      | [S3]                                 | s3fs                                               |
| adl://                     | [azure datalake gen1]                | adlfs                                              |
| abfs:// or az://           | [azure datalake gen2 + blob storage] | adlfs                                              |
| dask://                    | [dask worker]                        | dask                                               |
| github://                  | [github]                             | requests                                           |
| git://                     | [git]                                | pygit2                                             |
| smb://                     | [SMB]                                | smbprotocol or smbprotocol\[kerberos]              |
| jupyter:// or jlab://      | [jupyter]                            | requests                                           |

#### Example: Read Corona Stats from 

**Composite**

Filoc supports advanced scenarios, where multiple *set of files* are joined together mapping to a single DataFrame. This is especially useful if you need to aggregate files from different origin and different content. For example you can combine data from binary files and json files.

**singleton**

Filoc enables more advanced scenario too, where multiple *set of files* are joined together, building together a single DataFrame. This is especially useful if you need to aggregate files from different origin and different content. For example you can combine data from binary files and json files.

**Customizable Backend** 

You can choose to read/write JSON, YAML, CSV, pickle files, by choosing a predefined *filoc backend*. Or you may want to implement your own backend. For example, you could implement you own backend to read and pre-aggregate tensorflow log files (example coming soon). 

**Customizable Frontend** 

On the other side, you will work with DataFrames, but you can also use the alternative JSON *filoc frontend*. Or you can even implement you own *frontend*. For example, you could implement a frontend to map the files to a spark dataframe isntead of pandas DataFrame.

**Caching**

If you enable caching, filoc caches the data read by the *filoc backend*, so that unchanged files are not re-processed twice by the backend. This feature is critical in scenario with large files, especially when the *filoc backend* perform pre-aggregation of large files.

**Concurrent Safe writing**

In concurrent scenarios, you need to synchronize the writing of files. Filoc enables 

Machine Learning
----------------

(See the [Filoc Machine Learning Tour](https://htmlpreview.github.io/?https://github.com/jeromerg/filoc/blob/master/examples/example_ml.html))

Machine learning is the filoc devoted application field. Filoc enables to scale up from a single machine development to multiple server trainings without any other tools and without any changes in your code. Filoc is simple yet powerful to:

- Prepare hyperparameters
- Schedule simulations
- Analyze results

It has the following advantages in comparison to existing solution like neptune:

- No Server
- No Database
- Framework agnostic

To read how you can use filoc in Machine Learning, see the [Filoc Machine Learning Tour](https://htmlpreview.github.io/?https://github.com/jeromerg/filoc/blob/master/examples/example_ml.html).



Install
-------

```shell
pip install filoc
```

Import
------

```python
from filoc import filoc
```

Simple Example
--------------

todo

Features
--------

- Composite *file structure*
- Multiple frontends
- Multiple backends
- Customizable frontend
- Customizable backend
- Locking
- Caching



[format string]: https://docs.python.org/3/library/string.html#format-string-syntax

[local]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.local.LocalFileSystem
[memory]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.memory.MemoryFileSystem
[zip]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.zip.ZipFileSystem
[ftp]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.ftp.FTPFileSystem
[blockwise caching pseudo]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.cached.CachingFileSystem
[whole file caching pseudo]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.cached.WholeFileCacheFileSystem
[simple caching pseudo]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.cached.SimpleCacheFileSystem
[dropbox]: https://github.com/MarineChap/dropboxdrivefs
[http]: https://github.com/intake/filesystem_spec/blob/master/fsspec/implementations/http.py
[google storage]: https://gcsfs.readthedocs.io/en/latest/
[google drive]: https://github.com/intake/gdrivefs
[ssh]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem
[hadoop]: https://github.com/intake/filesystem_spec/blob/master/fsspec/implementations/hdfs.py
[hadoop over HTTP]:https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.webhdfs.WebHDFS
[S3]: https://s3fs.readthedocs.io/en/latest/index.html
[Azure Datalake Gen1]: https://github.com/dask/adlfs
[Azure Datalake Gen2 + Blob Storage]: https://github.com/dask/adlfs
[dask worker]: https://github.com/intake/filesystem_spec/blob/master/fsspec/implementations/dask.py
[github]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.github.GithubFileSystem
[git]: https://github.com/intake/filesystem_spec/blob/master/fsspec/implementations/git.py
[SMB]: https://github.com/intake/filesystem_spec/blob/master/fsspec/implementations/smb.py
[jupyter]: https://github.com/intake/filesystem_spec/blob/master/fsspec/implementations/jupyter.py