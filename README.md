![Tests](https://github.com/jeromerg/filoc/workflows/Tests/badge.svg)

<img src="./filoc.svg" alt="filoc" width="150"/>

Filoc is a highly customizable library that primarily enables you to:

- Visualize the content of a *set of files* as a pandas DataFrame
- Save a DataFrame into a *set of files*

The *set of files* is defined by a [format string] where the placeholders are part of the data. Consider the following 
format string:

```
/data/{country}/{company}/info.json
```

You see two placeholders, namely `country` and `company`. Both are part of the data read and saved by filoc. Let's 
say that the `info.json` files contain two additional attributes `address` and `phone`, then filoc works as a 
bidirectional binding between the files and a DataFrame with the following columns:

| <br> | country | company | address | phone |
| ---- | ------- | ------- | ------- | ----- |
| ...  | ...     | ...     | ...     | ...   |

This is the key feature of filoc, which enables you to choose the best path structure for *your needs* and at the 
same time to manipulate the whole data set in a single DataFrame!

Filoc is highly customizable: You can work with any type of files (builtins: *json*, *yaml*, *csv*, *pickle*) on any
 file system (*local*, *ftp*, *sftp*, *http*, *dropbox*, *google storage*, *google drive*, *hadoop*, *azure data storages*, 
 *samba*). You can even replace the pandas DataFrame by an alternative "frontend" if you need (builtins: *pandas* and *json*).

## Use Cases (Jupyter Notebook)

You can get a concrete and practical insight of filoc in the following show-case notebooks: 

[Machine Learning Workflow with filoc](https://htmlpreview.github.io/?https://github.com/jeromerg/filoc/blob/master/examples/machine_learning/example_ml.html)
 
[Covid-19 Data Analysis from the John Hopkins University Github repository](https://htmlpreview.github.io/?https://github.com/jeromerg/filoc/blob/master/examples/covid_github/example_covid_github.html)

## Basic example

### Install

First of all, you need to install the filoc library:

```shell
pip install filoc
```

### Import

In most scenarios, you only need to import the `filoc(...)` factory function:
```python
from filoc import filoc
```  

This is the most pythonic way to use filoc, but you can also use alternative factories to improve IDE static analysis,
namely `filoc_json_single(...)`, `filoc_json_composite(...)`, `filoc_pandas_single(...)`, `filoc_pandas_composite(...)`.

### Create a `Filoc` instance

Let's create a `Filoc` instance to work with *set of files* previously defined by the format path
`/data/{country}/{company}/info.json`: 

```python
loc = filoc('/data/{country}/{company}/info.json')
```

### Read all files

You read the whole *set of file* as follows:

```python
df = loc.read_contents()

print(df)

# OUTPUT
#        country  company   address  phone          
#  ----  -------  --------  -------  -------------- 
#  0     France   OVH       Roubaix  +33681906730   
#  1     Germany  Strato    Berlin   +49303001460   
#  2     Germany  DF        Munich   +4989998288026 
```

### Read a subset of files
Instead of reading all the files, you can restrict the reading to a subset of files by adding conditions:

```python
df = loc.read_contents(country='Germany')

print(df)

# OUTPUT
#        country  company  address  phone          
#  ----  -------  --------  -------  -------------- 
#  0     Germany  Strato    Berlin   +49303001460   
#  1     Germany  DF        Munich   +4989998288026 
```

### Write to the *set of files*

`Filoc` instance are by default readonly. We need to create a writable `Filoc`: 

```python
# The filoc need to be initialized as writable
loc = filoc('/data/{country}/{company}/info.json', writable=True)
```

Now, let's fix the address of the DF company and save the result:

```python
# Change the address
df.loc[df['company'] == 'DF', 'address'] = 'Ismaning (by Munich)' 

# Save the change
loc.write_contents(df)
```

Let's see with a linux shell, that the file was properly updated:

```shell
> cat /data/Germany/DF/info.json

{
  "address": "Ismaning (by Munich)",
  "phone": "+4989998288026"
}
```

### Working with a single entry

Sometimes, it is convenient to focus your work on a single row of the data set. Filoc allows you to work with 
a pandas Series instead of a DataFrame. The following table shows the filoc functions in relation 
to respectively DataFrame and Series: 

| cardinality | read                | write                | frontend class |
| ----------- | ------------------- | -------------------- | -------------- |
| 1           | loc.read_content()  | loc.write_content()  | Series         |
| *           | loc.read_contents() | loc.write_contents() | DataFrame      |

Here an example of how to use the *Series* related functions:

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

# Update the phone number and save back the change
series.phone = "+49 (0)89/998288026"
loc.write_content(series)
```

## Typed placeholders

A format placeholder can be typed to map to a specific python type. Filoc leverages the conventional 
[format string] syntax. Useful and tested are mappings to integer and float:

```python
'{value:d}'  # '123' maps value to integer 123
'{value:g}'  # '3.5' maps value to float 3.5
```

## Local and remote files

Under the hood, filoc accesses the files by using the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/index.html) library.
It enables filoc to work with the following file systems:

| Protocol                       | File system                          | Additional requirements                            |
| ------------------------------ | ------------------------------------ | -------------------------------------------------- |
| (none) or `file://`            | [local]                              |                                                    |
| `memory://`                    | [memory]                             |                                                    |
| `zip://`                       | [zip]                                |                                                    |
| `ftp://`                       | [ftp]                                |                                                    |
| `cached://` or `blockcache://` | [blockwise caching pseudo]           |                                                    |
| `filecache://`                 | [whole file caching pseudo]          |                                                    |
| `simplecache://`               | [simple caching pseudo]              |                                                    |
| `dropbox://`                   | [dropbox]                            | dropboxdrivefs, requests, dropbox                  |
| `http://` or `https://`        | [http]                               | requests, aiohttp                                  |
| `gcs://` or `gs://`            | [google storage]                     | gcsfs                                              |
| `gdrive://`                    | [google drive]                       | gdrivefs                                           |
| `sftp://` or `ssh://`          | [ssh]                                | paramiko                                           |
| `hdfs://`                      | [hadoop]                             | pyarrow and local java libraries required for HDFS |
| `webhdfs://`                   | [hadoop over HTTP]                   | requests                                           |
| `s3://`                        | [S3]                                 | s3fs                                               |
| `adl://`                       | [azure datalake gen1]                | adlfs                                              |
| `abfs://` or `az://`           | [azure datalake gen2 + blob storage] | adlfs                                              |
| `dask://`                      | [dask worker]                        | dask                                               |
| `github://`                    | [github]                             | requests                                           |
| `git://`                       | [git]                                | pygit2                                             |
| `smb://`                       | [SMB]                                | smbprotocol or smbprotocol\[kerberos]              |
| `jupyter://` or `jlab://`      | [jupyter]                            | requests                                           |

[Here is a example](https://htmlpreview.github.io/?https://github.com/jeromerg/filoc/blob/master/examples/covid_github/example_covid_github.html), how to use *github://* to read the covid statistics from the Johns Hopkins University github repository.

## Composite

Filoc instances can be joined together into a "composite filoc". The simplest syntax for that is to replace the single 
format path by a keyed list of paths:

```python
mloc = filoc({
    'contact' : '/data/contact/{country}/{company}/info.json',
    'finance' : '/data/finance/{country}/{company}/{year:d}_revenue.json'
})
```

The `contact` and `finance` keys are the name of the sub-filocs.

The alternative syntax consists in instantiating manually the sub-filocs:

```python
mloc = filoc({
    'contact' : contact_loc,
    'finance' : filoc('/data/finance/{country}/{company}/{year:d}_revenue.json', writable=True)
})
```

The alternative syntax is especially important, if you need to override the configuration for a specific "sub-filoc". In the
previous example, the second "sub-filoc" 'finance' is declared "writable", whereas the first one remains readonly.

Now, see how such a composite filoc works: 

```python
df = read_contents() 

print(df)

# OUTPUT
#   index.country  index.company  index.year      contact.address   contact.phone finance.revenue
#   -------------  -------------  ---------- --------------------  -------------- ---------------
# 0        France            OVH        2019              Roubaix    +33681906730        10256745
# 1       Germany             DF        2019 Ismaning (by Munich)  +4989998288026        14578415
# 2       Germany         Strato        2019               Berlin    +49303001460        54657631
# 3        France            OVH        2020              Roubaix    +33681906730        11132643
# 4       Germany             DF        2020 Ismaning (by Munich)  +4989998288026        37456466
# 5       Germany         Strato        2020               Berlin    +49303001460        54411544
```

Filoc joins the data from the two *set of files* together. It uses the format placeholders from the format path as 
join keys, to match and join the rows together from the both *set of files*. The shared keys are prefixed by `'index.'` whereas the attributes found 
in the files themselves are prefixed by the named of the filoc.

In this example, we have set the finance filoc *writable*, so we can edit the dataframe and save back the result:

```python
df.loc[ (df['index.year'] == 2019) & (df['index.company'] == 'OVH'), 'finance.revenue'] = 0
```

We check the updated file content:

```
$> cat /data/France/OVH/2019_revenue.json
{
  "revenue": 0
}

```

## Backend 

Filoc backend is the part of the implementation, that processes the files. You define the backend via the 
`backend` argument of the `filoc(...)` factory:

```python
loc = filoc(..., backend='yaml')
```

### Builtin backends 

Filoc has four builtin backends:

Name     | Description   | option `singleton` | option `encoding`
---------|---------------|------------------|----------------
`json`   | json files    | Yes              | Yes
`yaml`   | yaml files    | Yes              | Yes
`csv`    | csv files     | No               | Yes
`pickle` | pickle files  | Yes              | No

- Option `singleton`: If True, then filoc reads and writes a single object in each file (Mapping). If False the filoc
 reads and writes lists of object (List of Mapping).
- Option `encoding`: Configure the encoding of the file read and written by filoc. 

### Custom backends 

You can also work with custom files and perform custom pre-processing to the files, by passing a custom instance of
the `BackendContract` contract.

## Frontend

Filoc frontend is the part of the implementation, that transforms the file content to a python object, namely by 
default a DataFrame (returned by `read_contents(...)`) or a Series (returned by `read_content(..)`).

### Builtin frontends

Filoc has two builtin frontends:

| cardinality | read                | write                | frontend class       |
| ----------- | ------------------- | -------------------- | -------------------- |
| 1           | loc.read_content()  | loc.write_content()  | Dict[str, Any]       |
| *           | loc.read_contents() | loc.write_contents() | List[Dict[str, Any]] |

### Custom frontends

You can work with custom frontend objects, by passing a custom instance of the `FrontendContract` contract.

## Caching

The `filoc(...)` factory accepts a `cache_locpath` and `cache_fs` arguments. This feature is particularly useful when 
you work on remote file system or when the backend processes a large amount of data. The cache is invalidated when the 
path timestamp has changed on the file system.

The `cache_locpath` may contain format placeholders. In that case, the cache is split into multiple files basedd on 
the placeholder values. This features allows to "encapsulate" the cache data in the same folder as the original data, 
or in the same folder structure as the original data.

Example:

```python
loc = filoc('github://user:rep/data/{country}/{company}/info.json', cache_locpath='/cache/{country}/cache.dat')
```



## Locking

A simple locking mechanism working on local and remote file systems allows you to synchronize the reading and
 writing of files:


```python
with loc.lock():
    series = loc.read_content(country='Germany', company='DF')
    series.phone = "+49 (0)89/998288026"
    loc.write_content(series)
```

In this example, the reading and writing is garanteed to be concurrent safe.

The locking mechanism consists of writing a lock file on the file system: It means that the protection only
works against concurrent accesses that use the same call convention inside the `Filoc.lock()` statement.


<img src="./enjoy_filoc.svg" alt="enjoy_filoc" width="800"/>     


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