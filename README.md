![Tests](https://github.com/jeromerg/filoc/workflows/Tests/badge.svg)

<img src="./filoc.svg" alt="filoc" width="150"/>

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

## Use Cases (Jupyter Notebook)

[Machine Learning Workflow Example](https://htmlpreview.github.io/?https://github.com/jeromerg/filoc/blob/master/examples/machine_learning/example_ml.html)
 
[Covid-19 Data Analysis from the John Hopkins University Github repository](https://htmlpreview.github.io/?https://github.com/jeromerg/filoc/blob/master/examples/covid_github/example_covid_github.html)

## Basic example

### Read all files
Let's see concretely how to use filoc to simply read the whole *set of files* defined by the previous format path `/data/{country}/{company}/info.json`. 

First we need to import the filoc module and create a *Filoc* instance with the appropriate format path: 

```python
# import the filoc factory
from filoc import filoc

# create a Filoc instance via the filoc factory
loc = filoc('/data/{country}/{company}/info.json')
```

Then, you read the whole *set of file* as follows:

```python
# load the whole data set into a dataframe
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
Conversely, you can save the dataframe as a *set of files*. Let's fix the address of the DF company and save the result.
First you need to create a writable *Filoc* instance, because the previous was by default readonly: 

```python
# The filoc need to be initialized as writable
loc = filoc('/data/{country}/{company}/info.json', writable=True)
```

Then we apply the change to the previous dataframe and save the result
```python
# Change the address
df.loc[df['company'] == 'DF', 'address'] = 'Ismaning (by Munich)' 

# Save the change
loc.write_contents(df)
```

Let's see with a linux shell, that the file has changed correspondingly:
```shell
> cat /data/Germany/DF/info.json

{
  "address": "Ismaning (by Munich)",
  "phone": "+4989998288026"
}
```

### Working with one entry only

Sometimes, it is convenient to work on a single row of the data set. Filoc allows you to work with 
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

Unter the hood, filoc accesses the files by using the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/index.html) library.
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

Filoc instances can be joined together. The simplest syntax for that is to replace the single format path by a keyed list of path:

```python
mloc = filoc({
    'contact' : '/data/{country}/{company}/info.json',
    'finance' : '/data/{country}/{company}/{year:d}_revenue.json'
})
```

The `contact` and `finance` keys are the name of the sub-filocs.

You can also nest filocs together:

```python
mloc = filoc({
    'contact' : contact_loc,
    'finance' : filoc('/data/{country}/{company}/{year:d}_revenue.json', writable=True)
})
```

The second syntax is especially important, if you need to override the configuration for a specific "sub-filoc". In the previous example, the second "sub-filoc" 'finance' is declared "writable", whereas the first one remains readonly.

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

filoc join the data from the two *set of files* together. It uses the format placeholders from the format path as join keys, to try to match the rows together. The shared keys are prefixed by `index.` whereas the attributes found in the files themselves are prefixed by the named of the filoc.

In the example, we set the finance filoc writable, so we can edit the dataframe and save back the result:

```python
df.loc[ (df['index.year'] == 2019) & (df['index.company'] == 'OVH'), 'finance.revenue'] = 0
```

You can check that the file content has changed with the `cat` shell command:

```
$> cat /data/France/OVH/2019_revenue.json
{
  "revenue": 0
}

```

## Default Backends 

Filoc backend is the part of the implementation, that processes the files.
Filoc comes out-of-the-box with the following four backends:

Name     | Description   | option singleton | option encoding
---------|---------------|------------------|----------------
`json`   | json files    | Yes              | Yes
`yaml`   | yaml files    | Yes              | Yes
`csv`    | csv files     | No               | Yes
`pickle` | pickle files  | Yes              | No

### Option *singleton*

The `filoc(...)` factory argument `singleton` tells filoc to assume that the files described by the format path contain
single object. The argument works with `json`and `yaml` backends. If `singleton`is set to False, then, the files are assumed to
contain lists.

### Option *encoding*

For all three backends `json`, `yaml`, `csv`, you can configure the encoding of the file, by setting the `encoding` argument
of the `filoc(...)` factory. 

## Custom Backends 

If you work with more specific files, you may want to implement your own backend. 
For that, you just need to implement the `BackendContract` contract and pass an instance to the `filoc(...)` `backend` argument.

You can, for example, implement a custom backend to read tensorflow log files. 

## Default Frontends

On the other end - the frontend -, you can work with DataFrames as illustrated in the previous examples. But 
you can also use the alternative JSON frontend. It is a simple frontend, that work with the following frontend type:

| cardinality | read                | write                | frontend class       |
| ----------- | ------------------- | -------------------- | -------------------- |
| 1           | loc.read_content()  | loc.write_content()  | Dict[str, Any]       |
| *           | loc.read_contents() | loc.write_contents() | List[Dict[str, Any]] |

## Custom Frontends

You can implement you own frontend. For that you just need to implement the `FrontendContract`
contract.
That way, you can implement a frontend to map files to spark dataframes or to Excel Object for example.

## Caching

The `filoc(...)` factory accepts a `cache_locpath` and `cache_fs` arguments. This feature is particularly useful when you work on remote file system or when the backend process a large amount of data. The cache is invalidated when the path timestamp has changed on the file system.

The `cache_locpath` may contain format placeholders. In that case, the cache is splitted in multiple files. 
This features allows to "encapsulate" the cache data in the same folder as the original data, or in the same folder structure as the original data.

The `cache_fs` is optional and allows to specify a *fsspec* file system instance to overcome the limitations of `protocol://path` syntax, for example, if you need to pass credentials of some remote file system.


```python
loc = filoc('/data/{country}/{company}/info.json', cache_locpath='/cache/{country}/cache.dat')
```



## Locking

A simple locking mechanism allows to synchronize the reading and and writing of files:


```python
with loc.lock():
    series = loc.read_content(country='Germany', company='DF')
    series.phone = "+49 (0)89/998288026"
    loc.write_content(series)
```

In this example, the reading and writing is garanteed to be concurrent safe.
Because filoc works with local and remote file systems, the locking implementation can not leverage the locking
mechanism provided by local file systems. The locking mechanism is thus file based, what means that the protection only works against concurrent accesses that use the same call convention inside the `Filoc.lock()` statement.


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