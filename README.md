## RedPanda

This package is designed to make it easier to get data from redshift into a pandas DataFrame and vice versa.
The redpanda package only supports python3.

## Installation

```python
pip install redpanda
```

## Example

Import and set up the config dataclasses:

```python
from redpanda import RedPanda,redshiftconfig,s3config
redconf = redshiftconfig(database = 'dev',
                         schema = 'playground',
                         host = '***.redshift.amazonaws.com',
                         user = 'admin',
                         password = '******',
                         port = 5439 #optional,default
                        )

#s3 is optional and only required for pushing to redshift
s3conf = s3config(access_key = '***',
                  secret_access_key = '***',
                  bucket = 'automation',
                  subdirectory = 'tmp')

conf = redconf,s3conf
```

After that, writing and loading a pandas dataframe is as easy as:

```python
with RedPanda(*conf) as rp:
    rp.put(dataframe,'table_name')
    df = rp.load('table_name')

```

If the table currently exists **IT WILL BE DROPPED** and then the pandas DataFrame will be put in it's place.
You can perform a check if the table exists using ```.exists(table_name)```

```python 
with RedPanda(*conf) as rp:
    if not rp.exists('redpanda'):
        rp.put(df,'redpanda')

```

If you set append = True the table will be appended to (if it exists).

If you encounter the error:
psycopg2.InternalError: current transaction is aborted, commands ignored until end of transaction block

you can access the pyscopg2 internals with the following:

```python
pr.core.connect.commit()
pr.core.connect.rollback()
```

Adjust logging [levels](https://github.com/agawronski/pandas_redshift/pull/50):

```python
pr.set_log_level('info')                     # print the copy statement, but hide keys
pr.set_log_level('info', mask_secrets=False) # display the keys
pr.set_log_level('error')                    # only log errors
```


