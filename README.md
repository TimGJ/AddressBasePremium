#AddressBasePremium

##Description

Python3 scripts which ingest the OS _AddressBase Premium_ data from CSV. They do so using the SQLAlchemy ORM system, so as well as creating and populating the database tables there are also a set of Python classes created which can subseuquently be used to maniplate the data in a Pythonic way.

##Requirements

* Python >= 3.0
* SQLAlchemy
* The appropriate Python libraries for whatever SQLAlchemy connector you intend using: e.g. the `pyscopg2` package if you are using a Postgresql backend

## Files

* `AddressBasePremium.py` contains the various classes used by SQLAlchemy
* `BuildAddressBaseTables.py` contains the ingest routines, and it is this which should be run

##Environment and prerequisites

The software has been tested and runs OK on Windows and Linux with Postgresql and MySQL back-ends.

It assumes that a suitable database has been created and that the appopriate credential are presented for a user with the privileges to create, modify and drop tables. 

##Running the software

The software is designed to be run from the command line - e.g. under Linux.

`$ python3 --host myhost --username fred --password wilma --dbname abp /path/to/files/*.csv`

###Options

`--host`: hostname or IP address of the server on which the database resides

`--username`: Database user name

`--password`: Password to connect to database. (The system prompts if the `--password` option is not specified on the command line).

`--dbname`: Name of the database (has to have been created already)

`--connector`: SQLAlchemy connector (defaults to `postgresql`)

`--overwrite`: Drop and recreate any existing tables

##Notes

* The database specified will already have to have been created on the server e.g. `CREATE DATABASE ADDRESSBASEPLUS` or whatever.
* By default the not reload an existing file. That is, if it already has an entry for `foo.csv` in its files table, it will not reload it. To alter this specify the `--overwrite` flag.
* The tables are defined according to the definitions in the AddressBase Premium Technical Manual. This means that things like UPRN, USRN are integer values (actually BIGINTS to allow for them to be 12 digits long). If you want to change this make the appropriate changes to `AddressBase.py`. 
* Other things such as BLPU status codes, are characters, even the ones which have numeric values in the specification. This was done for consistency with the manual.
* Each table has a primary key `id`. Therefore columns such as `blpus.uprn` are indexed for performance. It should be safe in these cases to remove the `id` column and declare e.g. `blpu.uprn` as the primary key.
