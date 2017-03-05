#AddressBasePremium
##Description
Python3 scripts which ingest the OS _AddressBase Premium_ data from CSV. They do so using the SQLAlchemy ORM system, so as well as creating and populating the database tables there are also a set of Python classes created which can subseuquently be used to maniplate the data in a Pythonic way.
##Requirements
* Python >= 3.0
* SQLAlchemy
* The appropriate Python libraries for whatever SQLAlchemy connector you intend using: e.g. the `pyscopg2` package if you are using a Postgresql backend
##Environment and prerequisites
The software has been tested and runs OK on Windows and Linux with Postgresql and MySQL back-ends.
##Running the software
The software is designed to be run from the command line - e.g. under Linux. The source file `AddressBase.py` contains the various classes used by SQLAlchemy. The source file `BuildAddressTables.py` contains the ingest routines and it is this which should be run e.g.

$ python3 --host myhost --username fred --password wilma --dbname abp /path/to/files/*.csv`

###Options

`--host`: hostname or IP address of the server on which the database resides

`--username`: Database user name

`--password`: Password to connect to database

`--dbname`: Name of the database (has to have been created already)

`--overwrite`: Drop and recreate any existing tables

##Notes

* The database specified will already have to have been created on the server e.g. `CREATE DATABASE ADDRESSBASEPLUS` or whatever.
* By default the not reload an existing file. That is, if it already has an entry for `foo.csv` in its files table, it will not reload it. To alter this specify the `--overwrite` flag.
