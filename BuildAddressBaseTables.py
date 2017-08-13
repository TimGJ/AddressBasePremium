# -*- coding: utf-8 -*-
"""
Ingest the OS AddressBase Premium files from CSV.
"""
import csv
import os
import glob
import argparse
import sys
import logging
import getpass

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()


try:
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine
except ModuleNotFoundError:
    logging.error('Can\'t import SQLAlchemy. Aborting.')
    sys.exit()

from AddressBase import Base, RecordType, File
from AddressBase import Header, Street, StreetDescriptor, BLPU
from AddressBase import ApplicationCrossReference, LPI, MetaData
from AddressBase import DeliveryPointAddress, SuccessorCrossReference
from AddressBase import Organisation, Classification
from AddressBase import Trailer
from AddressBase import logger


def CreateRecordTypes():
    """
    Returns a dictionary of RecordTypes. See RecordType class for details of
    paramaters.
    """
    return {r[1]: RecordType(*r) for r in 
   [['Header',                      '10', Header,                    False],
    ['Street',                      '11', Street,                    False],
    ['Street Descriptor',           '15', StreetDescriptor,          False],
    ['BLPU',                        '21', BLPU,                      False],
    ['Application Cross Reference', '23', ApplicationCrossReference, False],
    ['LPI',                         '24', LPI,                       False],
    ['Delivery Point Address',      '28', DeliveryPointAddress,      False],
    ['MetaData',                    '29', MetaData,                  False],
    ['Successor Cross Reference',   '30', SuccessorCrossReference,   False],
    ['Organisation',                '31', Organisation,              False],
    ['Classification',              '32', Classification,            False],
    ['Trailer',                     '99', Trailer,                   False]]}

def CreateObject(rt, rec):
    """
    Returns an SQLalchemy object based on the record - e.g. if it's passed
    a Header (type 10) record, it creates a Header record and populates 
    it with the details from the record. 
    
    The RecordType object has the mapping (i.e. what class the record maps to) 
    and a list of the fields of that class in order. So all we need to do is 
    go through the fields in the record and allocate each to the appropriate
    attribute.
    
    rt:  RecordType
    rec: The record itself
    """
    try:
        if rt.mapping and not rt.ignore:
            ob = rt.mapping()
            for f, v in zip(rt.fields, rec):
                if v:
                    setattr(ob, f, v)
                else:
                    setattr(ob, f, None)
            return ob
        return None
    except KeyError:
        logger.warning("Got unknown record type '{}'".format(rec[0]))
        return None


def CreateAddressBaseTables(patterns, rebuild = False):    
    """
    Creates the various tables to hold the AddressBase Premium data which are
    read in from a series of CSV files, specified in 'patterns'
    
    patterns is a list of file names e.g. foo.csv or patterns e.g. *.csv.
    
    The Linux/Unix shell will automatically glob these from the command
    line, but Windows doesn't so we must manually glob the list...
    
    The rebuild flag causes all the tables to be rebuilt from sratch
    """

    RecTypes = CreateRecordTypes()

    # If applicable, drop the tables safely.
    try:
        if rebuild:
            for r in RecTypes:
                logger.info("Dropping {} table".format(RecTypes[r].name))
                RecTypes[r].mapping.__table__.drop(bind=engine, checkfirst=True)
            logger.info("Dropping File table")
            File.__table__.drop(bind=engine, checkfirst=True)
    
        Base.metadata.create_all(engine)
        
    except: # As we don't know what connector is being used, we don't know what error will be generated
        logger.error("Can't connect to database with {}".format(engine))
        sys.exit()
        
    # Now glob the filenames from the list of patterns (principally for Windows installations)
    files = []
    for p in patterns:
        files += glob.glob(p)
        
    if len(files):
        records = 0
        Session = sessionmaker(bind=engine)
        session = Session()    
        imported =[z[0] for z in session.query(File.FileName).filter(File.SupersededBy == None).all()]
        # Iterate through the list of files...
        for i, file in enumerate(files):
            fname = os.path.split(file)[1]
            # Check if the name is already in our list of imports
            if fname in imported:
                logger.info('File {} already imported. Skipping.'.format(fname))
                continue
            counts = {t:0 for t in RecTypes} # Keep track of the numbers of each record
            counts['Error'] = 0
            logger.info("Processing {} ({}/{})".format(fname, i+1, len(files)))
            frec=File(fname, session)
            # Note that at the time of writing the CSVs were encoded in latin-1, rather than utf-8
            with open(file, encoding='latin-1') as f:
                for j, row in enumerate(csv.reader(f)):
                    rt = RecTypes[row[0]]
                    if len(row) != len(rt.fields) + 1:
                        logger.warning("Got {} fields for {} record at line {} of {}: {}".\
                               format(len(row)-1, rt.name, j+1, fname, "|".join(row[1:])))
                    counts[rt.code] += 1
                    o = CreateObject(rt, row[1:])
                    if o:
                        session.add(o)
            records += j+1
            session.commit()
            frec.Update(counts, session)
        logger.info("Read {} files {:,} records".format(i+1, records))
        session.close()
    else:
        logger.warning('Cant find any files')


if __name__ == '__main__':

    # Parse the command line arguments
    parser = argparse.ArgumentParser('Ingest ABP files')
    parser.add_argument("files",        help='CSV files to read', nargs = '+')
    parser.add_argument('--host',       help='Database hostname or IP address')
    parser.add_argument('--password',   help='Database password')
    parser.add_argument('--username',     help='User name',
                        default='addressbasepremium')
    parser.add_argument('--dbname',     help='Datebase name',
                        default='addresses')
    parser.add_argument('--connector',  help='SQLAlchemy Connector',
                        default = 'mysql+mysqlconnector')
    parser.add_argument('--overwrite',  help='Overwrite existing database', 
                        action = "store_true",)
    args = parser.parse_args()

    # If we haven't got any credentials then abort
    if not all([args.host, args.dbname, args.username]):
        logger.error('Requires database credentials. See --help option for details.')
        sys.exit()

    # If we've not been given a password on the command line prompt for one
    if not args.password:
        args.password = getpass.getpass()
        
    # Setup the database connection        
    connectionstring = '{}://{}:{}@{}/{}'.format(args.connector, args.username, 
                        args.password, args.host, args.dbname)
    engine = create_engine(connectionstring)
    Session = sessionmaker(bind=engine)
    
    # Create the tables
    CreateAddressBaseTables(args.files, rebuild=args.overwrite)
