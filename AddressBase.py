# -*- coding: utf-8 -*-
"""
Reads Ordnance Survey AddressBase Premium records from 
CSV files and populates database tables with them using SQLAlchemy,
also defining the mapped classes as well.

Note that this iterprets the AddressBase Premium records strictly in 
accordance with v2.3 of the AddressBase Premium tehcnical specification
(March 2016), importing ALL fields from ALL record types, including stuff
like metadata and headers. It also (in accordance with the specification)
treats UPRN, USRN as integers not strings. This means we have to use BIGINTs
to hold stuff like UPRN which may be up to 12 digits long. 

Note that this code will take a LONG time to process even a relatively 
small subset of the AddressBase Premium data and is intended to be run either
from the command line or even as part of a batch job.

Tim Greening-Jackson (tim@greening-jackson.com)  21 January 2017
"""

import logging
import sys
import datetime
import re
import os

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()

try:
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, BigInteger
    from sqlalchemy import String, Date, Time, DateTime, Numeric
    from sqlalchemy import inspect
except ModuleNotFoundError:
    logging.error('Can\'t import SQLAlchemy. Aborting.')
    sys.exit()
    

Base = declarative_base()


class File(Base):
    """
    Class to track files which are loaded
    """
    __tablename__          = 'files'
    id                     = Column(Integer, primary_key=True)
    MaxFileNameLength      = 80
    id                     = Column(Integer, primary_key=True)
    FileName               = Column(String(MaxFileNameLength))
    CreateStart            = Column(DateTime, default=datetime.datetime.now)
    CreateEnd              = Column(DateTime)
    SupersededBy           = Column(Integer)
    Errors                 = Column(Integer)
    Headers                = Column(Integer)
    Streets                = Column(Integer)
    StreetDescriptors      = Column(Integer)
    BLPUs                  = Column(Integer)
    ApplicationXREFs       = Column(Integer)
    LPIs                   = Column(Integer)
    DeliveryPointAddresses = Column(Integer)
    MetaData               = Column(Integer)
    SuccessorXREFs         = Column(Integer)
    Organisations          = Column(Integer)
    Classifications        = Column(Integer)
    Trailers               = Column(Integer)

    def __init__(self, name, session):
        old = session.query(File).filter(
                File.FileName == name, 
                File.SupersededBy == None).all()
        self.FileName = os.path.split(name)[1]
        session.add(self)
        session.commit()
        for file in old:
            file.SupersededBy = self.id
            
    def Update(self, counters, session):
        self.CreateEnd              = datetime.datetime.now()
        self.Errors                 = counters['Error']
        self.Headers                = counters['10']
        self.Streets                = counters['11']
        self.StreetDescriptors      = counters['15']
        self.BLPUs                  = counters['21']
        self.ApplicationXREFs       = counters['23']
        self.LPIs                   = counters['24']
        self.DeliveryPointAddresses = counters['28']
        self.SuccessorXREFs         = counters['30']
        self.Organisations          = counters['31'] 
        self.Classifications        = counters['32']
        self.MetaData               = counters['29']
        self.Trailers               = counters['99']
        session.commit()

    def __repr__(self):
        return "{} {} {} {}".format(self.FileName, self.CreateStart, 
        self.CreateEnd, self.SupersededBy)
        
class Header(Base): # Type 10 records
    """
    OS AddressBase Premium Header record. See v2.3 of the AddressBase 
    Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'headers'
    id                     = Column(Integer, primary_key=True)
    CUSTODIAN_NAME         = Column(String(40))
    LOCAL_CUSTODIAN_CODE   = Column(Integer)
    PROCESS_DATE           = Column(Date)
    VOLUME_NUMBER          = Column(Integer)
    ENTRY_DATE             = Column(Date)
    TIME_STAMP             = Column(Time)
    VERSION                = Column(String(7))
    FILE_TYPE              = Column(String(1))
    
    def __repr__(self):
        return "{} {} {}".format(self.id, self.Created, self.FileName)

class Street(Base): # Type 11 records
    """
    OS AddressBase Premium Street record. See v2.3 of the AddressBase 
    Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'streets'
    id                     = Column(Integer, primary_key=True)
    CHANGE_TYPE            = Column(String(1))
    PRO_ORDER              = Column(BigInteger)
    USRN                   = Column(BigInteger, index=True)
    RECORD_TYPE            = Column(String(1))
    SWA_ORG_REF_NAMING     = Column(Integer)
    STATE                  = Column(String(1))
    STATE_DATE             = Column(Date)
    STREET_SURFACE         = Column(String(1))
    STREET_CLASSIFICATION  = Column(String(2))
    VERSION                = Column(Integer)
    STREET_START_DATE      = Column(Date)
    STREET_END_DATE        = Column(Date)
    LAST_UPDATE_DATE       = Column(Date)
    RECORD_ENTRY_DATE      = Column(Date)
    STREET_START_X         = Column(Numeric(8, 2))
    STREET_START_Y         = Column(Numeric(9, 2))
    STREET_START_LAT       = Column(Numeric(9, 7))
    STREET_START_LONG      = Column(Numeric(8, 7))
    STREET_END_X           = Column(Numeric(8, 2))
    STREET_END_Y           = Column(Numeric(9, 2))
    STREET_END_LAT         = Column(Numeric(9, 7))
    STREET_END_LONG        = Column(Numeric(8, 7))
    STREET_TOLERANCE       = Column(Integer)

    def __repr__(self):
        return "USRN {}".format(self.USRN)
        
class StreetDescriptor(Base): #Type 15 records
    """
    OS AddressBase Premium StreetDescriptor record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'streetdescriptors'
    id                     = Column(Integer, primary_key=True)
    CHANGE_TYPE            = Column(String(1))
    PRO_ORDER              = Column(BigInteger)
    USRN                   = Column(BigInteger, index=True)
    STREET_DESCRIPTION     = Column(String(100))
    LOCALITY_NAME          = Column(String(35))
    TOWN_NAME              = Column(String(30))
    ADMINISTRATIVE_AREA    = Column(String(30))
    LANGUAGE               = Column(String(3))
    START_DATE             = Column(Date)
    END_DATE               = Column(Date)
    LAST_UPDATE_DATE       = Column(Date)
    ENTRY_DATE             = Column(Date)

    def __repr__(self):
        return "{} {} {} {} {}".format(self.USRN, 
        self.STREET_DESCRIPTION, self.LOCALITY_NAME, 
        self.TOWN_NAME, self.ADMINISTRATIVE_AREA)

class BLPU(Base): # Type 21
    """
    OS AddressBase Premium BLPU record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    OSGridRegexp           = re.compile('^([A-Z]{2})(\d{2})(\d{2})$', re.IGNORECASE)
    __tablename__          = 'blpus'
    id                     = Column(Integer, primary_key=True)
    CHANGE_TYPE            = Column(String(1))
    PRO_ORDER              = Column(BigInteger)
    UPRN                   = Column(BigInteger, index=True)
    LOGICAL_STATUS         = Column(String(1))
    BLPU_STATE             = Column(String(1))
    BLPU_STATE_DATE        = Column(Date)
    PARENT_UPRN            = Column(BigInteger)
    X_COORDINATE           = Column(Numeric(8, 2))
    Y_COORDINATE           = Column(Numeric(9, 2))
    LATITUDE               = Column(Numeric(9, 7))
    LONGITUDE              = Column(Numeric(8, 7))
    RPC                    = Column(String(1))
    LOCAL_CUSTODIAN_CODE   = Column(Integer)
    COUNTRY                = Column(String(1))
    START_DATE             = Column(Date)
    END_DATE               = Column(Date)
    LAST_UPDATE_DATE       = Column(Date)
    ENTRY_DATE             = Column(Date)
    ADDRESSBASE_POSTAL     = Column(String(1))
    POSTCODE_LOCATOR       = Column(String(8), index=True)
    MULTI_OCC_COUNT        = Column(BigInteger)

    def __repr__(self):
        return "{} {} {}".format(self.UPRN, self.LATITUDE, self.LONGITUDE)

class ApplicationCrossReference(Base): # Type 23
    """
    OS AddressBase Premium Application Cross reference record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'appxrefs'
    id                     = Column(Integer, primary_key=True)
    CHANGE_TYPE            = Column(String(1))
    PRO_ORDER              = Column(BigInteger)
    UPRN                   = Column(BigInteger, index=True)
    XREF_KEY               = Column(String(14))
    CROSS_REFERENCE        = Column(String(50))
    VERSION                = Column(Integer)
    SOURCE                 = Column(String(6))
    START_DATE             = Column(Date)
    END_DATE               = Column(Date)
    LAST_UPDATE_DATE       = Column(Date)
    ENTRY_DATE             = Column(Date)

class LPI(Base): # Type24
    """
    OS AddressBase Premium LPI record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'lpis'
    id                     = Column(Integer, primary_key=True)
    CHANGE_TYPE            = Column(String(1))
    PRO_ORDER              = Column(BigInteger)
    UPRN                   = Column(BigInteger, index=True)
    LPI_KEY                = Column(String(14))
    LANGUAGE               = Column(String(3))
    LOGICAL_STATUS         = Column(String(1))
    START_DATE             = Column(Date)
    END_DATE               = Column(Date)
    LAST_UPDATE_DATE       = Column(Date)
    ENTRY_DATE             = Column(Date)
    SAO_START_NUMBER       = Column(Integer)
    SAO_START_SUFFIX       = Column(String(2))
    SAO_END_NUMBER         = Column(BigInteger)
    SAO_END_SUFFIX         = Column(String(2))
    SAO_TEXT               = Column(String(90))
    PAO_START_NUMBER       = Column(Integer)
    PAO_START_SUFFIX       = Column(String(2))
    PAO_END_NUMBER         = Column(Integer)
    PAO_END_SUFFIX         = Column(String(2))
    PAO_TEXT               = Column(String(90))
    USRN                   = Column(Integer)
    USRN_MATCH_INDICATOR   = Column(String(1))
    AREA_NAME              = Column(String(40))
    LEVEL                  = Column(String(30))
    OFFICIAL_FLAG          = Column(String(1))

    def __repr__(self):
        return "{} {} {} {}".format(self.UPRN, self.USRN, 
        self.PAO_START_NUMBER, self.PAO_TEXT)

class DeliveryPointAddress(Base): # Type 28
    """
    OS AddressBase Premium Delivery Point Address record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__                   = 'dpaddresses'
    id                              = Column(Integer, primary_key=True)
    CHANGE_TYPE                     = Column(String(1))
    PRO_ORDER                       = Column(BigInteger)
    UPRN                            = Column(BigInteger, index=True)
    UDPRN                           = Column(BigInteger, index=True)
    ORGANISATION_NAME               = Column(String(60))
    DEPARTMENT_NAME                 = Column(String(60))
    SUB_BUILDING_NAME               = Column(String(30))
    BUILDING_NAME                   = Column(String(50))
    BUILDING_NUMBER                 = Column(Integer)
    DEPENDENT_THOROUGHFARE          = Column(String(80))
    THOROUGHFARE                    = Column(String(80)) 
    DOUBLE_DEPENDENT_LOCALITY       = Column(String(35))
    DEPENDENT_LOCALITY              = Column(String(35))
    POST_TOWN                       = Column(String(30))
    POSTCODE                        = Column(String(8), index=True)
    POSTCODE_TYPE                   = Column(String(1))
    DELIVERY_POINT_SUFFIX           = Column(String(2))
    WELSH_DEPENDENT_THOROUGHFARE    = Column(String(80))
    WELSH_THOROUGHFARE              = Column(String(80))
    WELSH_DOUBLE_DEPENDENT_LOCALITY = Column(String(35))
    WELSH_DEPENDENT_LOCALITY        = Column(String(35))
    WELSH_POST_TOWN                 = Column(String(30))
    PO_BOX_NUMBER                   = Column(String(6))
    PROCESS_DATE                    = Column(Date)
    START_DATE                      = Column(Date)
    END_DATE                        = Column(Date)
    LAST_UPDATE_DATE                = Column(Date)
    ENTRY_DATE                      = Column(Date)

    def __repr__(self):
        s = "UPRN: {} ".format(self.UPRN)
        if self.ORGANISATION_NAME:
            s += "{} ".format(self.ORGANISATION_NAME)
        if self.BUILDING_NAME:
            s += "{} ".format(self.BUILDING_NAME)
        if self.BUILDING_NUMBER:
            s += "{} ".format(self.BUILDING_NUMBER)
        if self.THOROUGHFARE:
            s += "{} ".format(self.THOROUGHFARE)
        if self.POST_TOWN:
            s += "{} ".format(self.POST_TOWN)
        if self.POSTCODE:
            s += "{} ".format(self.POSTCODE)
        return s
            
class MetaData(Base): # Type 29
    """
    OS AddressBase Premium MetaData record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'metadata'
    id                     = Column(Integer, primary_key=True)
    GAZ_NAME               = Column(String(60))
    GAZ_SCOPE              = Column(String(60))
    TER_OF_USE             = Column(String(60))
    LINKED_DATA            = Column(String(100))
    GAZ_OWNER              = Column(String(15))
    NGAZ_FREQ              = Column(String(1))
    CUSTODIAN_NAME         = Column(String(40))
    CUSTODIAN_UPRN         = Column(BigInteger)
    LOCAL_CUSTODIAN_CODE   = Column(Integer)
    CO_ORD_SYSTEM          = Column(String(40))
    CO_ORD_UNIT            = Column(String(10))
    META_DATE              = Column(Date)
    CLASS_SCHEME           = Column(String(60))
    GAZ_DATE               = Column(Date) 
    LANGUAGE               = Column(String(3))
    CHARACTER_SET          = Column(String(30))

    
class SuccessorCrossReference(Base): # Type 30
    """
    OS AddressBase Premium Successor Cross Reference record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'succxrefs'
    id                     = Column(Integer, primary_key=True)
    CHANGE_TYPE            = Column(String(1))
    PRO_ORDER              = Column(BigInteger)
    UPRN                   = Column(BigInteger)
    SUCC_KEY               = Column(String(14))
    START_DATE             = Column(Date) 
    END_DATE               = Column(Date) 
    LAST_UPDATE_DATE       = Column(Date)   
    ENTRY_DATE             = Column(Date) 
    SUCCESSOR              = Column(BigInteger)
    
class Organisation(Base): # Type 31
    """
    OS AddressBase Premium Organisation record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'organisations'
    id                     = Column(Integer, primary_key=True)
    CHANGE_TYPE            = Column(String(1))
    PRO_ORDER              = Column(BigInteger)
    UPRN                   = Column(BigInteger, index=True)
    ORG_KEY                = Column(String(14))
    ORGANISATION           = Column(String(100))
    LEGAL_NAME             = Column(String(60))
    START_DATE             = Column(Date)
    END_DATE               = Column(Date)
    LAST_UPDATE_DATE       = Column(Date)
    ENTRY_DATE             = Column(Date)
    
    def __repr__(self):
        return "{} {}".format(self.UPRN, self.ORGANISATION)

class Classification(Base): # Type 32
    """
    OS AddressBase Premium Classification record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'classifications'
    id                     = Column(Integer, primary_key=True)
    CHANGE_TYPE            = Column(String(1))
    PRO_ORDER              = Column(BigInteger)
    UPRN                   = Column(BigInteger, index=True)
    CLASS_KEY              = Column(String(14))
    CLASSIFICATION_CODE    = Column(String(6), index=True)
    CLASS_SCHEME           = Column(String(60))
    SCHEME_VERSION         = Column(Numeric(2,1))
    START_DATE             = Column(Date)
    END_DATE               = Column(Date)
    LAST_UPDATE_DATE       = Column(Date)
    ENTRY_DATE             = Column(Date)
    # Our own column replacing the existing text-based one with a
    # number and also doing a look up on the AddressBasedClassifications
    CLASS_TYPE             = Column(Integer)
    ABC                    = Column(Integer)

    def __repr__(self):
       return "{} {} {} {} (ClassScheme.id={}, ABC.id={})".format(self.UPRN, self.CLASS_KEY, 
           self.CLASSIFICATION_CODE, self.CLASS_SCHEME, self.CLASS_TYPE, self.ABC)  

class Trailer(Base): # Type 99
    """
    OS AddressBase Premium Trailer record. See v2.3 of the 
    AddressBase Premium tehcnical specification (March 2016)
    """
    __tablename__          = 'trailers'
    id                     = Column(Integer, primary_key=True)
    NEXT_VOLUME_NUMBER     = Column(Integer)
    RECORD_COUNT           = Column(BigInteger)
    ENTRY_DATE             = Column(Date)
    TIME_STAMP             = Column(Time)

class RecordType:
    """
    The different types of records which are in the OS data set. Each record 
    type in the OS scheme has a long list of fields, each of which 
    (with the exception of the RECORD_IDENITIFER field which is always 
    the same value for a particular type of record) has a 
    corresponding entry in the various SQLAlchemy classes above.
    
    Rather than reproducing the two lists, the list of fields to 
    assign is taken by geting the list of class attributes. 
    
    IMPORTANT NOTE. 
    (1) DO NOT REMOVE ANY OF THE EXISTING FIELDS OR CHANGE THEIR ORDER

    (2) IF YOU WANT TO CHANGE ANYTHING (E.G. MAKE UPRN A STRING RATHER THAN
    A BIGINT) THEN MAKE SURE THAT ALL CHANGES ARE REFLECTED ACROSS ALL TABLES 
    BEFORE REBUILDING THE DATABASE.

    (3) IF YOU WANT TO ADD YOUR OWN ATTRIBUTES THEN PUT THE NAME OF THE 
    ATTRIBUTE IN THE MYFIELDS LIST BELOW AND NAME IT IN LOWER-CASE 
    """
    regexp   = re.compile('^[A-Z][A-Z_]*$')
    myfields = ['id', 'CLASS_TYPE', 'ABC']
    def __init__(self, name, code, mapping, ignore=False):
        self.name    = name    # e.g. "Header"
        self.code    = code    # e.g. '10' (note is a str not an int!)
        self.fields  = []      # List of fields in the record
        self.mapping = mapping # The SQLAlchemy type on to which the rectype maps
        self.ignore  = ignore  # Do we bother importing. Some records (e.g. Header/Trailer may be ignored)
        # Need to get a list of the SQL Alchemy class IN ORDER so we can't
        # use the Python dir() fuction as that returns them sorted aphabeltically
        for attribute in [a.key for a in inspect(self.mapping).attrs]: 
            if attribute not in RecordType.myfields and RecordType.regexp.match(attribute):
                self.fields.append(attribute)

    def __repr__(self):
        return "{:28} ({})".format(self.name, self.code)
  
        

"""
Various code lists as specified in the OS AddressBase Technical Specification.
"""

PostalCodes = {
    'D': 'Record linked to PAF',
    'N': 'Not a postal address',
    'C': 'Postal record with a parent linked to PAF',
    'L': 'Postal record (based on local authority data)'}

CountryCodes = {
    'E': 'England',
    'W': 'Wales',
    'S': 'Scotland',
    'N': 'Northern Ireland',
    'L': 'Channel Islands',
    'M': 'Isle of Man',
    'J': 'Unassigned'}
    
RPCCodes = {
    '1': 'Visual Centre',
    '2': 'General Internal Point',
    '3': 'SW Corner of referenced 100m grid square',
    '4': 'Start of referenced street',
    '5': 'General opint based on postcode unit',
    '9': 'Centre of Contributing Authority area'}

BLPUCodes = {
    '1': 'Under construction',
    '2': 'In use',
    '3': 'Unoccipied vacant or derelict',
    '4': 'Demolished',
    '6': 'Planning permission granted'}

