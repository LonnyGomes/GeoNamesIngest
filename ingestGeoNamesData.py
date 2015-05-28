#!/usr/bin/python
# -*- coding: utf_8 -*-
# coding: utf-8
import csv
import sqlite3
import sys
from datetime import datetime

csv.field_size_limit(1000000000)

db_name='geonames.db'

geo_fields=('RC','UFI','UNI','LAT','LONG','DMS_LAT','DMS_LONG','MGRS','JOG',
            'FC','DSG','PC','CC1','ADM1','POP','ELEV','CC2','NT','LC',
            'SHORT_FORM','GENERIC','SORT_NAME_RO','FULL_NAME_RO',
            'FULL_NAME_ND_RO','SORT_NAME_RG','FULL_NAME_RG','FULL_NAME_ND_RG',
            'NOTE','MODIFY_DATE','DISPLAY','NAME_RANK','NAME_LINK','TRANSL_CD',
            'NM_MODIFY_DATE')

lite_drop = "DROP TABLE IF EXISTS geonames"
geonames_tbl = "geonames"
lite_schema = \
"CREATE TABLE geonames(\
    geoname_id INTEGER PRIMARY KEY AUTOINCREMENT, RC NUMERIC, lat NUMERIC,\
    long NUMERIC, dms_lat TEXT, dms_long TEXT, mgrs TEXT, fc TEXT, dsg TEXT,\
    pc TEXT, cc1 TEXT, adm1pop TEXT, elev TEXT, short_form TEXT, generic TEXT,\
    sort_name_ro TEXT, full_name_ro TEXT, full_name_nd_ro TEXT,\
    sort_name_rg TEXT, full_name_rg TEXT, full_name_nd_rg TEXT, note TEXT,\
    modify_date TEXT);"

lite_insert_sql = "INSERT INTO geonames (\
    rc, lat,long,dms_lat,dms_long,mgrs,fc,dsg,pc,cc1,adm1pop,elev,short_form,\
    generic,sort_name_ro,full_name_ro,full_name_nd_ro,sort_name_rg,\
    full_name_rg,full_name_nd_rg,note,modify_date) \
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

def buildIndex(db_curs, table_name, col_name):
    sql = "CREATE INDEX IF NOT EXISTS '%s_idx' on '%s' ('%s' ASC)" % (col_name, table_name, col_name)
    db_curs.execute(sql)

def genRowTuple(row, col_range):
    listData = []

    #build a list of column data interpreting the values as utf8
    for idx in col_range:
        listData.append(row[idx].decode('utf8'))

    #convert list to tuple and return
    return tuple(listData)


if len(sys.argv) != 2:
    print 'Please supply Geo Names file to parse'
    sys.exit(1)

inputFile = sys.argv[1]

print "Connecting to sqlite db ..."
conn = sqlite3.connect(db_name)
curs = conn.cursor()
curs.execute(lite_drop)
curs.execute(lite_schema)

print "Processing dump ..."
start_time = datetime.now()
rowCount = 0
with open(inputFile, 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    for row in reader:

        #ignore first line
        if rowCount == 0:
            rowCount +=1
            continue

        if len(row) != len(geo_fields):
            print "Invalid column count:%d" % len(row)
        else:
            curData = genRowTuple(row, [0, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27])
            curs.execute(lite_insert_sql, curData)

        rowCount +=1
    
print "Completed database import ..."
print "Building indexes ..."
buildIndex(curs,geonames_tbl, 'SORT_NAME_RO')

#save end time
end_time = datetime.now()

print "Row count: %d" % rowCount
print "Total time: %s" % (end_time - start_time)

conn.commit()
conn.close()


