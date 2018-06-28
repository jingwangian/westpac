#!/bin/bash

# usage: ./dump_db t1 t1.csv 

TABLENAME=$1
DATAFILE=$2
DATBASE="db1"
SA_PASSWORD="yourStrong(!)Password"
FMTFILE="/db2/github/westpac_tools/db/data/record.fmt"

echo "Dump dbo.${TABLENAME} to file $DATAFILE"

echo "bcp dbo.${TABLENAME} out \"${DATAFILE}\" -c -S localhost -U sa -P \"${SA_PASSWORD}\" -d ${DATBASE} -e error.log -t \"\t\" -r \"\r\n\""

bcp dbo.${TABLENAME} out "${DATAFILE}" -c -S localhost -U sa -P "${SA_PASSWORD}" -d ${DATBASE} -e error.log -t "\t" -r "\r\n"

echo $?

