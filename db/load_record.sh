#!/bin/bash

DATAFILE=$1

DATBASE="db1"

if [ -z ${DATAFILE} ]
then
    DATAFILE="/db2/github/westpac_tools/db/data/record_1m.csv"
fi
echo "Load file from $DATAFILE"


SA_PASSWORD="yourStrong(!)Password"
#DATAFILE="/db2/github/westpac_tools/db/data/a1.csv"
FMTFILE="/db2/github/westpac_tools/db/data/record.fmt"

echo "bcp record in \"${DATAFILE}\" -f \"${FMTFILE}\" -S localhost -U sa -P \"${SA_PASSWORD}\" -d ${DATBASE} -b 2000 -e error.log -h"

#bcp record in "${DATAFILE}" -f "${FMTFILE}" -S localhost -U sa -P "${SA_PASSWORD}" -d master

#bcp record in "${DATAFILE}" -f "${FMTFILE}" -S localhost -U sa -P "${SA_PASSWORD}" -d master -b 2000 -e error.log -h

bcp dbo.record in "${DATAFILE}" -c -S localhost -U sa -P "${SA_PASSWORD}" -d ${DATBASE} -b 2000 -e error.log -t "\t" -r "\r\n"  -E

echo $?

