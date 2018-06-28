#!/bin/bash
#!/bin/bash

DATAFILE=$1
TABLE_NAME="dbo.record_token"
DATBASE="db1"

if [ -z ${DATAFILE} ]
then
    DATAFILE="/db2/github/westpac_tools/db/data/token_3m.csv"
fi
echo "Load file from $DATAFILE"


SA_PASSWORD="yourStrong(!)Password"
#DATAFILE="/db2/github/westpac_tools/db/data/a1.csv"
FMTFILE="/db2/github/westpac_tools/db/data/record.fmt"

echo "bcp ${TABLE_NAME} in \"${DATAFILE}\" -f \"${FMTFILE}\" -S localhost -U sa -P \"${SA_PASSWORD}\" -d ${DATBASE} -b 2000 -e error.log -h"

bcp ${TABLE_NAME} in "${DATAFILE}" -c -S localhost -U sa -P "${SA_PASSWORD}" -d ${DATBASE} -b 2000 -e error.log -t "\t" -r "\r\n"  -E

echo $?

