#!/bin/bash

SA_PASSWORD="yourStrong(!)Password"
DATBASE="db1"

sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -d ${DATBASE} -i scripts/drop_tables.sql