#!/bin/bash 


DATBASE="db1"
SA_PASSWORD="yourStrong(!)Password"

sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -d ${DATBASE}