#!/bin/bash

# run netstat and open connection on port 9999
echo "[Type record and hit Enter, e.g. 'C1,P234']"
nc -lk 9999
