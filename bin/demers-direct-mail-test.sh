#!/bin/bash

BROADCAST_MODULE=demers_direct_mail bin/counterexample-find.sh 

RETVAL=$?

if [ $RETVAL -ne 0 ]; then
    exit 0
fi

exit 1