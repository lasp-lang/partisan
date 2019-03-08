#!/bin/bash

BROADCAST_MODULE=demers_anti_entropy bin/counterexample-find.sh 

RETVAL=$?

if [ $RETVAL -ne 0 ]; then
    exit 0
fi

exit 1