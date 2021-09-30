#!/usr/bin/env bash

if [ $# -ne 1 ]
then
   echo "Usage $0 <name of output file>"
   exit
fi

grep -m 1 "model name" /proc/cpuinfo | tee $1
grep "model name" /proc/cpuinfo | wc | tee -a $1

