#!/bin/bash

gcc-7 -fopenmp openMPHash.c $(pkg-config --cflags --libs glib-2.0)
