#!/bin/bash

gcc-7 -fopenmp openMPImp.c $(pkg-config --cflags --libs glib-2.0)
