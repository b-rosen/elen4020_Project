#!/bin/bash

mpicc mpiImp.c $(pkg-config --cflags --libs glib-2.0)
