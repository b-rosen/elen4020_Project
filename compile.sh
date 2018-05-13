#!/bin/bash

gcc test.c $(pkg-config --cflags --libs glib-2.0)
