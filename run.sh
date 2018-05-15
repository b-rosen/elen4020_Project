#!/bin/bash

/usr/bin/time ./a.out --in1 "2.17.3/ref_data/1/customer.tbl.1" --in2 "2.17.3/ref_data/1/customer.tbl.499" --out "output.tbl" --hashCol1 3 --hashCol2 3
