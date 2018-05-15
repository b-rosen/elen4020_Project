# elen4020_Project

#Github repo
https://github.com/b-rosen/elen4020_Project.git

# Compiling Code
After specifying the compiler and file to compile, add:
- $(pkg-config --cflags --libs glib-2.0)
to the end in order to compile with the glib package.

Examples of this can be found in compileOMP.sh, compile.sh and compileMPI.sh.

# Running Code
After compiling openMPHash.c (for OpenMP implementation) or mpiImp.c (for MPI implementation), the resulting executable must be called with the following parameters (order not important):

- --in1 "filePath": The first input table.
- --in2 "filePath": The second input table.
- --out "output.tbl": The name of the file to write the output to.
- --hashCol1 NUM: The index of the hash column of first table to use.
- --hashCol2 NUM: The index of the hash column of the second table to use.

Examples of the above can be found in runMPI.sh and runOMP.sh
