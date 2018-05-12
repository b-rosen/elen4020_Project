// #include "mpi.h"
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

int main( int argc, char *argv[] )
{
  FILE *file;
  char *line = NULL;
  size_t len = 0;
  int pipeChar = 124;

  file = fopen("2.17.3/ref_data/1/customer.tbl.1", "rb");

  if (file == NULL)
        exit(1);
  GArray *lines = g_array_new(FALSE,FALSE,sizeof(line));
  while (getline(&line, &len, file) != -1) {
    // printf("%s\n", line);
    int startIndex = 0;
    int endIndex = 0;
    int colNum = 0;
    g_array_append_val(lines,line);
    do {
      if (line[endIndex] == pipeChar) {
        if (startIndex != 0 )
        {
          printf("Found in col %i: %.*s\n", colNum, (endIndex - startIndex) - 1, line + startIndex + 1);
        }
        else
        {
          printf("Found in col %i: %.*s\n", colNum, endIndex - startIndex, line + startIndex);
        }
        colNum++;
        startIndex = endIndex;
        // strcpy(input, "");
      }
      else {
        // strcat(input, line[index]);
        // printf("%s\n", &line[index]);
      }
      endIndex++;
    } while(strcmp(&line[endIndex], "\n") != 0);
    printf("\n");
  }

  free(line);
  fclose(file);
  char *testVal = g_array_index(lines,char*,1);
  printf(testVal);
  g_array_free(lines,TRUE);
  return 0;
/*** Select one of the following
    MPI_Init_thread( 0, 0, MPI_THREAD_SINGLE, &provided );
    MPI_Init_thread( 0, 0, MPI_THREAD_FUNNELED, &provided );
    MPI_Init_thread( 0, 0, MPI_THREAD_SERIALIZED, &provided );
    MPI_Init_thread( 0, 0, MPI_THREAD_MULTIPLE, &provided );
***/

    // MPI_Init_thread(0, 0, MPI_THREAD_MULTIPLE, &provided );
    // MPI_Query_thread( &claimed );
    //     printf( "Query thread level= %d  Init_thread level= %d\n", claimed, provided );
    //
    // MPI_Finalize();

}
