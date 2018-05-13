// #include "mpi.h"
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

void HashFile(char* fileName, int hashColumn)
{
  FILE *file;
  gchar *line = NULL;
  size_t len = 0;
  int pipeChar = 124;

  file = fopen(fileName, "rb");

  if (file == NULL)
        exit(1);
  GPtrArray *lines;
  // GHashTable *hashTable = g_hash_table_new(g_str_hash, g_str_equal);
  lines = g_ptr_array_new();
  while (getline(&line, &len, file) != -1) {
    // printf("%s\n", line);
    int startIndex = 0;
    int endIndex = 0;
    int colNum = 0;
    g_ptr_array_add(lines, (gpointer)g_strdup(line));

    char *colContent = strtok(line, "|");
    for (int i = 1; i <= hashColumn; i++)
    {
      colContent = strtok(NULL, "|");
      if (colContent == NULL || strcmp(colContent, "\n") == 0)
      {
        fprintf(stderr, "Error: Column %i out of range\n", hashColumn);
        exit(EXIT_FAILURE);
      }
    }
    // We are at column to hash
    printf("%s\n", colContent);
  }
  free(line);
  fclose(file);
}

int main( int argc, char *argv[] )
{

  char* fileName = "2.17.3/ref_data/1/customer.tbl.1";
  HashFile(fileName, 7);
  // g_print("Hello %s\n", g_ptr_array_index(lines,0));
  // g_ptr_array_free(lines,TRUE);
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
