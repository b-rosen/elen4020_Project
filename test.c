// #include "mpi.h"
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

char *outputFileName;
FILE *outputFile;

void destroy(gpointer key, gpointer value, gpointer data) {
 g_slist_free(value);
}

void SearchHashTable(GHashTable* hashTable, char* colContent, char* lineContent, int hashColumn)
{
  GSList *lines = g_hash_table_lookup(hashTable, colContent);
  if (lines == NULL)
    return;

  while (lines != NULL)
  {
    fprintf(outputFile, "%s", strtok(lineContent, "\n"));
    // ADD check for column being equal
    // Remove column from print
    int colIndex = 0;
    // char *secondFileLine = NULL;
    char *secondFileLine = strdup(lines->data);
    char *secondColContent = strtok(secondFileLine, "|\n");
    // gboolean correctCol = TRUE;
    while (secondColContent != NULL)
    {
      if (colIndex != hashColumn)
      {
        fprintf(outputFile, "%s|", secondColContent);
      }
      // strcat(secondFileLine, secondColContent);
      // strcat(secondFileLine, "|");

      secondColContent = strtok(NULL, "|\n");
      colIndex++;
    }
    fprintf(outputFile, "\n");
    // if (correctCol)
    //   printf("%s\n\n", secondFileLine);
    // printf("\n");
    free(secondFileLine);
    lines = lines->next;
  }
  // g_slist_foreach(g_hash_table_lookup(hashTable, colContent), (GFunc)SaveToFile, colContent);
}

void BuildHashTable(GHashTable* hashTable, char* colContent, char* lineContent, int hashColumn)
{
    // We are at column to hash
    // printf("%s\n", colContent);
    // printf("%s\n", line);
    // char* newColContent = strdup(colContent);
    /*gboolean test = */g_hash_table_insert(hashTable, colContent, g_slist_append(g_hash_table_lookup(hashTable, colContent), lineContent));
    // free(newColContent);
    // if (!test) {
    //   printf("Collision\n");
    // }
  // return hashTable;
}

void ReadLines(GHashTable* hashTable, char* fileName, int hashColumn, void (*action)(GHashTable*, char*, char*, int))
{
  FILE *file;
  gchar *line = NULL;
  size_t len = 0;

  file = fopen(fileName, "rb");

  if (file == NULL){
    fprintf(stderr, "Error: File %s cannot be found", fileName);
    exit(EXIT_FAILURE);
  }

  char* lineContent;
  char* newColContent;

  while (getline(&line, &len, file) != -1)
  {
    // printf("%s\n", line);
    lineContent = strdup(line);
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
    newColContent = strdup(colContent);
    (*action)(hashTable, newColContent, lineContent, hashColumn);
  }

    free(line);
    free(lineContent);
    free(newColContent);
    fclose(file);
}


int main( int argc, char *argv[] )
{
  char* file1Name;
  char* file2Name;
//Use argv to get file names
  for (size_t i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--in1") == 0) {
      file1Name = argv[i + 1];
    } else if (strcmp(argv[i], "--in2") == 0) {
      file2Name = argv[i + 1];
    } else if (strcmp(argv[i], "--out") == 0) {
      outputFileName = argv[i + 1];
    }
  }
  // char* file1Name = "2.17.3/ref_data/1/customer.tbl.1";
  // char* file2Name = "2.17.3/ref_data/1/customer.tbl.499";
  // outputFileName = "output.tbl";
  GHashTable *hashTable = g_hash_table_new(g_str_hash, g_str_equal);
  ReadLines(hashTable, file2Name, 3, BuildHashTable);
  //Add stuff to write to file instead
  outputFile = fopen(outputFileName, "wb");
  ReadLines(hashTable, file1Name, 3, SearchHashTable);
  fclose(outputFile);

  g_hash_table_foreach(hashTable, destroy, NULL);
  g_hash_table_destroy(hashTable);

  // g_hash_table_foreach(hashTable, print, NULL);
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
