// #include "mpi.h"
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include "mpi.h"

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

    int colIndex = 0;

    char *secondFileLine = strdup(lines->data);
    char *secondColContent = strtok(secondFileLine, "|\n");

    while (secondColContent != NULL)
    {
      if (colIndex != hashColumn)
      {
        fprintf(outputFile, "%s|", secondColContent);
      }
      secondColContent = strtok(NULL, "|\n");
      colIndex++;
    }
    fprintf(outputFile, "\n");

    free(secondFileLine);
    lines = lines->next;
  }
}

void BuildHashTable(GHashTable* hashTable, char* colContent, char* lineContent, int hashColumn)
{
    g_hash_table_insert(hashTable, colContent, g_slist_append(g_hash_table_lookup(hashTable, colContent), lineContent));
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

  GPtrArray *fileLines = g_ptr_array_new();

  while (getline(&line, &len, file) != -1)
  {
    g_ptr_array_add(fileLines, strdup(line));
  }

  char* fileLine;
  char* colContent;

  for(int i = 0; i < fileLines->len;i++)
  {
    lineContent = g_ptr_array_index(fileLines,i);
    fileLine = strdup(g_ptr_array_index(fileLines,i));
    colContent = strtok(fileLine, "|");
    for (int j = 1; j <= hashColumn; j++)
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


    g_ptr_array_free(fileLines,TRUE);
    free(line);
    // free(lineContent);
    // free(newColContent);
    fclose(file);
}


int main( int argc, char *argv[] )
{
  MPI_Init(&argc, &argv);
  int size, rank;
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  char* file1Name;
  char* file2Name;

  for (size_t i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--in1") == 0) {
      file1Name = argv[i + 1];
    } else if (strcmp(argv[i], "--in2") == 0) {
      file2Name = argv[i + 1];
    } else if (strcmp(argv[i], "--out") == 0) {
      outputFileName = argv[i + 1];
    }
  }

  GHashTable *hashTable = g_hash_table_new(g_str_hash, g_str_equal);
  ReadLines(hashTable, file2Name, 3, BuildHashTable);

  outputFile = fopen(outputFileName, "wb");
  // SearchHashTable(hashTable, "|1|", "49805|Customer#000049805| m,OiXCV1j0ua|15|25-607-134-2554|7363.01|HOUSEHOLD|ding requests engage permanently across the fluffily even excuses. ironic, even packages solve along the fluff|", 3);
  ReadLines(hashTable, file1Name, 3, SearchHashTable);
  fclose(outputFile);

  MPI_Finalize();

  g_hash_table_foreach(hashTable, destroy, NULL);
  g_hash_table_destroy(hashTable);

  return 0;

}
