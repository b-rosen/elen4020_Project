// #include "mpi.h"
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

char *outputFileName = "output.tbl";
FILE *outputFile;

void printVal(gpointer data)
{
  printf("%s, ", data);
}

void print(gpointer key, gpointer value, gpointer data)
{
 printf("Here are some cities in %s: ", key);
 g_slist_foreach((GSList*)value, (GFunc)printVal, NULL);
 printf("\n\n");
}


void SearchHashTable(GHashTable* hashTable, char* colContent, char* lineContent, int hashColumn)
{
  GSList *lines = g_hash_table_lookup(hashTable, colContent);
  if (lines == NULL)
    return;

  while (lines->next != NULL)
  {
    printf("%s", strtok(lineContent, "\n"));
    // ADD check for column being equal
    // Remove column from print
    int colIndex = 0;
    // char *secondFileLine = NULL;
    char *secondFileLine = "hello";
    char *secondColContent = strtok(secondFileLine, "|");
    gboolean correctCol = TRUE;
    while (secondColContent != NULL)
    {
      if (colIndex == hashColumn)
      {
        if (strcmp(colContent, secondColContent) == 0)
        {
          secondColContent = strtok(NULL, "|");
          colIndex++;
          continue;
        }
        else {
          correctCol = FALSE;
          break;
        }
      }
      // strcat(secondFileLine, secondColContent);
      // strcat(secondFileLine, "|");

      secondColContent = strtok(NULL, "|");
      colIndex++;
    }
    if (correctCol)
      printf("%s\n\n", secondFileLine);

    lines = lines->next;
  }
  // g_slist_foreach(g_hash_table_lookup(hashTable, colContent), (GFunc)SaveToFile, colContent);
}

void BuildHashTable(GHashTable* hashTable, char* colContent, char* lineContent, int hashColumn)
{
    // We are at column to hash
    // printf("%s\n", colContent);
    // printf("%s\n", line);
    /*gboolean test = */g_hash_table_insert(hashTable, strdup(colContent), g_slist_append(g_hash_table_lookup(hashTable, strdup(colContent)), lineContent));
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

  while (getline(&line, &len, file) != -1)
  {
    // printf("%s\n", line);
    char* lineContent = strdup(line);
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
    (*action)(hashTable, colContent, lineContent, hashColumn);
  }

    free(line);
    fclose(file);
}


int main( int argc, char *argv[] )
{
  //TODO: use argv to get file names
  char* file1Name = "2.17.3/ref_data/1/customer.tbl.1";
  char* file2Name = "2.17.3/ref_data/1/customer.tbl.499";
  GHashTable *hashTable = g_hash_table_new(g_str_hash, g_str_equal);
  ReadLines(hashTable, file2Name, 3, BuildHashTable);
  //Add stuff to write to file instead
  ReadLines(hashTable, file1Name, 3, SearchHashTable);

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
