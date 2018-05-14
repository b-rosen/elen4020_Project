#define NUM_THREADS 2
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <omp.h>

void readFile(char* fileName, GPtrArray* fileLines)
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
    g_ptr_array_add(fileLines, strdup(line));
  }
  fclose(file);
  free(line);
}

void createHash (GPtrArray* fileLines, GHashTable** hashTables, int hashColumn)
{
  int i;
  int j;
  char* workingLine;
  char* columnVal;
  char* currentColumnVal;
  int fileLength = fileLines->len;

  #pragma omp parallel for shared(fileLines, hashTables, hashColumn, fileLength) private(workingLine, columnVal, currentColumnVal, i, j)
  for(i = 0; i < fileLength;i++)
  {
    workingLine = strdup(g_ptr_array_index(fileLines,i));
    currentColumnVal = strtok(workingLine, "|");
    for(j = 1; j <= hashColumn; j++)
    {
      if(currentColumnVal == NULL || strcmp(currentColumnVal, "\n") == 0)
      {
        fprintf(stderr, "Error: Column %i out of range\n", hashColumn);
        exit(EXIT_FAILURE);
      }
      columnVal = strdup(currentColumnVal);
      currentColumnVal = strtok(NULL, "|");
    }
    g_hash_table_insert(hashTables[omp_get_thread_num()], columnVal, g_slist_append(g_hash_table_lookup(hashTables[omp_get_thread_num()], columnVal), strdup(g_ptr_array_index(fileLines,i))));
  }
}

void createTable(GPtrArray* fileLines, GHashTable** hashTables, GPtrArray** outFileLines, int hashColumn)
{
  int i;
  int j;
  int k;
  char* workingLine;
  char* columnVal;
  char* currentColumnVal;
  int fileLength = fileLines->len;
  GSList *lines;
  char* currentLine;
  char* tempLine;

  #pragma omp parallel for shared(fileLines, hashTables, hashColumn, fileLength) private(workingLine, columnVal, currentColumnVal, i, j, k, lines, tempLine, currentLine)
  for(i = 0; i < fileLength;i++)
  {
    workingLine = strdup(g_ptr_array_index(fileLines,i));
    currentColumnVal = strtok(workingLine, "|");
    for(j = 1; j <= hashColumn; j++)
    {
      if(currentColumnVal == NULL || strcmp(currentColumnVal, "\n") == 0)
      {
        fprintf(stderr, "Error: Column %i out of range\n", hashColumn);
        exit(EXIT_FAILURE);
      }
      columnVal = strdup(currentColumnVal);
      currentColumnVal = strtok(NULL, "|");
    }
    for(k = 0; k < NUM_THREADS; k++)
    {
      lines = g_hash_table_lookup(hashTables[k], columnVal);
      if(lines == NULL)
      {
        continue;
      }

      while(lines != NULL)
      {
        currentLine = strdup(g_ptr_array_index(fileLines,i));
        tempLine = strtok(currentLine, "\n");
        g_ptr_array_add(outFileLines[omp_get_thread_num()], tempLine);
        lines = lines->next;
      }
    }

  }
}

void printOutput(GPtrArray** outFileLines, char* fileName)
{
  FILE *outputFile = fopen(fileName, "wb");
  for(int i = 0; i < NUM_THREADS; i++)
  {
    for(int j = 0; j < outFileLines[i]->len; j++)
    {
      fprintf(outputFile, g_ptr_array_index(outFileLines[i],j));
    }
  }
  fclose(outputFile);
}

int main( int argc, char *argv[] )
{
  omp_set_num_threads(NUM_THREADS);
  char* file1Name;
  char* file2Name;
  char* outputFileName;

  for (size_t i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--in1") == 0) {
      file1Name = argv[i + 1];
    } else if (strcmp(argv[i], "--in2") == 0) {
      file2Name = argv[i + 1];
    } else if (strcmp(argv[i], "--out") == 0) {
      outputFileName = argv[i + 1];
    }
  }

  GPtrArray *file1Lines = g_ptr_array_new();
  readFile(file1Name,file1Lines);

  GHashTable *hashTable[NUM_THREADS];
  for(int i = 0; i < NUM_THREADS; i++)
  {
    hashTable[i] = g_hash_table_new(g_str_hash, g_str_equal);
  }

  createHash(file1Lines,hashTable,3);
  g_ptr_array_free(file1Lines,TRUE);

  GPtrArray *file2Lines = g_ptr_array_new();
  readFile(file2Name,file2Lines);

  GPtrArray *outFileLines[NUM_THREADS];
  for(int i = 0; i < NUM_THREADS; i++)
  {
    outFileLines[i] = g_ptr_array_new();
  }

  createTable(file2Lines, hashTable, outFileLines, 3);
  g_ptr_array_free(file2Lines,TRUE);

  printOutput(outFileLines,outputFileName);
  for(int i = 0; i < NUM_THREADS; i++)
  {
    g_ptr_array_free(outFileLines[i],TRUE);
  }


  return 0;

}
