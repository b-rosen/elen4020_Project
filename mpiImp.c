// #include "mpi.h"
#define _GNU_SOURCE
#define MASTER 0
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include "mpi.h"

char *outputFileName;
// FILE *outputFile;

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
    // fprintf(outputFile, "%s", strtok(lineContent, "\n"));

    int colIndex = 0;

    char *secondFileLine = strdup(lines->data);
    char *secondColContent = strtok(secondFileLine, "|\n");

    while (secondColContent != NULL)
    {
      if (colIndex != hashColumn)
      {
        // fprintf(outputFile, "%s|", secondColContent);
      }
      secondColContent = strtok(NULL, "|\n");
      colIndex++;
    }
    // fprintf(outputFile, "\n");

    free(secondFileLine);
    lines = lines->next;
  }
}

void BuildHashTable1(GHashTable* hashTable, char* colContent, char* lineContent, int hashColumn)
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
  // GArray lineLengths = g_array_new();

  while (getline(&line, &len, file) != -1)
  {
    g_ptr_array_add(fileLines, strdup(line));
    // g_array_add(fileLines, len);
  }

  // MPI stuff
  // if (rank == 0)
  // {
  //   MPI_Scatter(fileLines, )
  // }

  // char* fileLine;
  // char* colContent;
  //
  // for(int i = 0; i < fileLines->len;i++)
  // {
  //   lineContent = g_ptr_array_index(fileLines,i);
  //   fileLine = strdup(g_ptr_array_index(fileLines,i));
  //   colContent = strtok(fileLine, "|");
  //   for (int j = 1; j <= hashColumn; j++)
  //   {
  //     colContent = strtok(NULL, "|");
  //     if (colContent == NULL || strcmp(colContent, "\n") == 0)
  //     {
  //       fprintf(stderr, "Error: Column %i out of range\n", hashColumn);
  //       exit(EXIT_FAILURE);
  //     }
  //   }
  //   newColContent = strdup(colContent);
  //   (*action)(hashTable, newColContent, lineContent, hashColumn);
  // }


    g_ptr_array_free(fileLines,TRUE);
    free(line);
    // free(lineContent);
    // free(newColContent);
    fclose(file);
}

char* GetColumnContents(char* line, int column)
{
  char* colContent;

  colContent = strtok(line, "|");
  for (int j = 1; j <= column; j++)
  {
    colContent = strtok(NULL, "|");
    if (colContent == NULL || strcmp(colContent, "\n") == 0)
    {
      fprintf(stderr, "Error: Column %i out of range\n", column);
      exit(EXIT_FAILURE);
    }
  }
  return colContent;
}

void PrintLikeEntries(char* outputFileName, GHashTable* file2_HT, GPtrArray* file1_lines, int column1, int column2)
{
  FILE* outputFile = fopen(outputFileName, "wb");
  GList* file2_lines;
  for (size_t i = 0; i < file1_lines->len; i++)
  {
    char* file1_line = g_ptr_array_index(file1_lines, i);
    char* file1_lineCopy = strdup(file1_line);

    char* colContent = GetColumnContents(file1_lineCopy, column1);
    file2_lines = g_hash_table_lookup(file2_HT, colContent);

    while (file2_lines != NULL)
    {
      fprintf(outputFile, "%s", strtok(file1_line, "\n"));

      int colIndex = 0;

      char *secondFileLine = strdup(file2_lines->data);
      char *secondColContent = strtok(secondFileLine, "|\n");

      while (secondColContent != NULL)
      {
        if (colIndex != column2)
        {
          fprintf(outputFile, "%s|", secondColContent);
        }
        secondColContent = strtok(NULL, "|\n");
        colIndex++;
      }
      fprintf(outputFile, "\n");

      free(secondFileLine);
      file2_lines = file2_lines->next;
    }
  }

  fclose(outputFile);
}

GHashTable* BuildHashTable(GPtrArray* lineBuffer, int hashColumn)
{
  GHashTable *hashTable = g_hash_table_new(g_str_hash, g_str_equal);
  char* fileLineCopy;
  char* fileLineContent;
  for(int i = 0; i < lineBuffer->len; i++)
  {
    fileLineCopy = strdup(g_ptr_array_index(lineBuffer,i));

    char* colContent = GetColumnContents(fileLineCopy, hashColumn);
    fileLineContent = strdup(g_ptr_array_index(lineBuffer, i));

    g_hash_table_insert(hashTable, colContent, g_slist_append(g_hash_table_lookup(hashTable, colContent), fileLineContent));

    // free(fileLineCopy);
  }
  free(fileLineContent);
  return hashTable;
}

GPtrArray* ReadFile(char* fileName)
{
  FILE *file;
  gchar *line = NULL;
  size_t len = 0;

  file = fopen(fileName, "rb");

  if (file == NULL)
  {
    fprintf(stderr, "Error: File %s cannot be found", fileName);
    exit(EXIT_FAILURE);
  }

  GPtrArray *fileLines = g_ptr_array_new();
  // GArray lineLengths = g_array_new();

  while (getline(&line, &len, file) != -1)
  {
    g_ptr_array_add(fileLines, strdup(line));
    // g_array_add(fileLines, len);
  }

  fclose(file);
  return fileLines;
}

GPtrArray* ReadChunk(char* chunk)
{
  gchar *line = NULL;
  size_t len = 0;

  GPtrArray *fileLines = g_ptr_array_new();

  char *lineContent = strtok(chunk, "\n");

  while (lineContent != NULL)
  {
    g_ptr_array_add(fileLines, strdup(lineContent));
    lineContent = strtok(NULL, "\n");
  }

  // while (getline(&line, &len, file) != -1)
  // {
  //   g_ptr_array_add(fileLines, strdup(line));
  //   // g_array_add(fileLines, len);
  // }

  return fileLines;
}


GArray* ScanFile(FILE* file)
{
  if (file == NULL)
  {
    fprintf(stderr, "Error: File cannot be found");
    exit(EXIT_FAILURE);
  }

  GArray* lineLengths = g_array_new(FALSE, FALSE, sizeof(int));
  int c;
  int charCount = 0;

  while(c != EOF)
  {
    while(c != '\n')
    {
      c = fgetc(file);
      charCount++;
    }
    g_array_append_val(lineLengths, charCount);
    c = fgetc(file);
    charCount = 1;
  }

  return lineLengths;
}

char* GetFileChunk(FILE* file, GArray* linesInfo, int lineStartIndex, int numLines, int* len)
{
  if (file == NULL)
  {
    fprintf(stderr, "Error: File cannot be found");
    exit(EXIT_FAILURE);
  }

  int totalSize = 0;
  for (size_t i = lineStartIndex; i < lineStartIndex + numLines; i++)
  {
    totalSize += g_array_index(linesInfo, int, i);
  }
  char* lineChunk = (char*) malloc(sizeof(char)*totalSize);
  int actualRead = fread(lineChunk, totalSize, 1, file);

  if (actualRead != 1) {
    fprintf(stderr, "Error in reading chunk\n");
  }

  *len = totalSize;

  return lineChunk;
}

int main( int argc, char *argv[] )
{
  int numNodes, rank;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numNodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  //NB: Remove
  // rank = 0;
  // numNodes = 2;

  char* file1_name;
  char* file2_name;

  int hashCol1;
  int hashCol2;

  for (size_t i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--in1") == 0) {
      file1_name = argv[i + 1];
    } else if (strcmp(argv[i], "--in2") == 0) {
      file2_name = argv[i + 1];
    } else if (strcmp(argv[i], "--out") == 0) {
      outputFileName = argv[i + 1];
    } else if (strcmp(argv[i], "--hashCol1") == 0) {
      hashCol1 = atoi(argv[i + 1]);
    } else if (strcmp(argv[i], "--hashCol2") == 0) {
      hashCol2 = atoi(argv[i + 1]);
    }
  }

  if (rank == MASTER)
  {
    FILE* file2 = fopen(file2_name, "rb");

    GArray *file2_lineInfo = ScanFile(file2);
    // printf("%i\n", g_array_index(file1_lineInfo, int, 0));
    fseek(file2, 0, SEEK_SET);
    int linesPerNode = file2_lineInfo->len / (numNodes - 1);
    int extraLines = file2_lineInfo->len % (numNodes - 1);
    char* chunk;
    int linesToRead = linesPerNode;
    int chunkLen;
    for (size_t nodeRank = 1; nodeRank < numNodes; nodeRank++)
    {
      if (nodeRank == numNodes - 1)
        linesToRead += extraLines;

      chunk = GetFileChunk(file2, file2_lineInfo, (nodeRank-1)*linesPerNode, linesToRead, &chunkLen);
      MPI_Send (&chunk, chunkLen, MPI_CHAR, nodeRank, 0, MPI_COMM_WORLD);
      printf("Sent to: %zu\n", nodeRank);

      // // Convert chunk to line array
      // GPtrArray* file2_lines = ReadChunk(chunk);
      // // Convert line array to table
      // GHashTable* file2_hashTable = BuildHashTable(file2_lines, hashCol2);
      //
      // GPtrArray *file1_lines = ReadFile(file1_name);
      //
      // PrintLikeEntries(outputFileName, file2_hashTable, file1_lines, hashCol1, hashCol2);
      //
      // g_ptr_array_free(file1_lines, TRUE);
      // g_ptr_array_free(file2_lines, TRUE);
      //
      // g_hash_table_foreach(file2_hashTable, destroy, NULL);
      // g_hash_table_destroy(file2_hashTable);

      free(chunk);
    }

    fclose(file2);
    // char* chunk = GetFileChunk(file1, file1_lineInfo, 0, 2);
    // printf("%s\n", chunk);
    // chunk = GetFileChunk(file1, file1_lineInfo, 2, 2);
    // printf("%s", chunk);

  }
  else
  {
    int chunkSize;
    MPI_Status status;
    MPI_Probe(MASTER, 0, MPI_COMM_WORLD, &status);

    MPI_Get_count(&status, MPI_INT, &chunkSize);
    printf("%i\n", chunkSize);

    char* chunk = (char*) malloc(sizeof(char)*chunkSize);

    MPI_Recv(&chunk, chunkSize, MPI_CHAR, MASTER, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    // printf("%s\n", chunk);

    free(chunk);
  }

  // GHashTable *hashTable = g_hash_table_new(g_str_hash, g_str_equal);
  // ReadLines(hashTable, file2Name, 3, BuildHashTable);
  //
  // outputFile = fopen(outputFileName, "wb");
  // // SearchHashTable(hashTable, "|1|", "49805|Customer#000049805| m,OiXCV1j0ua|15|25-607-134-2554|7363.01|HOUSEHOLD|ding requests engage permanently across the fluffily even excuses. ironic, even packages solve along the fluff|", 3);
  // ReadLines(hashTable, file1Name, 3, SearchHashTable);
  // fclose(outputFile);
  //
  MPI_Finalize();
  //

  // int hashColumn = 3;
  // GPtrArray *file2_lines = ReadFile(file2_name);
  // GHashTable *file2_HashTable = BuildHashTable(file2_lines, hashCol2);
  //
  // GPtrArray *file1_lines = ReadFile(file1_name);
  // PrintLikeEntries(outputFileName, file2_HashTable, file1_lines, hashCol1, hashCol2);
  //
  // g_ptr_array_free(file1_lines, TRUE);
  // g_ptr_array_free(file2_lines, TRUE);
  //
  // g_hash_table_foreach(file2_HashTable, destroy, NULL);
  // g_hash_table_destroy(file2_HashTable);

  return 0;

}
