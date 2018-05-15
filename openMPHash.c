#define NUM_THREADS 30
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <omp.h>

//This reads in all the lines froma file into an array
void readFile(char* fileName, GPtrArray* fileLines)
{
  FILE *file;
  gchar *line = NULL;
  size_t len = 0;

  file = fopen(fileName, "rb");

  //exit if file isnt found
  if (file == NULL){
    fprintf(stderr, "Error: File %s cannot be found", fileName);
    exit(EXIT_FAILURE);
  }

  //reads each line and adds it to an array
  while (getline(&line, &len, file) != -1)
  {
    g_ptr_array_add(fileLines, strdup(line));
  }
  fclose(file);
  free(line);
}

//creates an array of hash tables from the input filedata
void createHash (GPtrArray* fileLines, GPtrArray* hashTables, int hashColumn)
{
  int i;
  int j;
  char* workingLine;
  char* currentColumnVal;
  int fileLength = fileLines->len;

  //splits this in parallel between all the threads
  #pragma omp parallel for shared(fileLines, hashTables, hashColumn, fileLength) private(workingLine, currentColumnVal, i, j)
  for(i = 0; i < fileLength;i++)
  {
    workingLine = strdup(g_ptr_array_index(fileLines,i));
    currentColumnVal = strtok_r(workingLine, "|", &workingLine);
    //finds the column to be hashed
    for(j = 1; j <= hashColumn; j++)
    {
      if(currentColumnVal == NULL || strcmp(currentColumnVal, "\n") == 0)
      {
        fprintf(stderr, "Error: Column %i out of range\n", hashColumn);
        exit(EXIT_FAILURE);
      }
      currentColumnVal = strtok_r(NULL, "|", &workingLine);
    }
    //hashes using the column as the key and puts the line in as the value
    g_hash_table_insert(g_ptr_array_index(hashTables,omp_get_thread_num()), currentColumnVal, g_slist_append(g_hash_table_lookup(g_ptr_array_index(hashTables,omp_get_thread_num()), currentColumnVal), strdup(g_ptr_array_index(fileLines,i))));

  }
}

//creates the lines for the output file and stores then in an array of arrays.
void createTable(GPtrArray* fileLines, GPtrArray* hashTables, GPtrArray** outFileLines, int hashColumn, int hashHashColumn)
{
  int i;
  int j;
  int k;
  char* workingLine;
  char* currentColumnVal;
  int fileLength = fileLines->len;
  GSList *lines;
  char* currentLine;
  char* currentLineStrip;
  char* hashValueLine;
  char* hashValueContent;
  int colIndex;
  char* tempLine;
  gboolean noClash;

  #pragma omp parallel for shared(fileLines, hashTables, hashColumn, hashHashColumn,fileLength, outFileLines) private(workingLine, currentColumnVal, i, j, k, lines, tempLine, currentLine, currentLineStrip, hashValueLine, hashValueContent, colIndex, noClash)
  for(i = 0; i < fileLength;i++)
  {
    workingLine = strdup(g_ptr_array_index(fileLines,i));
    currentColumnVal = strtok_r(workingLine, "|", &workingLine);
    //finds the column to find the coresponding values in the hash table
    for(j = 1; j <= hashColumn; j++)
    {
      if(currentColumnVal == NULL || strcmp(currentColumnVal, "\n") == 0)
      {
        fprintf(stderr, "Error: Column %i out of range\n", hashColumn);
        exit(EXIT_FAILURE);
      }
      currentColumnVal = strtok_r(NULL, "|", &workingLine);
    }

    currentLine = strdup(g_ptr_array_index(fileLines,i));
    currentLineStrip = strtok_r(currentLine,"\n",&currentLine);
    //loops through all the hash tables and looks for the key values
    for(k = 0; k < NUM_THREADS; k++)
    {
      lines = g_hash_table_lookup(g_ptr_array_index(hashTables,k), currentColumnVal);
      if(lines == NULL)
      {
        continue;
      }

      //creates the lines that will be output by storing the whole file line followed by each column of the hash
      //tables lines except for the column that is the same
      while(lines != NULL)
      {
        noClash = TRUE;
        hashValueLine = strdup(lines->data);
        tempLine = (char *) malloc(strlen(currentLineStrip)+strlen(hashValueLine));
        hashValueContent = strtok_r(hashValueLine,"|",&hashValueLine);
        strcpy(tempLine,currentLineStrip);
        colIndex = 0;

        while(hashValueContent != NULL)
        {
          if(strcmp(hashValueContent,"\n") != 0)
          {
            if (colIndex != hashHashColumn)
            {
              strcat(tempLine,"|");
              strcat(tempLine, hashValueContent);
            }
            else if(strcmp(hashValueContent,currentColumnVal) != 0)
            {
              noClash = FALSE;
              printf("Clash\n");
            }
          }

          hashValueContent = strtok_r(NULL,"|",&hashValueLine);
          colIndex++;
        }
        strcat(tempLine, "\n");
        if(noClash)
          g_ptr_array_add(outFileLines[omp_get_thread_num()], strdup(tempLine));
        lines = lines->next;
        free(tempLine);
      }
    }
  }
}

//prints out the array of output line arrays
void printOutput(GPtrArray** outFileLines, char* fileName)
{
  FILE *outputFile = fopen(fileName, "wb");
  for(int i = 0; i < NUM_THREADS; i++)
  {
    GPtrArray *temp =outFileLines[i];
    int size = temp->len;
    for(int j = 0; j < size; j++)
    {
      fprintf(outputFile, "%s", g_ptr_array_index(temp,j));
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
  int hashCol1;
  int hashCol2;
  for (size_t i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--in1") == 0) {
      file1Name = argv[i + 1];
    } else if (strcmp(argv[i], "--in2") == 0) {
      file2Name = argv[i + 1];
    } else if (strcmp(argv[i], "--out") == 0) {
      outputFileName = argv[i + 1];
    } else if (strcmp(argv[i], "--hashCol1") == 0) {
      hashCol1 = atoi(argv[i + 1]);
    } else if (strcmp(argv[i], "--hashCol2") == 0) {
      hashCol2 = atoi(argv[i + 1]);
    }
  }

  GPtrArray *file2Lines = g_ptr_array_new();
  readFile(file2Name,file2Lines);

GPtrArray *hashTables = g_ptr_array_new();
  for(int i = 0; i < NUM_THREADS; i++)
  {
    g_ptr_array_add(hashTables, g_hash_table_new(g_str_hash, g_str_equal));
  }
  createHash(file2Lines,hashTables,hashCol2);
  g_ptr_array_free(file2Lines,TRUE);

  GPtrArray *file1Lines = g_ptr_array_new();
  readFile(file1Name,file1Lines);

  GPtrArray *outFileLines[NUM_THREADS];
  for(int i = 0; i < NUM_THREADS; i++)
  {
    outFileLines[i] = g_ptr_array_new();
  }

  createTable(file1Lines, hashTables, outFileLines, hashCol1,hashCol2);
  g_ptr_array_free(file1Lines,TRUE);
  for(int i = 0; i < NUM_THREADS; i++)
  {
    g_hash_table_destroy(g_ptr_array_index(hashTables,i));
  }
  g_ptr_array_free(hashTables,FALSE);

  printOutput(outFileLines,outputFileName);
  for(int i = 0; i < NUM_THREADS; i++)
  {
    g_ptr_array_free(outFileLines[i],TRUE);
  }


  return 0;

}
