#define NUM_THREADS 2
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <omp.h>

void hashTest(GHashTable** hashTable)
{
  int i;
  #pragma omp parallel for shared(hashTable) private(i)
  for(i = 0; i < 10; i++)
  {
    g_hash_table_insert(hashTable[omp_get_thread_num()], "columnVal", g_slist_append(g_hash_table_lookup(hashTable[omp_get_thread_num()], "columnVal"), "A value"));
  }

}

int main()
{
  GHashTable *hashTable[NUM_THREADS];
  for(int i = 0; i < NUM_THREADS; i++)
  {
    hashTable[i] = g_hash_table_new(g_str_hash, g_str_equal);
  }

  hashTest(hashTable);

  GSList *lines = g_hash_table_lookup(hashTable[0], "columnVal");
  __mingw_printf("i am printing: %s",lines->data);
}
