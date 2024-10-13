#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "lunardb_client.h"

#define MAX_KEY_SIZE 20
#define MAX_VALUE_SIZE 100
#define MSET_BATCH_SIZE 100

void run_set_benchmark(int num_requests, int quiet);
void run_get_benchmark(int num_requests, int quiet);
void run_mset_benchmark(int num_requests, int quiet);
void run_mget_benchmark(int num_requests, int quiet);

int main(int argc, char *argv[]) {
    int num_requests = 100000; 
    int quiet = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-q") == 0) {
            quiet = 1;
        } else if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
            num_requests = atoi(argv[i + 1]);
            i++;
        }
    }

    run_set_benchmark(num_requests, quiet);
    run_get_benchmark(num_requests, quiet);
    run_mset_benchmark(num_requests, quiet);
    run_mget_benchmark(num_requests, quiet);

    return 0;
}

void run_set_benchmark(int num_requests, int quiet) {
    char key[MAX_KEY_SIZE];
    char value[MAX_VALUE_SIZE];
    clock_t start, end;
    double cpu_time_used;

    start = clock();

    for (int i = 0; i < num_requests; i++) {
        snprintf(key, MAX_KEY_SIZE, "key:%d", i);
        snprintf(value, MAX_VALUE_SIZE, "value:%d", i);
        lunardb_set(key, value);
    }

    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;

    if (!quiet) {
        printf("SET: %d requests completed in %.2f seconds\n", num_requests, cpu_time_used);
        printf("%.2f requests per second\n", num_requests / cpu_time_used);
    }
}

void run_get_benchmark(int num_requests, int quiet) {
    char key[MAX_KEY_SIZE];
    clock_t start, end;
    double cpu_time_used;

    start = clock();

    for (int i = 0; i < num_requests; i++) {
        snprintf(key, MAX_KEY_SIZE, "key:%d", i % (num_requests / 2)); // Reuse keys to ensure they exist
        lunardb_get(key);
    }

    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;

    if (!quiet) {
        printf("GET: %d requests completed in %.2f seconds\n", num_requests, cpu_time_used);
        printf("%.2f requests per second\n", num_requests / cpu_time_used);
    }
}

void run_mset_benchmark(int num_requests, int quiet) {
    char keys[MSET_BATCH_SIZE][MAX_KEY_SIZE];
    char values[MSET_BATCH_SIZE][MAX_VALUE_SIZE];
    const char* key_ptrs[MSET_BATCH_SIZE];
    const char* value_ptrs[MSET_BATCH_SIZE];
    clock_t start, end;
    double cpu_time_used;

    for (int i = 0; i < MSET_BATCH_SIZE; i++) {
        key_ptrs[i] = keys[i];
        value_ptrs[i] = values[i];
    }

    start = clock();

    for (int i = 0; i < num_requests; i += MSET_BATCH_SIZE) {
        for (int j = 0; j < MSET_BATCH_SIZE; j++) {
            snprintf(keys[j], MAX_KEY_SIZE, "mkey:%d", i + j);
            snprintf(values[j], MAX_VALUE_SIZE, "mvalue:%d", i + j);
        }
        lunardb_mset(key_ptrs, value_ptrs, MSET_BATCH_SIZE);
    }

    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;

    if (!quiet) {
        printf("MSET: %d requests completed in %.2f seconds\n", num_requests, cpu_time_used);
        printf("%.2f requests per second\n", num_requests / cpu_time_used);
    }
}

void run_mget_benchmark(int num_requests, int quiet) {
    char keys[MSET_BATCH_SIZE][MAX_KEY_SIZE];
    const char* key_ptrs[MSET_BATCH_SIZE];
    clock_t start, end;
    double cpu_time_used;

    for (int i = 0; i < MSET_BATCH_SIZE; i++) {
        key_ptrs[i] = keys[i];
    }

    start = clock();

    for (int i = 0; i < num_requests; i += MSET_BATCH_SIZE) {
        for (int j = 0; j < MSET_BATCH_SIZE; j++) {
            snprintf(keys[j], MAX_KEY_SIZE, "mkey:%d", (i + j) % (num_requests / 2)); // Reuse keys to ensure they exist
        }
        lunardb_mget(key_ptrs, MSET_BATCH_SIZE);
    }

    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;

    if (!quiet) {
        printf("MGET: %d requests completed in %.2f seconds\n", num_requests, cpu_time_used);
        printf("%.2f requests per second\n", num_requests / cpu_time_used);
    }
}
