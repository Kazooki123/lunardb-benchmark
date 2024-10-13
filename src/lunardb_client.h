#ifndef LUNARDB_CLIENT_H
#define LUNARDB_CLIENT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_COMMAND_LENGTH 1024
#define MAX_RESPONSE_LENGTH 1024

int execute_lunardb_command(const char* command, char* response, size_t response_size) {
    char cli_command[MAX_COMMAND_LENGTH];
    snprintf(cli_command, sizeof(cli_command), "../bin/lunar.exe %s", command);

    FILE* pipe = popen(cli_command, "r");
    if (!pipe) {
        return -1;
    }

    size_t bytes_read = fread(response, 1, response_size - 1, pipe);
    response[bytes_read] = '\0';

    int status = pclose(pipe);
    return (status == 0) ? 0 : -1;
}

int lunardb_set(const char* key, const char* value) {
    char command[MAX_COMMAND_LENGTH];
    char response[MAX_RESPONSE_LENGTH];

    snprintf(command, sizeof(command), "SET %s %s", key, value);
    
    if (execute_lunardb_command(command, response, sizeof(response)) == 0) {
        return (strncmp(response, "OK", 2) == 0) ? 0 : -1;
    }
    return -1;
}

char* lunardb_get(const char* key) {
    char command[MAX_COMMAND_LENGTH];
    static char response[MAX_RESPONSE_LENGTH];

    snprintf(command, sizeof(command), "GET %s", key);
    
    if (execute_lunardb_command(command, response, sizeof(response)) == 0) {
        if (strncmp(response, "(nil)", 5) == 0) {
            return NULL;
        }
        return response;
    }
    return NULL;
}

int lunardb_mset(const char** keys, const char** values, int count) {
    char command[MAX_COMMAND_LENGTH] = "MSET";
    char response[MAX_RESPONSE_LENGTH];

    for (int i = 0; i < count; i++) {
        char pair[256];
        snprintf(pair, sizeof(pair), " %s %s", keys[i], values[i]);
        strcat(command, pair);
    }
    
    if (execute_lunardb_command(command, response, sizeof(response)) == 0) {
        return (strncmp(response, "OK", 2) == 0) ? 0 : -1;
    }
    return -1;
}

char** lunardb_mget(const char** keys, int count) {
    char command[MAX_COMMAND_LENGTH] = "MGET";
    static char response[MAX_RESPONSE_LENGTH];
    static char* results[100];  // Assume max 100 results

    for (int i = 0; i < count; i++) {
        char key[256];
        snprintf(key, sizeof(key), " %s", keys[i]);
        strcat(command, key);
    }
    
    if (execute_lunardb_command(command, response, sizeof(response)) == 0) {
        char* token = strtok(response, "\n");
        int i = 0;
        while (token != NULL && i < 100) {
            results[i++] = token;
            token = strtok(NULL, "\n");
        }
        results[i] = NULL;  
        return results;
    }
    return NULL;
}

#endif // LUNARDB_CLIENT_H