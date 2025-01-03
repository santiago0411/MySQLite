#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "array.h"

typedef struct {
    char* data;
    size_t count;
    size_t capacity;
} StringBuilder;

void read_input(StringBuilder* sb)
{
    fflush(stdout);
    char buffer[1024*10+1];
    if (!fgets(buffer, sizeof buffer, stdin)) {
        fprintf(stderr, "Error reading input\n");
        exit(EXIT_FAILURE);
    }
    // Ignore \n at the end
    size_t length = strlen(buffer);
    buffer[length - 1] = 0;
    ARRAY_APPEND_MANY(sb, buffer, length);
}

int main(void)
{
    StringBuilder sb = {0};
    for (;;) {
        printf("db > ");
        read_input(&sb);
        if (strcmp(sb.data, ".exit") == 0) {
            ARRAY_FREE(&sb);
            exit(EXIT_SUCCESS);
        }
        printf("Unknown command '%s'.\n", sb.data);
        sb.count = 0;
    }
}