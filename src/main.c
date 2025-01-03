#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "array.h"

typedef struct {
    char* data;
    size_t count;
    size_t capacity;
} StringBuilder;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
} Statement;

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

bool do_meta_command(StringBuilder* sb)
{
    if (strcmp(sb->data, ".exit") == 0) {
        ARRAY_FREE(sb);
        exit(EXIT_SUCCESS);
    }

    return false;
}

bool prepare_statement(StringBuilder* sb, Statement* s)
{
    assert(s && "Must provide a valid Statement ptr");
    if (strncmp(sb->data, "insert", 6) == 0) {
        s->type = STATEMENT_INSERT;
        return true;
    }
    if (strcmp(sb->data, "select") == 0) {
        s->type = STATEMENT_SELECT;
        return true;
    }
    return false;
}

void execute_statement(Statement* s)
{
    assert(s && "Must provide a valid Statement ptr");
    switch (s->type) {
        case STATEMENT_INSERT:
            printf("Execute insert...\n");
            break;
        case STATEMENT_SELECT:
            printf("Execute select...\n");
            break;
        default:
            assert(false && "Invalid statement type in execute_statement");
            break;
    }
}

int main(void)
{
    StringBuilder sb = {0};
    for (;;) {
        printf("db > ");
        read_input(&sb);

        if (sb.data[0] == '.') {
            if (!do_meta_command(&sb)) {
                printf("Unknown command '%s'.\n", sb.data);
            }
            sb.count = 0;
            continue;
        }

        Statement statement;
        if (!prepare_statement(&sb, &statement)) {
            printf("Unrecognized keyword at the beginning of statement '%s'.\n", sb.data);
            sb.count = 0;
            continue;
        }

        execute_statement(&statement);
        sb.count = 0;
    }
}