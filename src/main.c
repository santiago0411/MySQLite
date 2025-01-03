#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "array.h"
#include "int_types.h"

typedef struct {
    char* data;
    size_t count;
    size_t capacity;
} StringBuilder;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_EXIT,
    META_COMMAND_UNKNOWN_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_ID_TOO_BIG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
    EXECUTE_FAILURE
} ExecuteResult;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
    u32 id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

#define SIZE_OF_MEMBER(Struct, Member) sizeof(((Struct*)0)->Member)
const size_t ID_SIZE = SIZE_OF_MEMBER(Row, id);
const size_t USERNAME_SIZE = SIZE_OF_MEMBER(Row, username);
const size_t EMAIL_SIZE = SIZE_OF_MEMBER(Row, email);
const size_t ROW_SIZE = sizeof(Row);
const size_t ID_OFFSET = offsetof(Row, id);
const size_t USERNAME_OFFSET = offsetof(Row, username);
const size_t EMAIL_OFFSET = offsetof(Row, email);

#define TABLE_MAX_PAGES 100
const u32 PAGE_SIZE = 4096;
const u32 ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const u32 TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    u32 rows_count;
    void* pages[TABLE_MAX_PAGES];
} Table;

void* row_slot(Table* t, u32 row_num)
{
    assert(t && "Must provide a valid to row_slot");
    u32 page_num = row_num / ROWS_PER_PAGE;
    void* page = t->pages[page_num];
    if (!page) {
        page = t->pages[page_num] = malloc(PAGE_SIZE);
    }
    u32 row_offset = row_num % ROWS_PER_PAGE;
    u32 byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

void print_row(Row* r)
{
    printf("(%u, %s, %s)\n", r->id, r->username, r->email);
}

void serialize_row(Row* r, void* dst)
{
    assert(r && dst && "Must provide valid ptrs to serialize_row");
    memcpy(dst + ID_OFFSET, &r->id, ID_SIZE);
    memcpy(dst + USERNAME_OFFSET, &r->username, USERNAME_SIZE);
    memcpy(dst + EMAIL_OFFSET, &r->email, EMAIL_SIZE);
}

void deserialize_row(void* src, Row* r)
{
    assert(src && r && "Must provide valid ptrs to deserialize_row");
    memcpy(&r->id, src + ID_OFFSET, ID_SIZE);
    memcpy(&r->username, src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&r->email, src + EMAIL_OFFSET, EMAIL_SIZE);
}

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

MetaCommandResult do_meta_command(StringBuilder* sb)
{
    if (strcmp(sb->data, ".exit") == 0) {
        return META_COMMAND_EXIT;
    }
    return META_COMMAND_UNKNOWN_COMMAND;
}

PrepareResult prepare_insert(StringBuilder* sb, Statement* s)
{
    s->type = STATEMENT_INSERT;
    strtok(sb->data, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (!id_string || !username || !email) {
        return PREPARE_SYNTAX_ERROR;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    i64 id = atoll(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (id > UINT32_MAX) {
        return PREPARE_ID_TOO_BIG;
    }

    s->row_to_insert.id = (u32)id;
    strcpy(s->row_to_insert.username, username);
    strcpy(s->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(StringBuilder* sb, Statement* s)
{
    assert(s && "Must provide a valid Statement ptr");
    if (strncmp(sb->data, "insert", 6) == 0) {
        return prepare_insert(sb, s);
    }
    if (strcmp(sb->data, "select") == 0) {
        s->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* s, Table* t)
{
    assert(s && t && "Must provide valid ptrs to execute_insert");
    if (t->rows_count >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    serialize_row(&s->row_to_insert, row_slot(t, t->rows_count));
    t->rows_count += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* s, Table* t)
{
    assert(s && t && "Must provide valid ptrs to execute_select");
    Row row;
    for (u32 i = 0; i< t->rows_count; i++) {
        deserialize_row(row_slot(t, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* s, Table* t)
{
    assert(s && t && "Must provide a valid ptrs to execute_statement");
    switch (s->type) {
        case STATEMENT_INSERT:
            return execute_insert(s, t);
        case STATEMENT_SELECT:
            return execute_select(s, t);
        default:
            assert(false && "Invalid statement type in execute_statement");
            return EXECUTE_FAILURE;
    }
}

Table* create_table()
{
    Table* t = malloc(sizeof(Table));
    memset(t, 0, sizeof(Table));
    return t;
}

void destroy_table(Table* t)
{
    for (u32 i = 0; t->pages[i]; i++) {
        free(t->pages[i]);
    }
    free(t);
}

int main(void)
{
    Table* table = create_table();
    StringBuilder sb = {0};
    for (;;) {
        sb.count = 0;
        printf("db > ");
        read_input(&sb);

        if (sb.data[0] == '.') {
            switch (do_meta_command(&sb)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_EXIT:
                    goto exit;
                case META_COMMAND_UNKNOWN_COMMAND:
                    printf("Unknown command '%s'.\n", sb.data);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(&sb, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_ID_TOO_BIG:
                printf("ID must be smaller.\n");
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement '%s'.\n", sb.data);
                continue;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at the beginning of statement '%s'.\n", sb.data);
                continue;
        }

        switch (execute_statement(&statement, table)) {
            case EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Table full.\n");
                break;
            case EXECUTE_FAILURE:
                printf("Execute failure.\n");
                break;
        }
    }
exit:
    destroy_table(table);
    ARRAY_FREE(&sb);
    exit(EXIT_SUCCESS);
}