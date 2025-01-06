#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

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
    FILE* file;
    u32 file_length;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    Pager* pager;
    u32 rows_count;
} Table;

typedef struct {
    Table* table;
    u32 row_num;
    bool end_of_table;
} Cursor;

Cursor table_start(Table* t)
{
    return (Cursor){
        .table = t,
        .row_num = 0,
        .end_of_table = t->rows_count == 0
    };
}

Cursor table_end(Table* t)
{
    return (Cursor){
        .table = t,
        .row_num = t->rows_count,
        .end_of_table = true
    };
}

void cursor_advance(Cursor* c)
{
    c->row_num += 1;
    if (c->row_num == c->table->rows_count) {
        c->end_of_table = true;
    }
}

void* get_page(Pager* p, u32 page_num)
{
    if (page_num >= TABLE_MAX_PAGES) {
        printf("Tried to fetch a page out of bounds. %u > %u\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (!p->pages[page_num]) {
        // Cache miss, must read from file
        void* page = malloc(PAGE_SIZE);
        u32 num_pages = p->file_length / PAGE_SIZE;
        if (p->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            fseek(p->file, page_num * PAGE_SIZE, SEEK_SET);
            fread(page, PAGE_SIZE, 1, p->file);
            rewind(p->file);
        }

        p->pages[page_num] = page;
    }

    return p->pages[page_num];
}

void* cursor_value(Cursor c)
{
    u32 row_num = c.row_num;
    u32 page_num = row_num / ROWS_PER_PAGE;
    void* page = get_page(c.table->pager, page_num);
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
    strncpy(dst + USERNAME_OFFSET, (const char*)&r->username, USERNAME_SIZE);
    strncpy(dst + EMAIL_OFFSET, (const char*)&r->email, EMAIL_SIZE);
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
    Cursor cursor = table_end(t);
    serialize_row(&s->row_to_insert, cursor_value(cursor));
    t->rows_count += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* s, Table* t)
{
    assert(s && t && "Must provide valid ptrs to execute_select");
    Cursor cursor = table_start(t);
    Row row;
    while (!cursor.end_of_table) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(&cursor);
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

Pager* pager_open(const char* filename)
{
    FILE* file = fopen(filename, "r+");
    if (!file) {
        file = fopen(filename, "w+");
    }
    if (!file) {
        fprintf(stderr, "Error opening pager file.");
        exit(EXIT_FAILURE);
    }
    fseek(file, 0, SEEK_END);
    size_t length = ftell(file);
    rewind(file);

    Pager* pager = malloc(sizeof(Pager));
    pager->file = file;
    pager->file_length = length;
    memset(pager->pages, 0, sizeof(pager->pages));
    return pager;
}

Table* db_open(const char* filename)
{
    Pager* pager = pager_open(filename);
    u32 rows_count = pager->file_length / ROW_SIZE;
    Table* t = malloc(sizeof(Table));
    t->pager = pager;
    t->rows_count = rows_count;
    return t;
}

void pager_flush(Pager* p, u32 page_num, u32 size)
{
    if (!p->pages[page_num]) {
        printf("Tried to flush a null page.\n");
        exit(EXIT_FAILURE);
    }

    if (fseek(p->file, page_num * PAGE_SIZE, SEEK_SET) != 0) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    size_t written = fwrite(p->pages[page_num], size, 1, p->file);
    if (written <= 0) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table* t)
{
    assert(t && "Must provide a valid Table ptr to db_close");
    Pager* p = t->pager;
    u32 full_pages_count = t->rows_count / ROWS_PER_PAGE;

    for (u32 i = 0; i < full_pages_count; i++) {
        if (!p->pages[i]) {
            continue;
        }
        pager_flush(p, i , PAGE_SIZE);
        free(p->pages[i]);
    }

    // There may be a partial page to write to the end of the file
    // This should not be needed after we switch to a B-tree
    uint32_t additional_rows_count = t->rows_count % ROWS_PER_PAGE;
    if (additional_rows_count > 0) {
        uint32_t page_num = full_pages_count;
        if (p->pages[page_num] != NULL) {
            pager_flush(p, page_num, additional_rows_count * ROW_SIZE);
        }
    }

    if (fclose(p->file) != 0) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (u32 i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = p->pages[i];
        if (page) {
            free(page);
        }
    }
    free(p);
    free(t);
}

int main(int argc, char** argv)
{
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    Table* table = db_open(argv[1]);
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
    db_close(table);
    ARRAY_FREE(&sb);
    exit(EXIT_SUCCESS);
}