#pragma once

#include <assert.h>
#include <stdlib.h>

#define ARRAY_INIT_CAP 2

#define ARRAY_ENSURE_CAPACITY(arr, required) \
    if ((arr)->count + (required) > (arr)->capacity) { \
        if ((arr)->capacity == 0) { \
            (arr)->capacity = ARRAY_INIT_CAP >= required ? ARRAY_INIT_CAP : required; \
        } else { \
            (arr)->capacity = (arr)->capacity * 2 >= (arr)->count + (required) ? (arr)->capacity * 2 : (arr)->count + (required); \
        } \
        (arr)->data = realloc((arr)->data, (arr)->capacity * sizeof(*(arr)->data)); \
        assert((arr)->data && "Out of ram lol"); \
    } \

#define ARRAY_INIT(arr, cap) \
    (arr)->data = malloc(cap * sizeof(*(arr)->data)); \
    assert((arr)->data && "Out of ram lol"); \
    (arr)->capacity = cap; \

#define ARRAY_APPEND(arr, item) \
    do { \
        assert(sizeof(*(arr)->data) == sizeof(item) && "Trying to append data of a different size");  \
        ARRAY_ENSURE_CAPACITY((arr), 1) \
        (arr)->data[(arr)->count++] = (item); \
    } while (0)

#define ARRAY_APPEND_MANY(arr, items, cnt) \
    do { \
        assert(sizeof(*(arr)->data) == sizeof(*items) && "Trying to append data of a different size");  \
        ARRAY_ENSURE_CAPACITY((arr), cnt) \
        memcpy((arr)->data + (arr)->count, items, sizeof(*(arr)->data) * cnt); \
        (arr)->count += cnt; \
    } while(0)

#define ARRAY_FREE(arr) \
    do { \
        if ((arr)->data) { \
            free((void*)(arr)->data); \
        } \
        (arr)->data = NULL; \
        (arr)->count = 0; \
        (arr)->capacity = 0; \
    } while (0)
