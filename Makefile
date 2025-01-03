CC = gcc
CFLAGS_DEBUG = -Wall -g
CFLAGS_RELEASE = -Wall -O3

SRC_DIR = src
SRC = $(wildcard $(SRC_DIR)/*.c)

ifeq ($(OS),Windows_NT)
    PLATFORM_MACRO = -DPLATFORM_WINDOWS
else
    PLATFORM_MACRO = -DPLATFORM_LINUX
endif

OBJ_DEBUG = $(patsubst $(SRC_DIR)/%.c, bin-int/debug-x64/%.o, $(SRC))
OBJ_RELEASE = $(patsubst $(SRC_DIR)/%.c, bin-int/release-x64/%.o, $(SRC))

BIN_INT_DIR_DEBUG = bin-int/debug-x64
BIN_INT_DIR_RELEASE = bin-int/release-x64
BIN_DIR_DEBUG = bin/debug-x64
BIN_DIR_RELEASE = bin/release-x64

TARGET_NAME = MySQLite

ifeq ($(OS),Windows_NT)
    TARGET_DEBUG = $(BIN_DIR_DEBUG)/$(TARGET_NAME).exe
    TARGET_RELEASE = $(BIN_DIR_RELEASE)/$(TARGET_NAME).exe
else
    TARGET_DEBUG = $(BIN_DIR_DEBUG)/$(TARGET_NAME)
    TARGET_RELEASE = $(BIN_DIR_RELEASE)/$(TARGET_NAME)
endif

all: debug release

# Create directories
$(BIN_INT_DIR_DEBUG) $(BIN_DIR_DEBUG):
	mkdir -p $(BIN_INT_DIR_DEBUG) $(BIN_DIR_DEBUG)

$(BIN_INT_DIR_RELEASE) $(BIN_DIR_RELEASE):
	mkdir -p $(BIN_INT_DIR_RELEASE) $(BIN_DIR_RELEASE)


# Debug Config
debug: $(TARGET_DEBUG)

$(TARGET_DEBUG): $(OBJ_DEBUG)
	$(CC) $(CFLAGS_DEBUG) $(PLATFORM_MACRO) -o $@ $^

$(BIN_INT_DIR_DEBUG)/%.o: $(SRC_DIR)/%.c | $(BIN_INT_DIR_DEBUG)
	$(CC) $(CFLAGS_DEBUG) $(PLATFORM_MACRO) -c $< -o $@


# Release Config
release: $(TARGET_RELEASE)

$(TARGET_RELEASE): $(OBJ_RELEASE)
	$(CC) $(CFLAGS_RELEASE) $(PLATFORM_MACRO) -o $@ $^

$(BIN_INT_DIR_RELEASE)/%.o: $(SRC_DIR)/%.c | $(BIN_INT_DIR_RELEASE)
	$(CC) $(CFLAGS_RELEASE) $(PLATFORM_MACRO) -c $< -o $@



clean:
	rm -rf bin-int
	rm -rf bin