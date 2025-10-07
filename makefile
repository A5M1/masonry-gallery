CC_x86=x86_64-w64-mingw32-gcc
CC_arm=aarch64-w64-mingw32-gcc

CFLAGS_COMMON=-std=c11 -O3 -Iinclude -s -ffunction-sections -fdata-sections -Wno-format-truncation -Wno-misleading-indentation
CFLAGS_x86=$(CFLAGS_COMMON) -msse2
CFLAGS_arm=$(CFLAGS_COMMON) -march=armv8-a+simd

LDFLAGS=-Wl,--gc-sections -lws2_32 -lmswsock

EXECUTABLE=galleria.exe
EXECUTABLE_ARM=galleria_arm64.exe
TEST_EXEC=test.exe
RM=rm -f
RMDIR=rm -rf
MKDIR=mkdir -p
UPX=upx

SRC_DIR=src
BUILD_DIR=build
ASM_DIR=asm
SRCS=$(shell find $(SRC_DIR) -name '*.c')
OBJS=$(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRCS))
ASMS=$(patsubst $(SRC_DIR)/%.c,$(ASM_DIR)/%.s,$(SRCS))

all: x86

$(BUILD_DIR):
	@$(MKDIR) $(BUILD_DIR)

$(ASM_DIR):
	@$(MKDIR) $(ASM_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@$(MKDIR) $(dir $@)
	@echo "[CC] $< -> $@"
	@$(CC) $(CFLAGS) -c $< -o $@

$(ASM_DIR)/%.s: $(SRC_DIR)/%.c
	@$(MKDIR) $(dir $@)
	@echo "[ASM] $< -> $@"
	@$(CC) $(CFLAGS) -S $< -o $@

x86: CC=$(CC_x86)
x86: CFLAGS=$(CFLAGS_x86)
x86: $(BUILD_DIR) $(EXECUTABLE)

arm: CC=$(CC_arm)
arm: CFLAGS=$(CFLAGS_arm)
arm: $(BUILD_DIR) $(EXECUTABLE_ARM)

asm: CC=$(CC_x86)
asm: CFLAGS=$(CFLAGS_x86)
asm: $(ASM_DIR) $(ASMS)

$(EXECUTABLE): $(OBJS)
	@echo "[LD] $@"
	@$(CC) -o $@ $(OBJS) $(LDFLAGS)

$(EXECUTABLE_ARM): $(OBJS)
	@echo "[LD] $@"
	@$(CC) -o $@ $(OBJS) $(LDFLAGS)

upx-x64: $(EXECUTABLE)
	@$(UPX) --best $(EXECUTABLE)

upx-arm: $(EXECUTABLE_ARM)
	@$(UPX) --best $(EXECUTABLE_ARM)

clean:
	@$(RM) $(OBJS) $(ASMS) $(EXECUTABLE) $(EXECUTABLE_ARM) $(TEST_EXEC)
	@$(RMDIR) $(BUILD_DIR) $(ASM_DIR)

rebuild: clean all

run: $(EXECUTABLE)
	@./$(EXECUTABLE)

.PHONY: all clean rebuild run x86 arm asm upx-x64 upx-arm
