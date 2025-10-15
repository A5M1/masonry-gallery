CC_x86=x86_64-w64-mingw32-gcc
CC_arm=aarch64-w64-mingw32-gcc
CFLAGS_COMMON=-std=c11 -Os -flto -Iinclude -s -ffunction-sections -fdata-sections -Wno-format-truncation -Wno-misleading-indentation
CFLAGS_x86=$(CFLAGS_COMMON) -msse2
CFLAGS_arm=$(CFLAGS_COMMON) -march=armv8-a+simd
LDFLAGS=-flto -Wl,--gc-sections -Wl,--strip-all -Wl,-O1 -lws2_32 -lmswsock
CFLAGS_DEBUG_COMMON=-std=c11 -g -O0 -Iinclude -ffunction-sections -fdata-sections -Wno-format-truncation -Wno-misleading-indentation
CFLAGS_DEBUG_x86=$(CFLAGS_DEBUG_COMMON) -msse2
CFLAGS_DEBUG_arm=$(CFLAGS_DEBUG_COMMON) -march=armv8-a+simd
LDFLAGS_DEBUG=-Wl,--gc-sections -Wl,-O1 -lws2_32 -lmswsock
EXECUTABLE=galleria.exe
EXECUTABLE_ARM=galleria_arm64.exe
EXECUTABLE_DEBUG=galleria_debug.exe
EXECUTABLE_ARM_DEBUG=galleria_arm64_debug.exe
SRC_DIR=src
BUILD_DIR=build
ASM_DIR=asm
SRCS=$(shell find $(SRC_DIR) -name '*.c')
OBJS=$(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRCS))
ASMS=$(patsubst $(SRC_DIR)/%.c,$(ASM_DIR)/%.s,$(SRCS))

all: x86

$(BUILD_DIR):
	@mkdir -p $@

$(ASM_DIR):
	@mkdir -p $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "[CC] $< -> $@"
	@$(CC) $(CFLAGS) -c $< -o $@

$(ASM_DIR)/%.s: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "[ASM] $< -> $@"
	@$(CC) $(CFLAGS) -S $< -o $@

x86: CC=$(CC_x86)
x86: CFLAGS=$(CFLAGS_x86)
x86: $(BUILD_DIR) $(EXECUTABLE)

arm: CC=$(CC_arm)
arm: CFLAGS=$(CFLAGS_arm)
arm: $(BUILD_DIR) $(EXECUTABLE_ARM)

debug: CC=$(CC_x86)
debug: CFLAGS=$(CFLAGS_DEBUG_x86)
debug: $(BUILD_DIR) $(EXECUTABLE_DEBUG)

debug-arm: CC=$(CC_arm)
debug-arm: CFLAGS=$(CFLAGS_DEBUG_arm)
debug-arm: $(BUILD_DIR) $(EXECUTABLE_ARM_DEBUG)

thumbs-tool: CC=$(CC_x86)
thumbs-tool: CFLAGS=$(CFLAGS_x86)
thumbs-tool: $(BUILD_DIR) $(TOOLS_OBJ) $(OBJS)
	@echo "[LD] tools/thumbs_tool.exe"
	@$(CC) $(CFLAGS) -o tools/thumbs_tool.exe $(TOOLS_OBJ) $(filter-out $(BUILD_DIR)/main.o,$(OBJS)) $(LDFLAGS)

$(BUILD_DIR)/tools/thumbs_tool.o: tools/thumbs_tool.c
	@mkdir -p $(dir $@)
	@echo "[CC] tools/thumbs_tool.c -> $@"
	@$(CC) $(CFLAGS) -c tools/thumbs_tool.c -o $@

TOOLS_OBJ=$(BUILD_DIR)/tools/thumbs_tool.o

asm: CC=$(CC_x86)
asm: CFLAGS=$(CFLAGS_x86)
asm: $(ASM_DIR) $(ASMS)

$(EXECUTABLE): $(OBJS)
	@echo "[LD] $@"
	@$(CC) -o $@ $(OBJS) $(LDFLAGS)

$(EXECUTABLE_DEBUG): $(OBJS)
	@echo "[LD] $@"
	@$(CC) -o $@ $(OBJS) $(LDFLAGS_DEBUG)

$(EXECUTABLE_ARM): $(OBJS)
	@echo "[LD] $@"
	@$(CC) -o $@ $(OBJS) $(LDFLAGS)

$(EXECUTABLE_ARM_DEBUG): $(OBJS)
	@echo "[LD] $@"
	@$(CC) -o $@ $(OBJS) $(LDFLAGS_DEBUG)

upx-x64: $(EXECUTABLE)
	@upx --best $@

upx-arm: $(EXECUTABLE_ARM)
	@upx --best $@

smallest: x86
	@echo "[SMALL] compressing $(EXECUTABLE) with UPX..."
	@upx --best $(EXECUTABLE) || echo "UPX failed; binary left uncompressed"

clean:
	@rm -f $(OBJS) $(ASMS) $(EXECUTABLE) $(EXECUTABLE_ARM) $(EXECUTABLE_DEBUG) $(EXECUTABLE_ARM_DEBUG)
	@rm -rf $(BUILD_DIR) $(ASM_DIR)

rebuild: clean all

run: $(EXECUTABLE)
	@./$(EXECUTABLE)

.PHONY: all clean rebuild run x86 arm asm debug debug-arm upx-x64 upx-arm smallest
