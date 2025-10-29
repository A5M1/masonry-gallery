# ======================================================
# Compilers
CC_x86=x86_64-w64-mingw32-clang
CC_ARM=aarch64-w64-mingw32-clang
CC_LINUX_X86=clang
CC_LINUX_ARM=aarch64-linux-gnu-clang

# ======================================================
# Compiler flags
CFLAGS_COMMON=-std=c11 -Os -Iinclude -ffunction-sections -fdata-sections \
                -Wno-format-truncation -Wno-misleading-indentation
CFLAGS_X86_RELEASE=$(CFLAGS_COMMON) -msse2
CFLAGS_ARM_RELEASE=$(CFLAGS_COMMON) -march=armv8-a+simd
CFLAGS_LINUX_X86_RELEASE=$(CFLAGS_COMMON)
CFLAGS_LINUX_ARM_RELEASE=$(CFLAGS_COMMON) -march=armv8-a+simd

CFLAGS_X86_DEBUG=-std=c11 -g -O0 -Iinclude \
                    -ffunction-sections -fdata-sections \
                    -Wno-format-truncation -Wno-misleading-indentation \
                    -DDEBUG_DIAGNOSTIC -msse2
CFLAGS_ARM_DEBUG=-std=c11 -g -O0 -Iinclude \
                    -ffunction-sections -fdata-sections \
                    -Wno-format-truncation -Wno-misleading-indentation \
                    -DDEBUG_DIAGNOSTIC -march=armv8-a+simd

# ======================================================
# Linker flags
LDFLAGS_WIN=-s -Wl,--gc-sections -Wl,--strip-all -Wl,-O1 \
                -lws2_32 -lmswsock -lDbgHelp -lpsapi
LDFLAGS_WIN_DEBUG=$(LDFLAGS_WIN)

LDFLAGS_LINUX=-Wl,--gc-sections -Wl,-O1 -ldl -lpthread
LDFLAGS_LINUX_DEBUG=$(LDFLAGS_LINUX)

# ======================================================
# Directories
SRC_DIR=src
BUILD_DIR=build
DIST_DIR=dist
RUST_DIR=rust

# ======================================================
# Executables
EXEC_X86=$(DIST_DIR)/galleria.exe
EXEC_ARM=$(DIST_DIR)/galleria_arm64.exe
EXEC_X86_DEBUG=$(DIST_DIR)/galleria_debug.exe
EXEC_ARM_DEBUG=$(DIST_DIR)/galleria_arm64_debug.exe
EXEC_LINUX_X86=$(DIST_DIR)/galleria_linux_x86
EXEC_LINUX_ARM=$(DIST_DIR)/galleria_linux_arm64
EXEC_RUST_RELEASE=$(DIST_DIR)/galleria_view
EXEC_RUST_DEBUG=$(DIST_DIR)/galleria_view_debug

# ======================================================
# File lists
define obj_dir
$(BUILD_DIR)/$1
endef

SRCS=$(shell find $(SRC_DIR) -name '*.c')

OBJS_X86=$(patsubst $(SRC_DIR)/%.c,$(call obj_dir,x86)/%.o,$(SRCS))
OBJS_ARM=$(patsubst $(SRC_DIR)/%.c,$(call obj_dir,arm)/%.o,$(SRCS))
OBJS_X86_DEBUG=$(patsubst $(SRC_DIR)/%.c,$(call obj_dir,x86_debug)/%.o,$(SRCS))
OBJS_ARM_DEBUG=$(patsubst $(SRC_DIR)/%.c,$(call obj_dir,arm_debug)/%.o,$(SRCS))
OBJS_LINUX_X86=$(patsubst $(SRC_DIR)/%.c,$(call obj_dir,linux_x86)/%.o,$(SRCS))
OBJS_LINUX_ARM=$(patsubst $(SRC_DIR)/%.c,$(call obj_dir,linux_arm)/%.o,$(SRCS))

# ======================================================
# Compilation rules
$(call obj_dir,x86)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "[CC] (Windows x86_64) $< -> $@"
	@$(CC_x86) $(CFLAGS_X86_RELEASE) -c $< -o $@

$(call obj_dir,arm)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "[CC] (Windows ARM64) $< -> $@"
	@$(CC_ARM) $(CFLAGS_ARM_RELEASE) -c $< -o $@

$(call obj_dir,x86_debug)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "[CC] (Windows x86_64 Debug) $< -> $@"
	@$(CC_x86) $(CFLAGS_X86_DEBUG) -c $< -o $@

$(call obj_dir,arm_debug)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "[CC] (Windows ARM64 Debug) $< -> $@"
	@$(CC_ARM) $(CFLAGS_ARM_DEBUG) -c $< -o $@

$(call obj_dir,linux_x86)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "[CC] (Linux x86_64) $< -> $@"
	@$(CC_LINUX_X86) $(CFLAGS_LINUX_X86_RELEASE) -c $< -o $@

$(call obj_dir,linux_arm)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "[CC] (Linux ARM64) $< -> $@"
	@$(CC_LINUX_ARM) $(CFLAGS_LINUX_ARM_RELEASE) -c $< -o $@

# ======================================================
# Linking rules
all: x86

x86: $(OBJS_X86)
	@mkdir -p $(DIST_DIR)
	@echo "[LD] $(EXEC_X86)"
	@$(CC_x86) -o $(EXEC_X86) $(OBJS_X86) $(LDFLAGS_WIN)
	@$(MAKE) copy-assets

arm: $(OBJS_ARM)
	@mkdir -p $(DIST_DIR)
	@echo "[LD] $(EXEC_ARM)"
	@$(CC_ARM) -o $(EXEC_ARM) $(OBJS_ARM) $(LDFLAGS_WIN)
	@$(MAKE) copy-assets

debug: $(OBJS_X86_DEBUG)
	@mkdir -p $(DIST_DIR)
	@echo "[LD] $(EXEC_X86_DEBUG)"
	@$(CC_x86) -o $(EXEC_X86_DEBUG) $(OBJS_X86_DEBUG) $(LDFLAGS_WIN_DEBUG)
	@$(MAKE) copy-assets

debug-arm: $(OBJS_ARM_DEBUG)
	@mkdir -p $(DIST_DIR)
	@echo "[LD] $(EXEC_ARM_DEBUG)"
	@$(CC_ARM) -o $(EXEC_ARM_DEBUG) $(OBJS_ARM_DEBUG) $(LDFLAGS_WIN_DEBUG)
	@$(MAKE) copy-assets

linux-x86: $(OBJS_LINUX_X86)
	@mkdir -p $(DIST_DIR)
	@echo "[LD] $(EXEC_LINUX_X86)"
	@$(CC_LINUX_X86) -o $(EXEC_LINUX_X86) $(OBJS_LINUX_X86) $(LDFLAGS_LINUX)
	@$(MAKE) copy-assets

linux-arm: $(OBJS_LINUX_ARM)
	@mkdir -p $(DIST_DIR)
	@echo "[LD] $(EXEC_LINUX_ARM)"
	@$(CC_LINUX_ARM) -o $(EXEC_LINUX_ARM) $(OBJS_LINUX_ARM) $(LDFLAGS_LINUX)
	@$(MAKE) copy-assets

# ======================================================
# Rust targets
rust-release:
	@echo "[Cargo] Building Rust release..."
	@cd $(RUST_DIR) && cargo build --release
	@mkdir -p $(DIST_DIR)
	@cp $(RUST_DIR)/target/release/galleria-view$(EXE) $(EXEC_RUST_RELEASE) 2>/dev/null || true
	@$(MAKE) copy-assets

rust-debug:
	@echo "[Cargo] Building Rust debug..."
	@cd $(RUST_DIR) && cargo build
	@mkdir -p $(DIST_DIR)
	@cp $(RUST_DIR)/target/debug/galleria-view$(EXE) $(EXEC_RUST_DEBUG) 2>/dev/null || true
	@$(MAKE) copy-assets

# ======================================================
# Helpers and meta targets
copy-assets:
	@echo "[COPY] Copying assets (views/, public/)..."
	@mkdir -p $(DIST_DIR)
	@if [ -d "views" ]; then cp -r views $(DIST_DIR)/; fi
	@if [ -d "public" ]; then cp -r public $(DIST_DIR)/; fi

all-platforms: x86 arm linux-x86 linux-arm rust-release
	@echo "[DONE] Built all platforms successfully."

view: rust-release
	@echo "[BUILD] Rust frontend built -> $(EXEC_RUST_RELEASE)"

clean:
	@echo "[CLEAN]"
	@rm -rf $(BUILD_DIR) $(DIST_DIR)

rebuild: clean all

run:
ifeq ($(OS),Windows_NT)
	@$(EXEC_X86)
else
	@$(EXEC_LINUX_X86)
endif

# ======================================================
.PHONY: all clean rebuild run x86 arm debug debug-arm \
	    linux-x86 linux-arm rust-release rust-debug \
	    copy-assets all-platforms view
