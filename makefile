CC = gcc
CFLAGS = -Wall -Wextra -O2
LDFLAGS =

SRC_DIR = src
DIST_DIR = dist
HTML_DIR = html
OUT = $(DIST_DIR)/ga

ifeq ($(OS),Windows_NT)
    RM = del /Q /F
    MKDIR = if not exist $(DIST_DIR) mkdir $(DIST_DIR)
    CP = xcopy /E /I /Y
    OUT := $(OUT).exe
    LIBS = -lws2_32
    RUN = start "" $(OUT)
else
    RM = rm -f
    MKDIR = mkdir -p $(DIST_DIR)
    CP = cp -r
    LIBS = -lpthread
    RUN = ./$(OUT)
endif

SRC = $(SRC_DIR)/GALLERY.c

all: prepare $(OUT) copy_html

prepare:
	$(MKDIR)

$(OUT): $(SRC)
	$(CC) $(CFLAGS) -o $(OUT) $(SRC) $(LIBS)

copy_html:
	$(CP) $(HTML_DIR) $(DIST_DIR)

run: all
	$(RUN)

clean:
	$(RM) $(OUT)
ifeq ($(OS),Windows_NT)
	rmdir /S /Q $(DIST_DIR)\html
else
	rm -rf $(DIST_DIR)/html
endif
