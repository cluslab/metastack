PREFIX ?= /opt/gridview/slurm
CC := gcc
TOP_BUILD_DIR := $(shell dirname $(shell dirname $(shell pwd)))
CFLAGS := -fPIC -Wall -I$(TOP_BUILD_DIR)

PLUGIN_NAME := spank_env
PLUGIN_SOURCES := spank_env.c

PLUGIN_OBJECTS := $(PLUGIN_SOURCES:%.c=%.o)

.PHONY: all clean install uninstall

all: $(PLUGIN_NAME).so

$(PLUGIN_NAME).so: $(PLUGIN_OBJECTS)
	$(CC) -shared -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(PLUGIN_OBJECTS) $(PLUGIN_NAME).so

install: all
	mkdir -p $(PREFIX)/slurm
	cp $(PLUGIN_NAME).so $(PREFIX)/slurm/

uninstall:
	rm -f $(PREFIX)/slurm/$(PLUGIN_NAME).so

