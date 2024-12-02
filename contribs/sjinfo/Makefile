PREFIX ?= /opt/gridview/slurm
BINDIR := $(PREFIX)/bin
HDRS   := sjinfo.h 
# SHOPTS := -shared -Wl,--version-script=version.map
CFLAGS := -Wall -Wextra -O2
LDFLAGS := -lcurl -ljansson
CC := gcc

# Source file list
SRCS := sjinfo.c  sjaes.c

# Source file list generates target file list
OBJS := $(SRCS:.c=.o)

# default target
all: sjinfo sjaes

# # Rules for generating executable files
# sjinfo: $(OBJS)
# 	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# sjaes: $(OBJS)
# 	$(CC) $(CFLAGS) -o $@ $^ 
# Link object files to generate executable files
sjinfo: sjinfo.o
	$(CC) $(CFLAGS) -o $@ $^ -lcurl -ljansson

sjaes: sjaes.o
	$(CC) $(CFLAGS) -o $@ $^
# Installation rules
install: sjinfo
	install -m0755 sjinfo $(BINDIR)/
	install -m0755 sjaes $(BINDIR)/
# Cleanup rules
clean:
	rm -f sjinfo sjaes  $(OBJS)
