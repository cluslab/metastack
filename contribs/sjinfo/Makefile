PREFIX ?= $(DESTDIR)/usr
BINDIR := $(PREFIX)/bin
HDRS   := sjinfo.h 
# SHOPTS := -shared -Wl,--version-script=version.map
CFLAGS := -Wall -Wextra -O0 -g
LDFLAGS := -lcurl -ljansson 
CC := gcc

# Source file list
SRCS := sjinfo.c sjaes.c spost.c

# Source file list generates target file list
OBJS := $(SRCS:.c=.o)

# default target
all: sjinfo sjaes spost

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

spost: spost.o 
	$(CC) $(CFLAGS) -o $@ $^ -lcurl
# Installation rules
install: sjinfo sjaes spost
	install -m0755 sjinfo $(BINDIR)/
	install -m0755 sjaes $(BINDIR)/
	install -m0755 spost $(BINDIR)/
# Cleanup rules
clean:
	rm -f sjinfo sjaes spost $(OBJS)
