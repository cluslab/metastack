PREFIX      ?= /opt/gridview/slurm
libdir      ?= $(PREFIX)

PLUGINDIR := $(abspath $(DESTDIR)/$(libdir)/slurm)
CONFDIR   := $(abspath $(DESTDIR)/$(PREFIX)/etc)
TOP_BUILD_DIR := $(shell dirname $(shell dirname $(shell pwd)))

ARCH      ?= $(shell uname -m)
VERSION   ?= 0.20.0

PLUGIN := spank_pyxis.so
CONF   := plugstack.conf

.PHONY: all install uninstall clean deb rpm

CPPFLAGS := -D_GNU_SOURCE -D_FORTIFY_SOURCE=2 -DPYXIS_VERSION=\"$(VERSION)\" $(CPPFLAGS)
CFLAGS := -std=gnu11 -g -Wall -Wunused-variable -fstack-protector-strong -fpic -I$(TOP_BUILD_DIR) $(CFLAGS) -O2
LDFLAGS := -Wl,-znoexecstack -Wl,-zrelro -Wl,-znow $(LDFLAGS)

C_SRCS := common.c args.c pyxis_slurmstepd.c pyxis_slurmd.c pyxis_srun.c pyxis_alloc.c pyxis_dispatch.c config.c enroot.c
C_OBJS := $(C_SRCS:.c=.o)

DEPS := $(C_OBJS:%.o=%.d)

all: $(PLUGIN)

$(C_OBJS): %.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -MMD -MF $*.d -c $<

$(PLUGIN): $(C_OBJS)
	$(CC) -shared $(LDFLAGS) -o $@ spank_pyxis.lds $^
	strip --strip-unneeded -R .comment $@

install: all
	install -d -m 755 $(PLUGINDIR)
	install -m 644 $(PLUGIN) $(PLUGINDIR)

uninstall:
	$(RM) $(PLUGINDIR)/$(PLUGIN)

clean:
	rm -rf $(C_OBJS) $(DEPS) $(PLUGIN)

orig: clean
	tar -caf ../nvslurm-plugin-pyxis_$(VERSION).orig.tar.xz --owner=root --group=root --exclude=.git .

deb: clean
	debuild -us -uc -G -i -tc

rpm: clean
	test -e $(ARCH) || ln -s . $(ARCH)
	rpmbuild --target=$(ARCH) --clean -ba -D"_topdir $(CURDIR)/rpm" -D"VERSION $(VERSION)" pyxis.spec
	$(RM) -r $(ARCH) $(addprefix rpm/, BUILDROOT SOURCES)

-include $(DEPS)
