#
# Copyright (c) 2012, Joyent, Inc. All rights reserved.
#
# Makefile: basic Makefile for manta compute engine
#
# This Makefile contains only repo-specific logic and uses included makefiles
# to supply common targets (javascriptlint, jsstyle, restdown, etc.), which are
# used by other repos as well. You may well need to rewrite most of this file,
# but you shouldn't need to touch the included makefiles.
#
# If you find yourself adding support for new targets that could be useful for
# other projects too, you should add these to the original versions of the
# included Makefiles (in eng.git) so that other teams can use them too.
#

#
# While we only support developing on SmartOS, where we have sdcnode builds
# available, it's convenient to be able to use tools like "mrjob" and the like
# from Mac laptops without having to set up a complete dev environment.
#
ifeq ($(shell uname -s),Darwin)
	USE_LOCAL_NODE=true
else
	USE_LOCAL_NODE=false
endif

#
# Tools
#
BASHSTYLE	 = $(NODE) dev/tools/bashstyle
CATEST		 = tools/catest
CC      	 = gcc
SMF_DTD		 = dev/tools/service_bundle.dtd.1

#
# Files
#
COMPONENT_DIRS   = agent client common dev jobsupervisor
BASH_FILES	 = \
    agent/npm/postinstall.sh		\
    agent/npm/preuninstall.sh		\
    agent/sbin/logpush.sh		\
    agent/sbin/mragentconf		\
    agent/sbin/mrdeploycompute		\
    agent/sbin/mrgroups			\
    agent/sbin/mrlogexpire.sh		\
    agent/sbin/mrzone			\
    agent/sbin/mrzonedisable		\
    agent/sbin/mrzoneremove		\
    agent/sbin/mrzones			\
    client/sbin/mrjobreport		\
    client/sbin/mrerrors		\
    client/sbin/mrextractjob	\
    dev/tools/catest			\
    dev/tools/mru			\
    jobsupervisor/boot/configure.sh

DOC_FILES	 = index.restdown ops.restdown

JSON_DIRS        = $(COMPONENT_DIRS:%=%/package.json %/etc %/sapi_manifests)
JSON_DIRS	+= $(COMPONENT_DIRS:%=%/etc)
JSON_DIRS	+= $(COMPONENT_DIRS:%=%/sapi_manifests)
JSON_FILES	:= $(shell find $(JSON_DIRS) -name '*.json' 2>/dev/null)
JSON_FILES	+= jobsupervisor/sapi_manifests/marlin/template

JS_DIRS		 = $(COMPONENT_DIRS:%=%/lib)
JS_DIRS		+= $(COMPONENT_DIRS:%=%/dev)
JS_FILES	:= $(shell find $(JS_DIRS) -name '*.js' 2>/dev/null)
JS_FILES	+= \
    client/sbin/mrerrorsummary	\
    client/sbin/mrjob		\
    client/sbin/mrmeter 	\
    dev/tools/bashstyle		\
    dev/tools/mrpound		\
    jobsupervisor/sbin/mlocate

JSL_CONF_NODE	 = dev/tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
SMF_MANIFESTS_IN = \
    agent/smf/manifests/marlin-agent.xml.in \
    agent/smf/manifests/marlin-lackey.xml.in

#
# v8plus uses the CTF tools as part of its build, but they can safely be
# overridden here so that this works in dev zones without them.
#
NPM_ENV		 = MAKE_OVERRIDES="CTFCONVERT=/bin/true CTFMERGE=/bin/true"

include ./dev/tools/mk/Makefile.defs
include ./dev/tools/mk/Makefile.smf.defs

ifneq ($(USE_LOCAL_NODE),true)
    NODE_PREBUILT_VERSION = v0.8.25
    NODE_PREBUILT_TAG = zone

    include ./dev/tools/mk/Makefile.node_prebuilt.defs
else
    NPM_EXEC :=
    NPM = npm
endif

#
# Repo-specific targets
#
CFLAGS		+= -Wall -Werror
EXECS   	 = dev/tests/mallocbomb/mallocbomb
CLEANFILES	+= $(EXECS)

.PHONY: all
all: $(SMF_MANIFESTS) deps $(EXECS) $(PROTO_FILES)

.PHONY: deps
deps: | $(NPM_EXEC)
	$(NPM_ENV) $(NPM) --no-rebuild install

.PHONY: test
test: all
	tools/catest -a

dev/test/mallocbomb/mallocbomb: dev/test/mallocbomb/mallocbomb.c

DISTCLEAN_FILES += node_modules

include ./dev/tools/mk/Makefile.deps
include ./dev/tools/mk/Makefile.smf.targ
include ./dev/tools/mk/Makefile.targ

ifneq ($(USE_LOCAL_NODE),true)
    include ./dev/tools/mk/Makefile.node_prebuilt.targ
endif

#
# proto area construction
#
PROTO_ROOT=$(BUILD)/proto
PROTO_SITE_ROOT=$(PROTO_ROOT)/site
PROTO_SMARTDC_ROOT=$(PROTO_ROOT)/root/opt/smartdc
PROTO_MARLIN_ROOT=$(PROTO_SMARTDC_ROOT)/marlin
PROTO_BOOT_ROOT=$(PROTO_SMARTDC_ROOT)/boot

MARLIN_AGENT  := $(shell cd agent  && find * -type f -not -name package.json -not -path 'smf/manifests/*.xml' -not -name .*.swp -not -path 'smf/manifests/*.xml.in')
MARLIN_CLIENT := $(shell cd client && find * -type f -not -name package.json -not -name .*.swp)
MARLIN_COMMON := $(shell cd common && find * -type f -not -name package.json -not -name .*.swp)
MARLIN_DEV    := package.json
MARLIN_JOBSUP := $(shell cd jobsupervisor  && find * -type f -not -name package.json -not -name .*.swp)

PROTO_MARLIN_AGENT = $(MARLIN_AGENT:%=$(PROTO_MARLIN_ROOT)/%)
PROTO_MARLIN_CLIENT = $(MARLIN_CLIENT:%=$(PROTO_MARLIN_ROOT)/%)
PROTO_MARLIN_COMMON = $(MARLIN_COMMON:%=$(PROTO_MARLIN_ROOT)/%)
PROTO_MARLIN_DEV = $(MARLIN_DEV:%=$(PROTO_MARLIN_ROOT)/%)
PROTO_MARLIN_JOBSUP = $(MARLIN_JOBSUP:%=$(PROTO_MARLIN_ROOT)/%)

$(PROTO_MARLIN_AGENT): $(PROTO_MARLIN_ROOT)/%: agent/%
	mkdir -p $(@D) && cp $^ $@

$(PROTO_MARLIN_CLIENT): $(PROTO_MARLIN_ROOT)/%: client/%
	mkdir -p $(@D) && cp $^ $@

$(PROTO_MARLIN_COMMON): $(PROTO_MARLIN_ROOT)/%: common/%
	mkdir -p $(@D) && cp $^ $@

$(PROTO_MARLIN_DEV): $(PROTO_MARLIN_ROOT)/%: dev/%
	mkdir -p $(@D) && cp $^ $@

$(PROTO_MARLIN_JOBSUP): $(PROTO_MARLIN_ROOT)/%: jobsupervisor/%
	mkdir -p $(@D) && cp $^ $@

PROTO_MARLIN_FILES  = \
    $(PROTO_MARLIN_COMMON) \
    $(PROTO_MARLIN_AGENT) \
    $(PROTO_MARLIN_CLIENT) \
    $(PROTO_MARLIN_DEV) \
    $(PROTO_MARLIN_JOBSUP)

PROTO_MANIFESTS = $(SMF_MANIFESTS:agent/%=$(PROTO_MARLIN_ROOT)/%)
PROTO_MARLIN_FILES += $(PROTO_MANIFESTS)
$(PROTO_MANIFESTS): $(PROTO_MARLIN_ROOT)/%: agent/%
	mkdir -p $(@D) && cp $^ $@

#
# It's unfortunate that build/node, build/docs, and build/scripts are referenced
# by directory rather than by file, since that means we can't do incremental
# updates when individual files change, but that's not a common case since these
# are pulled or built as a unit.
#
PROTO_MARLIN_FILES += $(PROTO_BOOT_ROOT)/scripts
$(PROTO_BOOT_ROOT)/scripts: $(BUILD)/scripts
	mkdir -p $(@D) && cp -r $^ $@
$(BUILD)/scripts: scripts

PROTO_MARLIN_BUILD  = \
    $(PROTO_MARLIN_ROOT)/$(BUILD)/docs \
    $(PROTO_MARLIN_ROOT)/$(BUILD)/node

PROTO_MARLIN_FILES += $(PROTO_MARLIN_BUILD)
PROTO_FILES += $(PROTO_MARLIN_FILES)
$(PROTO_MARLIN_ROOT)/$(BUILD)/node: | $(BUILD)/node
	mkdir -p $(@D) && cp -r $(BUILD)/node $@
$(BUILD)/node: deps

$(PROTO_MARLIN_ROOT)/$(BUILD)/docs: docs
	rm -rf "$@" && mkdir -p "$(@D)" && cp -r $(BUILD)/docs "$@"

PROTO_FILES += $(PROTO_BOOT_ROOT)/configure.sh
$(PROTO_BOOT_ROOT)/configure.sh:
	mkdir -p $(@D) && ln -fs $(PROTO_BOOT_ROOT)/configure.sh $@

#
# Some tools were historically delivered in "tools", but really belong on
# "sbin".  We moved these to "sbin", but we add symlinks into "tools".
#
LINKS_AGENT_BIN = mragentconf mrdeploycompute mrzone mrzoneremove
PROTO_LINKS_AGENT_BIN = $(LINKS_AGENT_BIN:%=$(PROTO_MARLIN_ROOT)/tools/%)
PROTO_MARLIN_FILES += $(PROTO_LINKS_AGENT_BIN)
$(PROTO_MARLIN_ROOT)/tools/%: | agent/sbin/%
	mkdir -p $(@D) && ln -fs ../sbin/$* $@

PROTO_FILES += $(PROTO_SITE_ROOT)/.do-not-delete-me
$(PROTO_SITE_ROOT)/.do-not-delete-me:
	mkdir -p $(@D) && touch $@

.PHONY: proto
proto: $(PROTO_FILES) proto_deps

.PHONY: proto_deps
proto_deps: $(PROTO_FILES)
	cd $(PROTO_MARLIN_ROOT) && $(NPM_ENV) $(NPM) --no-rebuild install

#
# Mountagain Gorilla targets
#
MG_NAME 		 = marlin
MG_PROTO		 = $(PROTO_ROOT)
MG_IMAGEROOT		 = $(PROTO_MARLIN_ROOT)
MG_RELEASE_TARBALL	 = $(MG_NAME)-pkg-$(STAMP).tar.bz2
PROTO_TARBALL 		 = $(BUILD)/$(MG_RELEASE_TARBALL)
BITS_PROTO_TARBALL	 = $(BITS_DIR)/$(MG_NAME)/$(MG_RELEASE_TARBALL)

MG_AGENT_TARBALL	 = $(MG_NAME)-$(STAMP).tar.gz
AGENT_TARBALL	 	 = $(BUILD)/$(MG_AGENT_TARBALL)
BITS_AGENT_TARBALL	 = $(BITS_DIR)/$(MG_NAME)/$(MG_AGENT_TARBALL)

CLEAN_FILES		+= $(MG_PROTO) \
			   $(BUILD)/$(MG_NAME)-pkg-*.tar.bz2 \
			   $(BUILD)/$(MG_NAME)-*.tar.gz

#
# "release" target creates two tarballs: the first to be used as an input for
# the jobworker zone, the second to be installed in each system's global zone
# via apm.
#
.PHONY: release
release: $(PROTO_TARBALL) $(AGENT_TARBALL)

$(PROTO_TARBALL): proto
	$(TAR) -C $(MG_PROTO) -cjf $@ root site

$(AGENT_TARBALL): proto
	$(TAR) -C $(MG_PROTO)/root/opt/smartdc -czf $@ $(MG_NAME)

#
# "publish" target copies the release tarball into BITS_DIR.
#
.PHONY: publish
publish: check-bitsdir $(BITS_PROTO_TARBALL) $(BITS_AGENT_TARBALL)

.PHONY: check-bitsdir
check-bitsdir:
	@if [[ -z "$(BITS_DIR)" ]]; then \
		echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi

$(BITS_PROTO_TARBALL): $(PROTO_TARBALL) | $(dir $(BITS_PROTO_TARBALL))
	cp $< $@

$(BITS_AGENT_TARBALL): $(AGENT_TARBALL) | $(dir $(BITS_PROTO_TARBALL))
	cp $< $@

$(dir $(BITS_PROTO_TARBALL)):
	mkdir -p $@
