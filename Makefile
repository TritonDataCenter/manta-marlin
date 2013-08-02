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
    agent/smf/marlin-agent.xml.in \
    agent/smf/marlin-lackey.xml.in

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

# XXX need to build the agent, jobsupervisor, and dev packages
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

#include ./Makefile.mg.targ
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
PROTO_SMARTDC_ROOT=$(PROTO_ROOT)/root/opt/smartdc
PROTO_MARLIN_ROOT=$(PROTO_SMARTDC_ROOT)/marlin
PROTO_BOOT_ROOT=$(PROTO_SMARTDC_ROOT)/boot

MARLIN_AGENT  := $(shell cd agent  && find * -type f -not -name package.json)
MARLIN_COMMON := $(shell cd common && find * -type f -not -name package.json)
MARLIN_DEV    := $(shell cd dev && find * -type f -not -name package.json)
MARLIN_JOBSUP := $(shell cd jobsupervisor  && find * -type f -not -name package.json)

PROTO_MARLIN_AGENT = $(MARLIN_AGENT:%=$(PROTO_MARLIN_ROOT)/%)
PROTO_MARLIN_COMMON = $(MARLIN_COMMON:%=$(PROTO_MARLIN_ROOT)/%)
PROTO_MARLIN_DEV = $(MARLIN_DEV:%=$(PROTO_MARLIN_ROOT)/%)
PROTO_MARLIN_JOBSUP = $(MARLIN_JOBSUP:%=$(PROTO_MARLIN_ROOT)/%)

$(PROTO_MARLIN_AGENT): $(PROTO_MARLIN_ROOT)/%: agent/%
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
    $(PROTO_MARLIN_DEV) \
    $(PROTO_MARLIN_JOBSUP)

#
# It's unfortunate that build/node and build/docs are referenced by directory
# rather than by file, since that means we can't do incremental updates when
# individual files change, but this is not a common case.
#
PROTO_MARLIN_BUILD  = \
    $(PROTO_MARLIN_ROOT)/build/docs \
    $(PROTO_MARLIN_ROOT)/build/node

PROTO_MARLIN_FILES += $(PROTO_MARLIN_BUILD)
$(PROTO_MARLIN_ROOT)/build/node: deps
	mkdir -p $(@D) && cp -r build/node $@
$(PROTO_MARLIN_ROOT)/build/docs: docs
	mkdir -p $(@D) && cp -r build/docs $@

PROTO_FILES = $(PROTO_MARLIN_FILES)

.PHONY: proto
proto: $(PROTO_FILES)
