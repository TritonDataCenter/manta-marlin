#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

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
# MARLIN BUILD SYSTEM
#
# The Marlin build system is substantially more complex than most Node module
# repositories because it represents several distinct but tightly coupled
# components with overlapping dependencies.  There are four logical groups of
# code:
#
#     agent/		Marlin agent code
#     jobsupervisor/	Marlin jobsupervisor service code
#     client/		Marlin (moray) client code
#     common/		Code used by all of the above
#
# We keep all these in the same repository because they are inherently tightly
# coupled (e.g., they share internal structure definitions in common code), and
# the git repository is a useful consistency abstraction.
#
# The main consumers of the Marlin client are muskie, mackerel, and developers
# installing this package on OS X to use the debugging tools against remote
# deployments.  In order to support these clients without pulling in the (many)
# additional dependencies required to support the rest of Marlin, the
# package.json at the root of this repository represents only the client package
# and contains only the dependencies required to run the client library and
# tools.
#
# The other two consumers are builds of the marlin agent and jobsupervisor
# image.  To build these, we construct a proto area by merging the above four
# directories and then build a tarball of that.  The agent and jobsupervisor
# tarballs are logically different, but they're currently built from the exact
# same proto area, which contains a single package.json that describes the union
# of each components' dependencies.  These could be separated in the future for
# cleanliness.
#
# Finally, developers also need to be able to run the tools and the test suite.
# The tools work when run either directly out of the repo or from the proto
# area, but the test suite must be run from the proto area, since the common
# package.json contains test suite dependencies as well.
#
# Because of all this, the "all" target in this repo builds both the client
# package at the root of this repository as well as the complete proto area.
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
CATEST		 = dev/tools/catest
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
    agent/sbin/mragentdestroy		\
    agent/sbin/mrdeploycompute		\
    agent/sbin/mrgroups			\
    agent/sbin/mrlogexpire.sh		\
    agent/sbin/mrzone			\
    agent/sbin/mrzonedisable		\
    agent/sbin/mrzoneremove		\
    agent/sbin/mrzones			\
    client/sbin/mrjobreport		\
    client/sbin/mrereport		\
    client/sbin/mrerrors		\
    client/sbin/mrextractjob		\
    dev/tools/catest			\
    dev/tools/endtoend.sh		\
    dev/tools/mru			\
    jobsupervisor/boot/setup.sh		\
    jobsupervisor/sbin/mrsup

DOC_FILES	 = index.restdown ops.restdown

JSON_DIRS        = $(COMPONENT_DIRS:%=%/package.json %/etc %/sapi_manifests)
JSON_DIRS	+= $(COMPONENT_DIRS:%=%/etc)
JSON_DIRS	+= $(COMPONENT_DIRS:%=%/sapi_manifests)
JSON_FILES	:= $(shell find $(JSON_DIRS) -name '*.json' 2>/dev/null)
JSON_FILES	+= jobsupervisor/sapi_manifests/marlin/template
JSON_FILES	+= package.json

JS_DIRS		 = $(COMPONENT_DIRS)
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
    NODE_PREBUILT_VERSION = v0.10.24
    NODE_PREBUILT_TAG = zone
    NODE_PREBUILT_IMAGE = fd2cc906-8938-11e3-beab-4359c665ac99

    include ./dev/tools/mk/Makefile.node_prebuilt.defs
else
    NPM_EXEC :=
    NPM = npm
endif

#
# Repo-specific targets
#
CFLAGS		+= -Wall -Werror
EXECS   	 = dev/test/mallocbomb/mallocbomb
CLEANFILES	+= $(EXECS)

#
# The default ("all") target is used by developers to build the client package
# at the root of the repo as well as the full proto area.
#
.PHONY: all
all: deps proto

#
# The "deps" target builds the Node dependency and the installs the npm
# dependencies of the *client* package.
#
.PHONY: deps
deps: | $(NPM_EXEC)
	$(NPM_ENV) $(NPM) --no-rebuild install

.PHONY: test
test: all
	$(CATEST) -a

dev/test/mallocbomb/mallocbomb: dev/test/mallocbomb/mallocbomb.c
CLEAN_FILES += dev/test/mallocbomb/mallocbomb

DISTCLEAN_FILES += node_modules

include ./dev/tools/mk/Makefile.deps
include ./dev/tools/mk/Makefile.smf.targ
include ./dev/tools/mk/Makefile.targ

ifneq ($(USE_LOCAL_NODE),true)
    include ./dev/tools/mk/Makefile.node_prebuilt.targ
endif

#
# This rule installs the common manta scripts into the build/scripts directory.
#
scripts: deps/manta-scripts/.git
	mkdir -p $(BUILD)/scripts
	cp deps/manta-scripts/*.sh $(BUILD)/scripts

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
MARLIN_DEV    := $(shell cd dev && find package.json test -type f -not -name .*.swp -not -name mallocbomb)
MARLIN_DEV    += test/mallocbomb/mallocbomb
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
PROTO_MARLIN_FILES += $(PROTO_MARLIN_ROOT)/boot/scripts
$(PROTO_MARLIN_ROOT)/boot/scripts: $(BUILD)/scripts
	mkdir -p $(@D) && cp -r $^ $@
$(BUILD)/scripts: scripts

PROTO_MARLIN_BUILD  = \
    $(PROTO_MARLIN_ROOT)/$(BUILD)/docs \
    $(PROTO_MARLIN_ROOT)/$(BUILD)/node

PROTO_MARLIN_FILES += $(PROTO_MARLIN_BUILD)
PROTO_FILES += $(PROTO_MARLIN_FILES)
$(PROTO_MARLIN_ROOT)/$(BUILD)/node: | $(BUILD)/node
	mkdir -p $(@D) && cp -r $(BUILD)/node $@
$(BUILD)/node: $(NODE_EXEC)

$(PROTO_MARLIN_ROOT)/$(BUILD)/docs: docs
	rm -rf "$@" && mkdir -p "$(@D)" && cp -r $(BUILD)/docs "$@"

PROTO_FILES += $(PROTO_BOOT_ROOT)/setup.sh
$(PROTO_BOOT_ROOT)/setup.sh:
	mkdir -p $(@D) && ln -fs /opt/smartdc/marlin/boot/setup.sh $@

#
# We deliver two sets of tools for use by users inside Marlin zones: the
# node-manta tools, and the manta-compute-bin tools.  However, these are
# mostly Node programs, some of which contain binary modules, which means we
# must run them using the same Node that we build them with -- which is our
# Node.  They generally use the first "node" they find in the environment, but
# we want /opt/local/bin/node first on the path, since a *user* invoking Node
# should always get a stock pkgsrc version, not whatever Marlin happened to be
# built with.  In order to support having these tools on the PATH and having
# them use our Node rather than the one on the user's PATH, we create a bunch of
# shims that invoke the proper Node.
#
# Making things worse, we don't even know which tools we need to do this for
# until after we've installed node_modules into the proto area.  For simplicity,
# we just define them here, but if this becomes a maintenance burden, we could
# define a target that re-invokes "make" again after node_modules has been
# installed.
#
USER_TOOLS_MANTA = mfind mget mjob mln mlogin mls mmd5 mmkdir mput mrm mrmdir \
    msign muntar
USER_TOOLS_MCB = maggr mcat mpipe msplit mtee
PROTO_USER_TOOLS_ROOT = $(PROTO_MARLIN_ROOT)/ubin

PROTO_USER_TOOLS_MANTA = $(USER_TOOLS_MANTA:%=$(PROTO_USER_TOOLS_ROOT)/%)
PROTO_USER_TOOLS_MCB = $(USER_TOOLS_MCB:%=$(PROTO_USER_TOOLS_ROOT)/%)
PROTO_MARLIN_AGENT += $(PROTO_USER_TOOLS_MANTA) $(PROTO_USER_TOOLS_MCB)

$(PROTO_USER_TOOLS_MANTA): $(PROTO_USER_TOOLS_ROOT)/%: dev/stub_template
	mkdir -p $(@D) && \
	    sed -e 's#@@ARG0@@#$*#g' \
	        -e 's#@@CMDBASE@@#manta/bin#g' \
	        dev/stub_template > $@ && chmod 755 $@

$(PROTO_USER_TOOLS_MCB): $(PROTO_USER_TOOLS_ROOT)/%: dev/stub_template
	mkdir -p $(@D) && \
	    sed -e 's#@@ARG0@@#$*#g' \
	        -e 's#@@CMDBASE@@#manta-compute-bin/bin#g' \
	        dev/stub_template > $@ && chmod 755 $@


#
# Some tools were historically delivered in "tools", but really belong on
# "sbin".  We moved these to "sbin", but we add symlinks into "tools".
#
LINKS_AGENT_BIN = mragentconf mragentdestroy mrdeploycompute mrzone mrzoneremove
PROTO_LINKS_AGENT_BIN = $(LINKS_AGENT_BIN:%=$(PROTO_MARLIN_ROOT)/tools/%)
PROTO_MARLIN_FILES += $(PROTO_LINKS_AGENT_BIN)
$(PROTO_MARLIN_ROOT)/tools/%: | agent/sbin/%
	mkdir -p $(@D) && ln -fs ../sbin/$* $@

PROTO_FILES += $(PROTO_SITE_ROOT)/.do-not-delete-me
$(PROTO_SITE_ROOT)/.do-not-delete-me:
	mkdir -p $(@D) && touch $@

.PHONY: proto
proto: $(PROTO_FILES) proto_deps

.PHONY: proto_files
proto_files: $(PROTO_FILES)

.PHONY: proto_deps
proto_deps: $(PROTO_FILES)
	cd $(PROTO_MARLIN_ROOT) && $(NPM_ENV) $(NPM) --no-rebuild install

#
# Mountain Gorilla targets
#
MG_NAME 		 = marlin
MG_PROTO		 = $(PROTO_ROOT)
MG_IMAGEROOT		 = $(PROTO_MARLIN_ROOT)
MG_RELEASE_TARBALL	 = $(MG_NAME)-pkg-$(STAMP).tar.bz2
PROTO_TARBALL 		 = $(BUILD)/$(MG_RELEASE_TARBALL)
BITS_PROTO_TARBALL	 = $(BITS_DIR)/$(MG_NAME)/$(MG_RELEASE_TARBALL)

MG_AGENT_TARBALL	 = $(MG_NAME)-$(STAMP).tar.gz
MG_AGENT_MANIFEST	 = $(MG_NAME)-$(STAMP).manifest
AGENT_TARBALL	 	 = $(BUILD)/$(MG_AGENT_TARBALL)
AGENT_MANIFEST	 	 = $(BUILD)/$(MG_AGENT_MANIFEST)
BITS_AGENT_TARBALL	 = $(BITS_DIR)/$(MG_NAME)/$(MG_AGENT_TARBALL)
BITS_AGENT_MANIFEST	 = $(BITS_DIR)/$(MG_NAME)/$(MG_AGENT_MANIFEST)

CLEAN_FILES		+= $(MG_PROTO) \
			   $(BUILD)/$(MG_NAME)-pkg-*.tar.bz2 \
			   $(BUILD)/$(MG_NAME)-*.tar.gz \
			   $(BUILD)/$(MG_NAME)-*.manifest

#
# "release" target creates two tarballs: the first to be used as an input for
# the jobworker zone, the second to be installed in each system's global zone
# via apm.
#
.PHONY: release
release: $(PROTO_TARBALL) $(AGENT_TARBALL) $(AGENT_MANIFEST)

$(PROTO_TARBALL): proto
	$(TAR) -C $(MG_PROTO) -cjf $@ root site

$(AGENT_TARBALL): proto
	uuid -v4 > $(MG_PROTO)/root/opt/smartdc/$(MG_NAME)/image_uuid
	$(TAR) -C $(MG_PROTO)/root/opt/smartdc -czf $@ $(MG_NAME)

$(AGENT_MANIFEST): $(AGENT_TARBALL)
	cat agent/manifest.tmpl | sed \
		-e "s/UUID/$$(cat $(MG_PROTO)/root/opt/smartdc/$(MG_NAME)/image_uuid)/" \
		-e "s/VERSION/$$(json version < package.json)/" \
		-e "s/BUILDSTAMP/$(STAMP)/" \
		-e "s/SIZE/$$(stat --printf="%s" $(AGENT_TARBALL))/" \
		-e "s/SHA/$$(openssl sha1 $(AGENT_TARBALL) \
		    | cut -d ' ' -f2)/" \
		> $@

#
# "publish" target copies the release tarball into BITS_DIR.
#
.PHONY: publish
publish: check-bitsdir $(BITS_PROTO_TARBALL) $(BITS_AGENT_TARBALL) \
	$(BITS_AGENT_MANIFEST)

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

$(BITS_AGENT_MANIFEST): $(AGENT_MANIFEST) | $(dir $(BITS_PROTO_TARBALL))
	cp $< $@

$(dir $(BITS_PROTO_TARBALL)):
	mkdir -p $@
