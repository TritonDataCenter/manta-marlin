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
# Tools
#
CATEST		 = tools/catest

#
# Files
#
BASH_FILES	 = tools/mrzone tools/mrpost tools/mrmakezone
DOC_FILES	 = index.restdown
JS_FILES	:= $(shell find src lib test -name '*.js')
JS_FILES	+= cmd/dpipe cmd/mlocate cmd/mrstat cmd/mrjob
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
SMF_MANIFESTS_IN = \
    smf/manifests/marlin-agent.xml.in \
    smf/manifests/marlin-zone-agent.xml.in \
    smf/manifests/marlin-mock-manta.xml.in

REPO_MODULES     = src/node-hyprlofs

NODE_PREBUILT_VERSION = v0.8.5
NODE_PREBUILT_TAG = zone

#
# v8plus uses the CTF tools as part of its build, but they can safely be
# overridden here so that this works in dev zones without them.
#
NPM_ENV		 = MAKE_OVERRIDES="CTFCONVERT=/bin/true CTFMERGE=/bin/true"

include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node_prebuilt.defs
include ./tools/mk/Makefile.smf.defs
include ./tools/mk/Makefile.node_deps.defs

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) deps

.PHONY: deps
deps: | $(REPO_DEPS) $(NPM_EXEC)
	$(NPM_ENV) $(NPM) --no-rebuild install

.PHONY: test
test: all
	tools/catest -a

DISTCLEAN_FILES += node_modules

include ./Makefile.mg.targ
include ./tools/mk/Makefile.node_deps.targ
include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node_prebuilt.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
