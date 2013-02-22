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
USE_LOCAL_NODE=false

#
# Tools
#
BASHSTYLE	 = $(NODE) tools/bashstyle
CATEST		 = tools/catest

#
# Files
#
BASH_FILES	 = \
    npm/postinstall.sh		\
    npm/preuninstall.sh		\
    tools/mparams		\
    tools/mrdeploycompute	\
    tools/mrpost		\
    tools/mrzone		\
    tools/mrzoneremove		\
    tools/mrzonestag
DOC_FILES	 = index.restdown
JS_FILES	:= $(shell find src lib test -name '*.js')
JS_FILES	+= \
    bin/maggr	\
    bin/mcat	\
    bin/mlocate \
    bin/mpipe	\
    bin/mrjob 	\
    bin/msplit	\
    bin/mtee	\
    tools/mrpound

JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
SMF_MANIFESTS_IN = \
    smf/manifests/marlin-agent.xml.in \
    smf/manifests/marlin-lackey.xml.in

#
# v8plus uses the CTF tools as part of its build, but they can safely be
# overridden here so that this works in dev zones without them.
#
NPM_ENV		 = MAKE_OVERRIDES="CTFCONVERT=/bin/true CTFMERGE=/bin/true"

include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.smf.defs
include ./tools/mk/Makefile.node_deps.defs

ifneq ($(USE_LOCAL_NODE),true)
    REPO_MODULES     = src/node-hyprlofs
    NODE_PREBUILT_VERSION = v0.8.20
    NODE_PREBUILT_TAG = zone

    include ./tools/mk/Makefile.node_prebuilt.defs
else
    NPM_EXEC :=
    NPM = npm
endif

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
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ

ifneq ($(USE_LOCAL_NODE),true)
    include ./tools/mk/Makefile.node_prebuilt.targ
endif
