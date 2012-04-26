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
# Files
#
DOC_FILES	 = index.restdown
JS_FILES	:= $(shell find src lib test -name '*.js')
JS_FILES	+= cmd/dpipe
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
SMF_MANIFESTS_IN = smf/manifests/marlin-zone-agent.xml.in
REPO_MODULES     = src/node-hyprlofs

include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node.defs
include ./tools/mk/Makefile.smf.defs
include ./tools/mk/Makefile.node_deps.defs

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) deps

.PHONY: deps
deps: | $(REPO_DEPS) $(NPM_EXEC)
	$(NPM) --no-rebuild install

DISTCLEAN_FILES += node_modules

include ./tools/mk/Makefile.node_deps.targ
include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
