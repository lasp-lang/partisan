#  -------------------------------------------------------------------
#
#  Copyright (c) 2014 Basho Technologies, Inc.
#
#  This file is provided to you under the Apache License,
#  Version 2.0 (the "License"); you may not use this file
#  except in compliance with the License.  You may obtain
#  a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#
#  -------------------------------------------------------------------

#  -------------------------------------------------------------------
#  NOTE: This file is is from https://github.com/basho/tools.mk.
#  It should not be edited in a project. It should simply be updated
#  wholesale when a new version of tools.mk is released.
#  -------------------------------------------------------------------

REBAR            = $(shell pwd)/rebar3
REVISION 		    ?= $(shell git rev-parse --short HEAD)
PROJECT         ?= $(shell basename `find src -name "*.app.src"` .app.src)
DEP_DIR         ?= "deps"
EBIN_DIR        ?= "ebin"

.PHONY: compile-no-deps test docs xref dialyzer-run dialyzer-quick dialyzer \
		cleanplt upload-docs

compile-no-deps:
	${REBAR} compile skip_deps=true

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer: compile
	${REBAR} dialyzer
