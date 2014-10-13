all:

WGET = wget
CURL = curl
GIT = git

updatenightly: local/bin/pmbp.pl \
    clean-lib/AnyEvent/MySQL/Client/Promise.pm \
    lib/AnyEvent/MySQL/Client/Promise.pm
	$(CURL) -s -S -L https://gist.githubusercontent.com/wakaba/34a71d3137a52abb562d/raw/gistfile1.txt | sh
	$(GIT) add t_deps/modules
	perl local/bin/pmbp.pl --update
	$(GIT) add config lib

## ------ Setup ------

deps: git-submodules pmbp-install lib/AnyEvent/MySQL/Client/Promise.pm

git-submodules:
	$(GIT) submodule update --init

PMBP_OPTIONS=

local/bin/pmbp.pl:
	mkdir -p local/bin
	$(CURL) -s -S -L https://raw.githubusercontent.com/wakaba/perl-setupenv/master/bin/pmbp.pl > $@
pmbp-upgrade: local/bin/pmbp.pl
	perl local/bin/pmbp.pl $(PMBP_OPTIONS) --update-pmbp-pl
pmbp-update: git-submodules pmbp-upgrade
	perl local/bin/pmbp.pl $(PMBP_OPTIONS) --update
pmbp-install: pmbp-upgrade
	perl local/bin/pmbp.pl $(PMBP_OPTIONS) --install

lib/AnyEvent/MySQL/Client/Promise.pm:
	mkdir -p lib/AnyEvent/MySQL/Client
	$(CURL) -s -S -L https://raw.githubusercontent.com/wakaba/perl-promise/master/lib/Promise.pm | \
	perl -n -e 's/package Promise/package AnyEvent::MySQL::Client::Promise/g; print' > $@
	perl -c $@

clean-lib/AnyEvent/MySQL/Client/Promise.pm:
	rm -fr lib/AnyEvent/MySQL/Client/Promise.pm

## ------ Tests ------

PROVE = ./prove

test: test-deps test-main

test-deps: deps

test-main:
	$(PROVE) t/*.t