prefix = /usr/local
datadir = $(prefix)/share
confdir = /etc
initdir = $(confdir)/init.d
rubylibdir = $(shell ruby -rrbconfig -e "puts RbConfig::CONFIG['sitelibdir']")
rundir = /var/run

classifier.jar:
	lein uberjar
	mv target/classifier.jar classifier.jar

install-classifier: classifier.jar
	install -d -m 0755 "$(DESTDIR)$(datadir)/classifier"
	install -m 0644 classifier.jar "$(DESTDIR)$(datadir)/classifier"
	install -d -m 0755 "$(DESTDIR)$(confdir)/classifier"
	install -m 0644 ext/classifier.ini "$(DESTDIR)$(confdir)/classifier"
	install -d -m 0700 "$(DESTDIR)$(confdir)/classifier/ssl"

install-terminus:
	install -d -m 0755 "$(DESTDIR)$(rubylibdir)/puppet/indirector/node"
	install -m 0644 puppet/lib/puppet/indirector/node/classifier.rb "$(DESTDIR)$(rubylibdir)/puppet/indirector/node/classifier.rb"
