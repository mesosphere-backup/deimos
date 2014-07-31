tmp := /tmp/deimos
prefix := usr/local
PKG_REL := 0.1.$(shell date -u +'%Y%m%d%H%M%S')

.PHONY: proto
proto: proto/mesos.proto
	protoc --proto_path=proto/ --python_out=deimos/ proto/*.proto

.PHONY: pep8
pep8:
	pep8 --exclude=*_pb2.py --ignore E127 deimos | tee pep8.txt | head -n8

.PHONY: deb
deb: clean freeze
	fpm -C toor -t deb -s dir \
		-n deimos -v `cat deimos/VERSION` --iteration $(PKG_REL) .

.PHONY: rpm
rpm: clean freeze
	fpm -C toor -t rpm -s dir \
		-n deimos -v `cat deimos/VERSION` --iteration $(PKG_REL) .

# You will have to install bbfreeze to create a package `pip install bbfreeze`
# Prep:
# - sudo python setup.py develop
.PHONY: freeze
freeze:
	mkdir -p toor/$(prefix)/bin
	mkdir -p toor/opt/mesosphere/deimos
	cp bin/run toor/$(prefix)/bin/deimos
	cp -R . $(tmp)
	cd $(tmp) && sudo python setup.py bdist_bbfreeze
	# Fix for ubuntu using directories for eggs instead of zips
	sudo chmod a+r $(tmp)/dist/*/protobuf*/EGG-INFO/* || :
	sudo cp -R $(tmp)/dist/*/* toor/opt/mesosphere/deimos

.PHONY: clean
clean:
	rm -rf toor
	rm -rf dist
	rm -rf build
	sudo rm -rf $(tmp)

.PHONY: prep-ubuntu
prep-ubuntu:
	sudo apt-get install ruby-dev python-pip python-dev libz-dev protobuf-compiler
	sudo gem install fpm
	sudo pip install bbfreeze
