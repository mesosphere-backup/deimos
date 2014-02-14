.PHONY: proto
proto: proto/mesos.proto
	protoc --proto_path=proto/ --python_out=lib/medea/ proto/mesos.proto


