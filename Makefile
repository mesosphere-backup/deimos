.PHONY: proto
proto: proto/mesos.proto
	protoc --proto_path=proto/ --python_out=deimos/ proto/mesos.proto


