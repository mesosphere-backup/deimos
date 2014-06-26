import struct
import sys

import google.protobuf

from deimos.err import Err
from deimos.logger import log


class recordio():  # Really just a namespace

    """
    Read and write length-prefixed Protobufs on the STDIO streams.
    """
    @staticmethod
    def read(cls):
        unpacked = struct.unpack('I', sys.stdin.read(4))
        size = unpacked[0]
        if size <= 0:
            raise Err("Expected non-zero size for Protobuf")
        data = sys.stdin.read(size)
        if len(data) != size:
            raise Err("Expected %d bytes; received %d", size, len(data))
        return deserialize(cls, data)

    @staticmethod
    def write(cls, **properties):
        data = serialize(cls, **properties)
        sys.stdout.write(struct.pack('I', len(data)))
        sys.stdout.write(data)
        pass

    @staticmethod
    def writeProto(proto):
        data = proto.SerializeToString()
        sys.stdout.write(struct.pack('I', len(data)))
        sys.stdout.write(data)
        pass


def serialize(cls, **properties):
    """
    With a Protobuf class and properties as keyword arguments, sets all the
    properties on a new instance of the class and serializes the resulting
    value.
    """
    obj = cls()
    for k, v in properties.iteritems():
        log.debug("%s.%s = %r", cls.__name__, k, v)
        setattr(obj, k, v)
    return obj.SerializeToString()


def deserialize(cls, data):
    obj = cls()
    obj.ParseFromString(data)
    for line in lines(obj):
        log.debug(line)
    return obj


def lines(proto):
    s = google.protobuf.text_format.MessageToString(proto)
    return s.strip().split("\n")
