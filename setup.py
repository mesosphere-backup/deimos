#!/usr/bin/env python

from setuptools import setup
import subprocess
import sys

version = "deimos/VERSION"


def sync_version():
    code = "git describe --tags --exact-match 2>/dev/null"
    try:
        v = subprocess.check_output(code, shell=True)
        with open(version, "w+") as h:
            h.write(v)
    except subprocess.CalledProcessError as e:
        print >>sys.stderr, "Not able to determine version from Git; skipping."

def read_version():
    with open(version) as h:
        return h.read().strip()

sync_version()

setup(name                   =  "deimos",
      license                =  "Apache",
      version                =  read_version(),
      install_requires       =  ["protobuf"],
      description            =  "Mesos containerization hooks for Docker",
      author                 =  "Jason Dusek",
      author_email           =  "jason.dusek@gmail.com",
      maintainer             =  "Mesosphere",
      maintainer_email       =  "support@mesosphere.io",
      url                    =  "https://github.com/mesosphere/deimos",
      packages               =  ["deimos"],
      package_data           =  { "deimos": ["VERSION"] },
      entry_points           =  { "console_scripts": ["deimos = deimos:cli"] },
      classifiers            =  [ "Environment :: Console",
                                  "Intended Audience :: Developers",
                                  "Operating System :: Unix",
                                  "Operating System :: POSIX",
                                  "Programming Language :: Python",
                                  "Topic :: System",
                                  "Topic :: System :: Systems Administration",
                                  "Topic :: Software Development",
                        "License :: OSI Approved :: Apache Software License",
                                  "Development Status :: 4 - Beta" ])
