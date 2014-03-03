#!/usr/bin/env python

from setuptools import setup

setup(name                   =  "deimos",
      license                =  "Apache",
      version                =  "0.0.0",
      install_requires       =  ["protobuf"],
      description            =  "Mesos containerization hooks for Docker",
      author                 =  "Jason Dusek",
      author_email           =  "jason.dusek@gmail.com",
      maintainer             =  "Mesosphere",
      maintainer_email       =  "support@mesosphere.io",
      url                    =  "https://github.com/mesosphere/deimos",
      packages               =  ["deimos"],
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
