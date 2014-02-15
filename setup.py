#!/usr/bin/env python

from setuptools import setup

setup(name                   =  "medea",
      license                =  "Apache",
      version                =  "0.0",
      install_requires       =  ["protobuf"],
      description            =  "Mesos containerization hooks for Docker",
      author                 =  "Jason Dusek",
      author_email           =  "jason.dusek@gmail.com",
      maintainer             =  "Mesosphere",
      maintainer_email       =  "support@mesosphere.io",
      url                    =  "https://github.com/mesosphere/medea",
      packages               =  ["medea"],
      entry_points           =  { "console_scripts": ["medea = medea:cli"] })
