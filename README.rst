======
deimos
======

Deimos is a Docker plugin for Mesos, providing external containerization as
described in `MESOS-816`_.

------------
Installation
------------

For a complete installation walkthrough, see `this Gist`_.

Deimos can be installed `from the Cheeseshop`_.

.. code-block:: bash

    pip install deimos

----------------------------
Passing Parameters to Docker
----------------------------

In Mesos, every successful resource offer is ultimately followed up with a
``TaskInfo`` that describes the work to be done. Within the ``TaskInfo`` is a
``CommandInfo`` and within the ``CommandInfo`` there is a ``ContainerInfo``
(following `MESOS-816`_). The ``ContainerInfo`` structure allows specification
of an *image URL* and *container options*. For example:

.. code-block:: c

    {
      container = ContainerInfo {
        image = "docker:///ubuntu"
        options = ["-c", "10240"]
      }
    }

Deimos handles image URLs beginning with ``docker:///`` by stripping the
prefix and using the remainder as the image name. The container options are
passed to ``docker run`` when the task is launched.

If no ``ContainerInfo`` is present in a task, Deimos will still containerize
it, by using the ``--default_container_image`` passed to the slave, or taking
a reasonable guess based on the host's distribution and release.

Some options for Docker, like ``-H``, do not apply only to ``docker run``.
These options should be set in the Deimos configuration file.

Deimos recognizes Mesos resources that specify ports, CPUs and memory and
translates them to appropriate Docker options.


-----------------------------------
Passing Parameters through Marathon
-----------------------------------

Marathon has a REST api to submit JSON-formatted requests to run long-running commands.

From this JSON object, the following keys are used by Deimos:

* ``container`` A nested object with details about what Docker image to run

  * ``image`` What Docker image to run, it may have a custom registry but
    must have a version tag

  * ``options`` A list of extra options to add to the Docker invocation

* ``cmd`` What command to run with Docker inside the image. Deimos
  automatically adds ``/bin/sh -c`` to the front

* ``env`` Extra environment variables to pass to the Docker image

* ``cpus`` How many CPU shares to give to the container, can be fractional,
  gets multiplied by 1024 and added with ``docker run -c``

* ``mem`` How much memory to give to the container, in megabytes

.. code-block:: bash

    curl -v -X POST http://mesos1.it.corp:8080/v2/apps \
            -H Content-Type:application/json -d '{
        "id": "marketing",
        "container": {
          "image": "docker:///registry.int/marketing:latest",
          "options": ["-v", "/srv:/srv"]
        },
        "cmd": "/webapp/script/start.sh",
        "env": {"VAR":"VALUE"},
        "cpus": 2,
        "mem": 768.0,
        "instances": 2
    }'

This turns into a Docker execution line similar to this:

.. code-block:: bash

    docker run --sig-proxy --rm \
               --cidfile /tmp/deimos/mesos/10330424-95c2-4119-b2a5-df8e1d1eead9/cid \
               -w /tmp/mesos-sandbox \
               -v /tmp/deimos/mesos/10330424-95c2-4119-b2a5-df8e1d1eead9/fs:/tmp/mesos-sandbox \
               -v /srv:/srv -p 31014:3000 \
               -c 2048 -m 768m \
               -e PORT=31014 -e PORT0=31014 -e PORTS=31014 -e VAR=VALUE \
               registry.int/marketing:latest sh -c "/webapp/script/start.sh"


-------
Logging
-------

Deimos logs to the console when run interactively and to syslog when run in the
background. You can configure logging explicitly in the Deimos configuration
file.


-------------
Configuration
-------------

There is an example configuration file in ``example.cfg`` which documents all
the configuration options. The two config sections that are likely to be most
important in production are:

* ``[docker]``: global Docker options (``--host``)

* ``[log]``: logging settings

Configuration files are searched in this order:

.. code-block:: bash

    ./deimos.cfg
    ~/.deimos
    /etc/deimos.cfg
    /usr/etc/deimos.cfg
    /usr/local/etc/deimos.cfg

Only one configuration file -- the first one found -- is loaded. To see what
Deimos thinks its configuration is, run ``deimos config``.


-------------------
The State Directory
-------------------

Deimos creates a state directory for each container, by default under
``/tmp/deimos``, where it tracks the container's status, start time and PID.
File locks are maintained for each container to coordinate invocations of
Deimos that start, stop and probe the container.

To clean up state directories belonging to exited containers, invoke Deimos
as follows:

.. code-block:: bash

    deimos state --rm

This task can be run safely from Cron at a regular interval. In the future,
Deimos will not require separate invocation of the ``state`` subcommand for
regular operation.


-------------------------------
Configuring Mesos To Use Deimos
-------------------------------

Only the slave needs to be configured. Set these options:

.. code-block:: bash

    --containerizer_path=/usr/local/bin/deimos --isolation=external

The packaged version of Mesos can also load these options from files:

.. code-block:: bash

    echo /usr/local/bin/deimos    >    /etc/mesos-slave/containerizer_path
    echo external                 >    /etc/mesos-slave/isolation


.. _`from the Cheeseshop`: https://pypi.python.org/pypi/deimos

.. _MESOS-816: https://issues.apache.org/jira/browse/MESOS-816

.. _`this Gist`: https://gist.github.com/solidsnack/10944095

