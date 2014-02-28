medea
=====

Mesos containerizer hooks for Docker


Passing Options
---------------

Options to `docker run` are passed as part of the `CommandInfo` structure.

    {
      command = CommandInfo {
        image = "docker:///ubuntu"
        options = ["-c", "10240"]
      }
    }

Some options for Docker, like `-H`, do not apply only to `docker run`. These
can be set in the Medea configuration file.

Medea recognizes Mesos resources that specify ports, CPUs and memory and
translates them in to Docker options.


Logging
-------

Medea logs to the console when run interactively and to syslog when run in the
background. You can configure logging explicitly in the Medea configuration
file.


Medea Configuration File
------------------------

There is an example configuration file in `example.cfg` which documents all
the configuration options. The two config sections that are likely to be most
important in production are:

  * `[docker]`: global Docker options (`--host`)

  * `[log]`: logging settings

Configuration files are searched in this order:

    ./medea.cfg
    ~/.medea
    /etc/medea.cfg
    /usr/etc/medea.cfg
    /usr/local/etc/medea.cfg

Only *one* of these files is used -- the first one found.
