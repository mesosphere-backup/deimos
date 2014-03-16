import time


def iso(t=time.time()):
    ms  = ("%0.03f" % (t % 1))[1:]
    iso = time.strftime("%FT%T", time.gmtime(t))
    return iso + ms + "Z"

