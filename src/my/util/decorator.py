import logging

class Trace(object):
    def __init__(self, f):
        self.f =f

    def __call__(self, *args, **kwargs):
        print "entering function " + self.f.__name__
        i=0
        for arg in args:
            print "arg {0}: {1}".format(i, arg)
            i =i+1

        return self.f(*args, **kwargs)

    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)
