import logging


class ReportGenerator(object):

    def __init__(self, f):
        self.f = f

    def __get__(self, instance, owner):
        self.cls = owner
        self.obj = instance

        return self.__call__

    def __call__(self, *args, **kwargs):
        print self

        return self.f(*args, **kwargs)

    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)
