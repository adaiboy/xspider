# -*- coding: utf-8 -*-

import functools
import random


def retry(attempt):
    def decorateor(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            att = 0
            while att < attempt:
                res = func(*args, **kwargs)
                if not res:
                    att += 1
                    print "retry %d for %s" % (att, func.__name__)
                else:
                    return res
            return None

        return wrapper
    return decorateor


@retry(3)
def cfun(name):
    i = random.randint(1, 10)
    print "get i %d" % i
    if i < 5:
        return name
    else:
        return None


if __name__ == "__main__":
    cfun("hello")
