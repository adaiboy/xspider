# -*- coding: utf-8 -*-

import ctypes


def c_str(string):
    return ctypes.c_char_p(string)


def c_int(value):
    return ctypes.c_int(value)


def c_uint(value):
    return ctypes.c_uint(value)


def c_bool(value):
    return ctypes.c_bool(value)


class BloomFilter(object):
    def __init__(self,
                 seed_num=10,
                 bucket_num=(1 << 31),
                 shm_key=0,
                 dump_path="",
                 reload_path=""):
        self._clib = ctypes.cdll.LoadLibrary("./lib/libbloomfilter.so")
        self._bloomfilter = \
            self._clib.CreateBloomFilter(
                seed_num, bucket_num, shm_key, dump_path, reload_path)

    def __del__(self):
        del self._bloomfilter

    def insert(self, s):
        return self._clib.Query(
            self._bloomfilter, c_str(s), c_uint(len(s)), c_bool(True))

    def exist(self, s, insert=False):
        return self._clib.Query(
            self._bloomfilter, c_str(s), c_uint(len(s)), c_bool(insert))

    def dump(self):
        return self._clib.Dump(self._bloomfilter)

    def load(self):
        return self._clib.Load(self._bloomfilter)

    def clear(self):
        return self._clib.Reset(self._bloomfilter)
