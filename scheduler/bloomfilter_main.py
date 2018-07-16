# -*- coding: utf-8 -*-

from bloomfilter import BloomFilter


def run():
    bf = BloomFilter(shm_key=0x00912323)
    print bf.exist("http://1.com/", True)
    print bf.exist("http://1.com/", True)
    print bf.exist("http://1.com/")


if __name__ == "__main__":
    run()
