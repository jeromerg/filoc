import socket
import threading
from filoc.core import LockException
import json
from logging import exception
import os
import shutil
import tempfile
from threading import Thread
import time
import unittest
from pathlib import Path

from filoc import filoc, filoc_json, FilocIO


# noinspection DuplicatedCode
class TestFilocConcurrency(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.loc = filoc(self.test_dir + r'/id={id:d}/myfile.json', writable=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_lock_block(self):
        with self.loc.lock():
            self.loc.write_content({'id' : 1, 'val' : 1})
            res = self.loc.read_content(id=1)
            print(res)

    def test_lock_info(self):
        host   = socket.gethostname()
        pid    = os.getpid()
        thread = threading.get_ident()

        self.assertIsNone(self.loc.lock_info())

        with self.loc.lock():            
            lock_info = self.loc.lock_info()
            
            self.assertEqual(lock_info['host'], host)
            self.assertEqual(lock_info['pid'], pid)
            self.assertEqual(lock_info['thread'], thread)
            
            self.loc.write_content({'id' : 1, 'val' : 1})
            res = self.loc.read_content(id=1)
            print(res)

        self.assertIsNone(self.loc.lock_info())

    def test_lock_force(self):
        self.loc.lock().__enter__()            
        self.loc.lock_force_release()
        self.assertIsNone(self.loc.lock_info())

    def test_lock_force(self):
        with self.loc.lock():
            self.loc.lock_force_release()
            self.assertIsNone(self.loc.lock_info())

    def test_lock_reenter(self):
        with self.loc.lock():
            with self.loc.lock():
                pass

    def test_lock_enter_from_other_thread(self):
        thread = Thread(target=lambda:self.loc.lock().__enter__())
        thread.start()
        thread.join()

        try:
            with self.loc.lock():
                self.fail("this line should never be called")
        except LockException:
            print("lock worked")


if __name__ == '__main__':
    unittest.main()
