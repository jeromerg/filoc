""" Test """
import os
import shutil
import socket
import tempfile
import threading
import time
import unittest
from threading import Thread

from filoc import filoc
from filoc.core import LockException


# noinspection DuplicatedCode,PyMissingOrEmptyDocstring
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

    def test_lock_force2(self):
        with self.loc.lock():
            self.loc.lock_force_release()
            self.assertIsNone(self.loc.lock_info())

    def test_lock_reenter(self):
        with self.loc.lock():
            with self.loc.lock():
                pass

    def test_lock_enter_from_other_thread(self):

        state = 0

        def wait_state_and_increment(expected_state):
            nonlocal state
            while state != expected_state:
                time.sleep(0.1)
            state += 1

        def async_lock():
            with self.loc.lock():
                wait_state_and_increment(1)
                wait_state_and_increment(4)
                    
        thread = Thread(target=async_lock)

        # BEGIN ASYNC PLAY
        wait_state_and_increment(0)  # state 0 --> 1
        thread.start()               # state 1 --> 2 (then lock is set)
        wait_state_and_increment(2)  # state 2 --> 3

        try:
            with self.loc.lock(attempt_count=3, attempt_secs=0.2):
                self.fail("this line should never be called")
        except LockException:
            print("lock worked")
        finally:
            wait_state_and_increment(3)  # state 3 --> 4 (trigger lock release)
            wait_state_and_increment(5)  # state 5 --> 6 (lock released)


if __name__ == '__main__':
    unittest.main()
