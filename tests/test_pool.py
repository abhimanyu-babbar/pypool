import unittest
from pypool import pool


class TestResourceFactory():

    def create(self):
        return TestResource()


class TestResource(object):

    def ping(self):
        return True

    def close(self):
        return True


class TestConnectionPool(unittest.TestCase):

    def setUp(self):
        self.pool = pool.ConnectionPool(factory=TestResourceFactory(), maxsize=2)

    def test_connection_pool_sunshine(self):

        connection1 = self.pool.get_connection()
        connection2 = self.pool.get_connection()

        self.assertIsNotNone(connection1)
        self.assertIsNotNone(connection2)

    def test_connection_pool_empty(self):
        self.pool.get_connection()
        self.pool.get_connection()

        with self.assertRaises(pool.EmptyPool):
            self.pool.get_connection()
