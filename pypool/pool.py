import Queue
import time
from datetime import datetime
from threading import Thread, Lock


class EmptyPool(Exception):
    pass


class ConnectionPool(object):

    class Connection(object):
        '''Abstraction used by the pool to keep the
        track of a resource'''

        def __init__(self, resource, enter, return_connection):
            self.resource = resource
            self.enter = enter
            self.return_connection = return_connection
            self.create_time = datetime.now()

        def __enter__(self):
            '''Execute any hooks'''
            self.enter(self)

        def __exit__(self, type, value, traceback):
            '''Teardown things'''

            if traceback is not None:
                print 'Error: {}'.format(traceback)
            # Even though we might have an exception in the
            # clause, we should return the connection to the pool
            # and wait for the keepalive thread to check for aliveness.
            self.return_connection(self)

        def ping(self):
            try:
                self.resource.ping()
                return True
            except:
                return False

        def close(self):
            try:
                self.resource.close()
            except:
                pass

    def __init__(self, factory,
                 maxsize=10,
                 connection_ping_rate_sec=60,
                 connection_timeout_sec=600):

        self.factory = factory
        self.active = 0
        self.maxsize = maxsize
        self.queue = Queue.Queue(maxsize=maxsize)
        self.lock = Lock()
        t = Thread(name='keepalive-connections',
                   target=self._keepalive,
                   kwargs={
                       'connection_ping_rate_sec': connection_ping_rate_sec,
                       'connection_timeout_sec': connection_timeout_sec
                   })
        t.daemon = True
        t.start()

    def enter(self, connection):
        '''Before the work could begin, execute any hooks on the connection'''
        pass

    def return_connection(self, connection):
        ''' Return the connection to the pool to be utilized by other
        operations'''
        self.queue.put(connection)

    def get_connection(self, block=True, timeout=3):

        def _get(block, timeout):
            return self.queue.get(block=block, timeout=timeout)

        def _create():
            ''' Try and create a new instance of connection
            object'''

            connection = None
            self.lock.acquire()
            try:
                if self.active < self.maxsize:
                    resource = self.factory.create()
                    self.active += 1
                    connection = ConnectionPool.Connection(resource=resource, enter=self.enter,
                                                           return_connection=self.return_connection)

            except Exception as ex:
                print 'Unable to create resource: {}'.format(str(ex))
                raise ex

            finally:
                self.lock.release()

            return connection

        # Stage 1: Check if we have any connection free
        # in the pool.
        connection = None
        try:
            connection = _get(block=False, timeout=0)
        except Queue.Empty:
            # Stage 2: Try and create the connection
            # in case we have none available in the
            # pool.
            connection = _create()

        try:
            if connection is None:
                # Stage 3: Finally, block for a timeout
                # till you get the connection. This is best
                # effort and in case we still are empty handed,
                # raise Exception.
                connection = _get(block=block, timeout=timeout)
        except Queue.Empty:
            raise EmptyPool('No Connection available in pool')

        return connection

    def _keepalive(self, connection_ping_rate_sec,
                   connection_timeout_sec):

        def remove_connection(connection):

            if connection is None:
                return

            self.lock.acquire()
            try:
                connection.close()
            finally:
                self.active -= 1
                self.lock.release()

        print 'Starting background process to ping connections'
        while True:

            try:
                now = datetime.now()
                connection = self.queue.get(block=False)

                if (now - connection.create_time).seconds >= connection_timeout_sec:
                    print 'Connection alive for more than required duration, killing it'
                    remove_connection(connection)
                    continue

                if not connection.ping():
                    # release the connection
                    # and its resources.
                    remove_connection(connection)
                    continue

                print 'Successfully pinged the connection, putting it back in the pool'
                self.return_connection(connection)

            except Queue.Empty:
                print 'No connection available to test, sleeping'
                time.sleep(connection_ping_rate_sec)
