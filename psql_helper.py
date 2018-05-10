from __future__ import unicode_literals

import logging
import threading

import flask
import psycopg2
import psycopg2.extensions
import psycopg2.extras
from werkzeug.local import LocalProxy

postgres = LocalProxy(lambda: flask.current_app.postgres)


class PostgreSQL(object):
    """
    A PostgreSQL helper extension for Flask apps

    On initialisation it adds an after_request function that commits the
    transaction (so that if the transaction rolls back the request will
    fail) and a app context teardown function that disconnects any active
    connection.

    You can of course (and indeed should) use :meth:`commit` if you need to
    ensure some changes have made it to the database before performing
    some other action. :meth:`teardown` is also available to be called
    directly.

    Connections are created by ``psycopg2.connect(**app.config["POSTGRES"])``
    (e.g., ``app.config["POSTGRES"] = {"database": "mydb"}``),
    are pooled (you can adjust the pool size with `pool`) and are tested for
    server shutdown before being given to the request.
    """

    def __init__(self, app=None, pool_size=2):
        self.app = app
        self._pool = []
        self.pool_size = pool_size
        self._lock = threading.RLock()
        self.logger = logging.getLogger(__name__ + ".PostgreSQL")

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """
        Initialises the app by adding hooks

        * Hook: ``app.after_request(self.commit)``
        * Hook: ``app.teardown_appcontext(self.teardown)``
        """

        psycopg2.extras.register_uuid()
        app.after_request(self.commit)
        app.teardown_appcontext(self.teardown)
        app.postgresql = self

    def _connect(self):
        """Returns a connection to the database"""

        with self._lock:
            c = None

            if len(self._pool):
                c = self._pool.pop()
                try:
                    # This tests if the connection is still alive.
                    c.reset()
                except psycopg2.OperationalError:
                    self.logger.debug("assuming pool dead", exc_info=True)

                    # assume that the entire pool is dead
                    try:
                        c.close()
                    except psycopg2.OperationalError:
                        pass

                    for c in self._pool:
                        try:
                            c.close()
                        except psycopg2.OperationalError:
                            pass

                    self._pool = []
                    c = None
                else:
                    self.logger.debug("got connection from pool")

            if c is None:
                c = self._new_connection()

        return c

    def _new_connection(self):
        """Create a new connection to the database"""
        dbname = flask.current_app.config["PSQL_DBNAME"]
        user = flask.current_app.config["PSQL_USER"]
        password = flask.current_app.config["PSQL_PASSWORD"]
        host = flask.current_app.config["PSQL_HOST"]
        self.logger.debug("connecting dbname=%s, user=%s, pwd=%s, host=%s" % (dbname, user, "REMOVED", host))
        c = psycopg2.connect(dbname=dbname, user=user, password=password, host=host,
                             connection_factory=PostgreSQLConnection)
        return c

    @property
    def connection(self):
        """
        Gets the PostgreSQL connection for this Flask request

        If no connection has been used in this request, it connects to the
        database. Further use of this property will reference the same
        connection

        The connection is committed and closed at the end of the request.
        """

        with self.app.app_context():
            if not hasattr(self.app, '_postgresql'):
                self.app._postgresql = self._connect()
            return self.app._postgresql

    def cursor(self, real_dict_cursor=False, dict_cursor=True):
        """
        Get a new postgres cursor for immediate use during a request

        If a cursor has not yet been used in this request, it connects to the
        database. Further cursors re-use the per-request connection.

        The connection is committed and closed at the end of the request.

        If real_dict_cursor is set, a RealDictCursor is returned
        """

        return self.connection.cursor(real_dict_cursor if real_dict_cursor else dict_cursor)

    def commit(self, response=None):
        """
        (Almost an) alias for self.connection.commit()

        ... except if self.connection has never been used this is a noop
        (i.e., it does nothing)

        Returns `response` unmodified, so that this may be used as an
        :meth:`flask.after_request` function.
        """
        with self.app.app_context():
            if hasattr(self.app, '_postgresql'):
                self.logger.debug("committing")
                self.app._postgresql.commit()
            return response

    def teardown(self, exception):
        """Either return the connection to the pool or close it"""
        g = flask.g
        if hasattr(g, '_postgresql'):
            c = g._postgresql
            del g._postgresql

            with self._lock:
                s = len(self._pool)
                if s >= self.pool_size:
                    self.logger.debug("teardown: pool size %i - closing", s)
                    c.close()
                else:
                    self.logger.debug("teardown: adding to pool, new size %i",
                                      s + 1)
                    c.reset()
                    self._pool.append(c)


class PostgreSQLConnection(psycopg2.extensions.connection):
    """
    A custom `connection_factory` for :func:`psycopg2.connect`.

    This
    * puts the connection into unicode mode (for text)
    * modifies the :meth:`cursor` method of a :class:`psycopg2.connection`,
      facilitating easy acquiring of cursors made from
      :cls:`psycopg2.extras.RealDictCursor`.
    """

    # this may be omitted in py3k
    def __init__(self, *args, **kwargs):
        super(PostgreSQLConnection, self).__init__(*args, **kwargs)
        for type in (psycopg2.extensions.UNICODE,
                     psycopg2.extensions.UNICODEARRAY):
            psycopg2.extensions.register_type(type, self)

    def cursor(self, real_dict_cursor=False):
        """
        Get a new cursor.

        If real_dict_cursor is set, a RealDictCursor is returned
        """

        kwargs = {}
        if real_dict_cursor:
            kwargs["cursor_factory"] = psycopg2.extras.RealDictCursor
        return super(PostgreSQLConnection, self).cursor(**kwargs)
