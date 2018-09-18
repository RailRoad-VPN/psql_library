import logging
from typing import Optional

from psycopg2._psycopg import DatabaseError

from psql_helper import PostgreSQL


class StorageService(object):
    __version__ = 1

    def __init__(self):
        pass

    def create(self, sql: str, data: tuple, is_return: bool = False) -> None:
        pass

    def get(self, sql: str, data: tuple = None) -> dict:
        pass

    def update(self, sql: str, data: tuple, is_return: bool = False) -> None:
        pass

    def delete(self, sql: str, data: tuple) -> None:
        pass

    def rollback(self):
        pass


class StoredObject(object):
    __version__ = 1

    logger = logging.getLogger(__name__)

    _storage_service = None
    _limit = None
    _offset = None

    def __init__(self, storage_service: StorageService, limit: int = None, offset: int = None):
        self._limit = limit
        self._offset = offset
        self._storage_service = storage_service


class DBStorageService(StorageService):
    __version__ = 1

    logger = logging.getLogger(__name__)

    __dbname = None
    __dbuser = None
    __dbpwd = None

    __pool = None
    _current_connection = None

    _psql = None

    def __init__(self, psql: PostgreSQL) -> None:
        super().__init__()
        self._psql = psql

    def create(self, sql: str, data: tuple, is_return: bool = False) -> Optional[list]:
        self.logger.debug('create method.')
        data = self.__execute_sql(sql=sql, data=data, is_return=is_return)
        if is_return:
            return data

    def get(self, sql: str, data: tuple = None) -> Optional[dict]:
        self.logger.debug('get method.')
        data = self.__execute_sql(sql=sql, data=data, is_return=True)
        self.logger.debug(f"{self.__class__}: Fetch size rows: " + str(len(data)))
        return data

    def update(self, sql: str, data: tuple, is_return: bool = False) -> Optional[list]:
        self.logger.debug('update method.')
        self.__execute_sql(sql=sql, data=data, is_return=is_return)
        data = self.__execute_sql(sql=sql, data=data, is_return=is_return)
        if is_return:
            return data

    def delete(self, sql: str, data: tuple = None) -> None:
        self.logger.debug('delete method.')
        self.__execute_sql(sql=sql, data=data, is_return=False)

    def __execute_sql(self, sql: str, is_return: bool, data: tuple = None) -> Optional[list]:
        self.logger.debug('__execute_sql method.')
        sql = sql.replace("?", "%s")
        log_txt = "\nSQL: %s\nParameters: %s " % (sql, data)
        self.logger.debug(log_txt)
        self.logger.debug(f"{self.__class__}: Create cursor")
        try:
            with self._psql.cursor() as cursor:
                self.logger.debug(f"{self.__class__}: Executing...")
                cursor.execute(sql, data)
                if is_return:
                    response = cursor.fetchall()
                    return response
                else:
                    return []
        except DatabaseError as err:
            logging.error("Database Error happened. Code: %s . Message: %s" % (err.pgcode, err.pgerror))
            logging.error(err)
            self.rollback()
            raise DatabaseError(err)
