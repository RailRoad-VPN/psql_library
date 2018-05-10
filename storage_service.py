import logging
from typing import Optional

from psql_helper import PostgreSQL


class StorageService(object):
    __version__ = 1

    def __init__(self):
        pass

    def create(self, sql: str, data: tuple, is_return_field: bool = False, return_field_name: str = None,
               is_commit: bool = True) -> None:
        pass

    def get(self, sql: str, data: tuple = None, is_return_field: bool = False, return_field_name: str = None) -> dict:
        pass

    def update(self, sql: str, data: tuple, is_commit: bool = True) -> None:
        pass

    def delete(self, sql: str, data: tuple, is_commit: bool = True) -> None:
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


class DBStorageService(StorageService):
    __version__ = 1

    __dbname = None
    __dbuser = None
    __dbpwd = None

    __pool = None
    _current_connection = None

    _psql = None

    def __init__(self, psql: PostgreSQL) -> None:
        super().__init__()
        self._psql = psql

    def create(self, sql: str, data: tuple, is_return_field: bool = False, return_field_name: str = None,
               is_commit: bool = True) -> None:
        logging.debug('create method.')
        id = self.__execute_sql(sql=sql, data=data, is_return_field=is_return_field,
                                return_field_name=return_field_name, is_commit=is_commit)
        return id

    def get(self, sql: str, data: tuple = None, is_return_field: bool = False,
            return_field_name: str = None) -> Optional[dict]:
        logging.debug('get method.')
        data = self.__execute_sql(sql=sql, data=data, is_return_field=is_return_field,
                                  return_field_name=return_field_name, is_return=True)
        if not is_return_field:
            logging.debug("Fetch size rows: " + str(len(data)))
        else:
            logging.debug("Return field %s = %s" % (return_field_name, str(data)))
        return data

    def update(self, sql: str, data: tuple, is_commit: bool = True) -> None:
        logging.debug('update method.')
        self.__execute_sql(sql=sql, data=data, is_commit=is_commit)

    def delete(self, sql: str, data: tuple, is_commit: bool = True) -> None:
        logging.debug('delete method.')
        self.__execute_sql(sql=sql, data=data, is_commit=is_commit)

    def __execute_sql(self, sql: str, data: tuple = None, is_return_field: bool = False, return_field_name: str = None,
                      is_commit: bool = True, is_return: bool = False) -> Optional[list]:
        logging.debug('__execute_sql method.')
        sql = sql.replace("?", "%s")
        log_txt = "\nSQL: %s\nParameters: %s " % (sql, data)
        logging.debug(log_txt)
        logging.debug("Create cursor")
        try:
            with self._psql.cursor() as cursor:
                logging.debug("Executing...")
                cursor.execute(sql, data)
                if is_return_field:
                    response = cursor.fetchone()[return_field_name]
                    return response
                elif is_return:
                    response = cursor.fetchall()
                    return response
                else:
                    return []
        except DatabaseError as err:
            logging.error("Database Error happened. Code: %s . Message: %s" % (err.pgcode, err.pgerror))
            logging.error(err)
            self.rollback()
            raise DatabaseError(err)
