import psycopg2
import psycopg2.extras
from time import sleep

from user import panic, print_err, command_error, PANIC_DB_ERROR_OCCURRED, PANIC_DB_RETRIED_ERROR


def retry(name, onerror):
    def _retry(operation):
        def wrapper(self):
            return self.retry_operation(name, operation, onerror)

        return wrapper

    return _retry


class DbOperation:
    @staticmethod
    def retry_no_reconnect(_self, name, operation, retries, onerror):
        def wrapper(*args, **kwargs):
            failed = False
            for i in range(retries + 1):
                if failed:
                    print_err(f"\r\x1b[1K\rUnable to perform operation {name}! Retrying ({i}/{retries})...")
                try:
                    result = operation(*args, **kwargs)
                except psycopg2.OperationalError as e:
                    sleep(1)
                    failed = True
                except psycopg2.InterfaceError as e:
                    raise e
                except psycopg2.Error as e:
                    onerror(e)
                else:
                    if failed:
                        print_err(f"Operation {name} succeeded after {i} retries, continuing...")
                    return result
            panic(f"Unable to perform {name} ({retries} retries)! Exiting...", PANIC_DB_RETRIED_ERROR)

        return wrapper

    def retry_operation(self, name, operation, onerror):
        while True:
            try:
                return DbOperation.retry_no_reconnect(self, name, operation, self.db.retries, onerror)(self)
            except psycopg2.InterfaceError:
                print_err(f"Operation {name} failed due to InterfaceError, reconnecting to db...")
                self.connect()

    def __init__(self, db):
        self.db = db

    @retry("DB_CONNECT", lambda e: panic(f"Error occurred:\n{e}", PANIC_DB_ERROR_OCCURRED))
    def connect(self):
        self.db.conn = psycopg2.connect(**self.db.auth)
        self.db.curr = self.db.conn.cursor()

    def fetchall(self, name, command, data):
        @retry(name, lambda e: command_error(name, e, command, data))
        def _execute(inner_self):
            inner_self.db.curr.execute(command, data)
            return self.db.curr.fetchall()

        return _execute(self)

    def fetchone(self, name, command, data):
        @retry(name, lambda e: command_error(name, e, command, data))
        def _execute(inner_self):
            inner_self.db.curr.execute(command, data)
            a = self.db.curr.fetchone()
            return a

        return _execute(self)

    def execute(self, name, command, data):
        @retry(name, lambda e: command_error(name, e, command, data))
        def _execute(inner_self):
            inner_self.db.curr.execute(command, data)

        _execute(self)

    def execute_batch(self, name, command, data):
        @retry(name, lambda e: command_error(name, e, command, data))
        def _execute(inner_self):
            psycopg2.extras.execute_batch(inner_self.db.curr, command, data)

        _execute(self)

    def try_advisory_lock(self, lock_id, operation_name="TRY ADVISORY LOCK"):
        return self.fetchone(operation_name,
                             'SELECT pg_try_advisory_lock(%s)',
                             (lock_id,))[0]

    def advisory_unlock(self, lock_id, operation_name="ADVISORY UNLOCK"):
        return self.fetchone(operation_name,
                             'SELECT pg_advisory_unlock(%s)',
                             (lock_id,))[0]

    def revoke_privileges(self, table_name, operation_name="REVOKE PRIVILEGES"):
        self.execute(operation_name,
                     f'REVOKE ALL PRIVILEGES ON "{table_name}" FROM PUBLIC', ())

    def grant_privileges(self, table_name, operation_name="GRANT PRIVILEGES"):
        self.execute(operation_name,
                     f'GRANT ALL PRIVILEGES ON "{table_name}" FROM PUBLIC', ())

    def check_table_exists(self, table_name, operation_name="CHECK TABLE EXISTS"):
        return self.fetchone(operation_name,
                             'SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = %s)',
                             (table_name,))[0]

    def create_table(self, table_name, table_schema, operation_name="CREATE TABLE"):
        table_schema_str = ', '.join([f"{col_name} {col_type}" for col_name, col_type in table_schema.items()])
        self.execute(operation_name,
                     f'CREATE TABLE "{table_name}" ({table_schema_str})', ())

    def drop_table(self, table_name, operation_name="DROP TABLE"):
        self.execute(operation_name,
                     f'DROP TABLE "{table_name}"', ())

    def insert_many_into_table(self, table_name, names, values, operation_name="INSERT MANY INTO TABLE"):
        val_format_str = ", ".join(["%s"] * len(names))
        names_str = ", ".join(map(str, names))
        self.execute_batch(operation_name,
                           f'INSERT INTO "{table_name}" ({names_str}) VALUES ({val_format_str})',
                           values)

    def get_table_column_types(self, table_name, operation_name="SELECT TABLE COLUMNS"):
        return self.fetchall(operation_name,
                             f'SELECT column_name,data_type FROM information_schema.columns '
                             f'WHERE table_name = %s',
                             (table_name,))

    def close(self):
        self.db.conn.commit()
        self.db.conn.close()


class Db:
    def __init__(self, auth, retries):
        self.auth = auth
        self.retries = retries
        self.conn = None
        self.curr = None

    def connect(self):
        DbOperation(self).connect()

    def disconnect(self):
        DbOperation(self).close()

    def commit(self):
        self.conn.commit()
