from fs import Fs
from db import Db, DbOperation
from datafiles import get_file_encoding, get_file_size, format_file_size, strip, strip_arr
from user import get_env, print_flush, panic, PANIC_DB_LOCKED, is_panic


class Populate:
    ADVISORY_LOCK_ID = 54321234

    def __init__(self):
        auth = dict(host=get_env("DB_HOST"),
                    dbname=get_env("POSTGRES_DB"),
                    user=get_env("POSTGRES_USER"),
                    password=get_env("POSTGRES_PASSWORD"))
        retries = int(get_env("RETRIES"))
        self.target_table_name = get_env("TARGET_TABLE_NAME")
        self.aux_table_name = get_env("AUX_TABLE_NAME")

        self.fs = Fs()
        self.db = Db(auth, retries)

    def __enter__(self):
        self.fs.connect()
        self.db.connect()
        if not self.lock():
            panic("Db is locked by another instance, exiting.", PANIC_DB_LOCKED)

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        if is_panic():
            return
        self.unlock()
        self.db.disconnect()
        self.fs.disconnect()

    def lock(self):
        return DbOperation(self.db).try_advisory_lock(Populate.ADVISORY_LOCK_ID,
                                                      "LOCK POPULATE")

    def unlock(self):
        return DbOperation(self.db).advisory_unlock(Populate.ADVISORY_LOCK_ID,
                                                    "UNLOCK POPULATE")

    def revoke(self, target=True, aux=True):
        if target:
            DbOperation(self.db).revoke_privileges(self.target_table_name, "REVOKE TARGET TABLE PRIVILEGES")
        if aux:
            DbOperation(self.db).revoke_privileges(self.aux_table_name, "REVOKE AUX TABLE PRIVILEGES")

    def grant(self, target=True, aux=True):
        if target:
            DbOperation(self.db).grant_privileges(self.target_table_name, "GRANT TARGET TABLE PRIVILEGES")
        if aux:
            DbOperation(self.db).grant_privileges(self.aux_table_name, "GRANT AUX TABLE PRIVILEGES")

    def get_state(self):
        schema_loaded = self.fs.schema is not None

        if not schema_loaded:
            return "no_schema"

        target_table_exists = DbOperation(self.db).check_table_exists(self.target_table_name,
                                                                      "CHECK EXISTS TARGET_TABLE")
        aux_table_exists = DbOperation(self.db).check_table_exists(self.aux_table_name,
                                                                   "CHECK EXISTS AUX_TABLE")
        if target_table_exists:
            if aux_table_exists:
                return "interrupted"
            else:
                return "finished"
        else:
            if aux_table_exists:
                return "inconsistent"
            else:
                return "clear"

    def drop_target(self):
        DbOperation(self.db).drop_table(self.target_table_name, "DROP TARGET TABLE")
        return True

    def drop_aux(self):
        DbOperation(self.db).drop_table(self.aux_table_name, "DROP AUX TABLE")

    def do_query(self):
        print_flush("Executing query for year 2019...")
        year2019 = DbOperation(self.db).fetchall("EXAMPLE QUERY",
                                                 f'SELECT Regname, MIN(PhysBall100) FROM "{self.target_table_name}" '
                                                 f'WHERE PhysTestStatus=%s AND Year=%s GROUP BY Regname;',
                                                 ("Зараховано", 2019))
        print_flush("Saving result...")
        lines = [["Region", "MinBall"]] + year2019
        with open("query2019_result.csv", "w", encoding="utf-8") as f:
            f.writelines([';'.join(line)+'\n' for line in lines])

        print_flush("Executing query for year 2020...")
        year2020 = DbOperation(self.db).fetchall("EXAMPLE QUERY",
                                                 f'SELECT Regname, MIN(PhysBall100) FROM "{self.target_table_name}" '
                                                 f'WHERE PhysTestStatus=%s AND Year=%s GROUP BY Regname;',
                                                 ("Зараховано", 2020))
        print_flush("Saving result...")
        lines = [["Region", "MinBall"]] + year2020
        with open("query2020_result.csv", "w", encoding="utf-8") as f:
            f.writelines([';'.join(line)+'\n' for line in lines])



    @staticmethod
    def parse_sql_val(text, sql_type):
        text = strip(text)
        sql_type = sql_type.upper()
        if sql_type == "SMALLINT":
            return int(text) if text != "null" else None
        elif sql_type == "UUID":
            return text
        elif sql_type == "CHARACTER VARYING":
            return text
        return None

    def start(self):
        column_types = DbOperation(self.db).get_table_column_types(self.target_table_name,
                                                                   "SELECT TARGET TABLE COLUMNS")
        columns = [c[0].upper() for c in column_types]
        entries = DbOperation(self.db).fetchall(f'SELECT FROM AUX TABLE',
                                                f'SELECT * FROM "{self.aux_table_name}" '
                                                f'ORDER BY year, '
                                                f'file_seek DESC '
                                                f'LIMIT 1', ())
        if len(entries) == 0:
            self.drop_aux()
            return True
        file_name, year, file_seek, header_text = entries[0]
        file_size = get_file_size(file_name)
        print_flush(f"Populating from file '{file_name}' ({year}): ", end='')
        with open(file_name, "r", encoding=get_file_encoding(file_name)) as file:
            if file_seek == 0:
                header_text = file.readline().strip()
                DbOperation(self.db).execute("UPDATE AUX HEADER",
                                             f'UPDATE "{self.aux_table_name}" '
                                             f'SET header = %s '
                                             f'WHERE file_name = %s',
                                             (header_text, file_name))
            else:
                header_text = DbOperation(self.db).fetchone("SELECT AUX HEADER",
                                                            f'SELECT header FROM "{self.aux_table_name}" '
                                                            f'WHERE file_name = %s',
                                                            (file_name,))[0]
                file.seek(file_seek)
            header = strip_arr(header_text.split(';'))
            header = [h.upper() for h in header]
            row_ind = [(header.index(c) if c in header else None) for c in columns]
            batch_size = 1000
            while True:
                print_flush(f"\rPopulating from file '{file_name}' ({year}): "
                            f"{format_file_size(file_seek)} / {format_file_size(file_size)} "
                            f"({file_seek / file_size:.2%})", end="")
                end = False
                rows = []
                for i in range(batch_size):
                    line = []
                    prev_line_text = ""
                    while True:
                        line_text = prev_line_text + file.readline().strip()
                        if not line_text:
                            end = True
                            break
                        line = [strip(l) for l in line_text.rstrip().split(';')]
                        if len(line) == len(header):
                            break
                        prev_line_text = line_text
                    if end:
                        break
                    row = []
                    for ind in range(len(columns)):
                        if row_ind[ind] is None:
                            row.append(None)
                        else:
                            lv = line[row_ind[ind]]
                            ct = column_types[ind][1]
                            p = Populate.parse_sql_val(lv, ct)
                            row.append(p)
                    row[columns.index("YEAR")] = year
                    rows.append(row)

                DbOperation(self.db).insert_many_into_table(self.target_table_name,
                                                            columns,
                                                            rows,
                                                            "INSERT INTO TARGET TABLE")

                if end:
                    print_flush(f"{' ' * 35}\r\x1b[1K\rPopulating from file '{file_name}' ({year}): done!")
                    DbOperation(self.db).execute("DELETE FROM AUX TABLE",
                                                 f'DELETE FROM "{self.aux_table_name}" '
                                                 f'WHERE file_name = %s',
                                                 (file_name,))
                    self.commit()
                    return self.start()

                file_seek = file.tell()
                DbOperation(self.db).execute("UPDATE AUX FILE SEEK",
                                             f'UPDATE "{self.aux_table_name}" '
                                             f'SET file_seek = %s '
                                             f'WHERE file_name = %s',
                                             (file_seek, file_name))
                self.commit()

    def prepare(self):
        DbOperation(self.db).create_table(self.target_table_name, self.fs.schema, "CREATE TARGET TABLE")
        aux_table_schema = {
            "file_name": "TEXT",
            "year": "SMALLINT",
            "file_seek": "BIGINT",
            "header": "TEXT",
        }
        DbOperation(self.db).create_table(self.aux_table_name, aux_table_schema, "CREATE AUX TABLE")
        DbOperation(self.db).insert_many_into_table(self.aux_table_name,
                                                    ["file_name", "year", "file_seek", "header"],
                                                    [(file, year, 0, "") for file, year in self.fs.data_files],
                                                    "INSERT INTO AUX TABLE")

    def commit(self):
        self.db.commit()
