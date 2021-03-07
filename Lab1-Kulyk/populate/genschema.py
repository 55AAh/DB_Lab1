import string

from datafiles import DATA_FOLDER, get_datafiles_list, read_file, \
    check_schema, delete_schema, load_schema, save_schema, strip_arr
from user import print_flush, ask_variants, ask_confirm


def main():
    print_flush("Schema generation script started\n")

    schema = Schema(DATA_FOLDER)
    while schema.handle_state():
        print_flush()

    print_flush("\nSchema generation script stopped")


def default_columns():
    return [("OUTID", SqlValueType(sql_type=SqlValueType.SQL_TYPE_UUID)),
            ("YEAR", SqlValueType(sql_type=SqlValueType.SQL_TYPE_SMALLINT))]


class SqlValueType:
    SQL_TYPE_SMALLINT = 1
    SQL_TYPE_UUID = 2
    SQL_TYPE_VARCHAR = 3

    def __init__(self, sql_type=None, val=None):
        if val is None:
            self.sql_type = sql_type
            self.sql_len = 1
        else:
            if SqlValueType.can_be_smallint(val):
                self.sql_type = SqlValueType.SQL_TYPE_SMALLINT
            elif SqlValueType.can_be_uuid(val):
                self.sql_type = SqlValueType.SQL_TYPE_UUID
            else:
                self.sql_type = SqlValueType.SQL_TYPE_VARCHAR
            self.sql_len = len(str(val))

    @staticmethod
    def can_be_smallint(val):
        try:
            num = int(val)
            return -32768 <= num <= 32767
        except ValueError:
            return False

    @staticmethod
    def can_be_uuid(val):
        return len(val) == 36 and val[8] == val[13] == val[18] == val[23] == '-' and \
            all([(c == '-' if i in [8,13,18,23] else c in string.hexdigits) for i, c in enumerate(val)])

    def fit(self, other):
        self.sql_type = max(self.sql_type, other.sql_type)
        self.sql_len = max(self.sql_len, other.sql_len)

    def dump(self):
        if self.sql_type == SqlValueType.SQL_TYPE_VARCHAR:
            return f"VARCHAR({self.sql_len})"
        if self.sql_type == SqlValueType.SQL_TYPE_SMALLINT:
            return "SMALLINT"
        if self.sql_type == SqlValueType.SQL_TYPE_UUID:
            return "UUID"


class Schema:
    def __init__(self, folder):
        self.folder = folder
        self.columns = default_columns()

    def get_state(self):
        if check_schema(self.folder):
            if load_schema(self.folder):
                return "correct"
            else:
                return "corrupted"
        else:
            return "clear"

    def make(self):
        self.columns = default_columns()
        data_files = get_datafiles_list(self.folder)
        for i, (file, year) in enumerate(data_files):
            year_str = f" ({year})" if year is not None else ""
            print_flush(f"Processing ({i+1}/{len(data_files)}) '{file}'{year_str}: reading file... ", end='')
            lines = read_file(file)
            new_columns = []
            for column_name in strip_arr(lines[0].split(';')):
                new_columns.append((column_name, SqlValueType(sql_type=SqlValueType.SQL_TYPE_SMALLINT)))
            for j, line in enumerate(lines[1:]):
                if j % 1000 == 0:
                    print_flush(f"\rProcessing ({i+1}/{len(data_files)}) '{file}'{year_str}: processing rows... "
                                f"({j}/{len(lines) - 1})", end='')
                new_vals = strip_arr(line.split(';'))
                for val, column in zip(new_vals, new_columns):
                    column[1].fit(SqlValueType(None, val))
            print_flush(f"\r\x1b[1K\rProcessing ({i+1}/{len(data_files)}) '{file}'{year_str}: combining... ", end='')
            for new_column in new_columns:
                found = False
                for column in self.columns:
                    if new_column[0].upper() == column[0].upper():
                        column[1].fit(new_column[1])
                        found = True
                        break
                if not found:
                    self.columns.append(new_column)
            print_flush("done!")
        print_flush(f"Saving schema ({len(data_files)} files, {len(self.columns)} columns) "
                    f"to folder '{self.folder}'... ", end='')
        names_texts = []
        types_texts = []
        for column in self.columns:
            names_texts.append(column[0])
            types_texts.append(column[1].dump())
        save_schema(self.folder, names_texts, types_texts)
        print_flush("done!")
        return False

    def handle_state(self):
        state = self.get_state()
        if state == "correct":
            while True:
                sel = ask_variants("Correct schema found.\n", {
                    "r": "reload state",
                    "d": "delete schema",
                    "e": "exit",
                })
                if sel == "r":
                    return True
                elif sel == "d":
                    if ask_confirm():
                        delete_schema(self.folder)
                        return True
                elif sel == "e":
                    return False
                print_flush()
        elif state == "corrupted":
            while True:
                sel = ask_variants("Corrupted schema found.\n", {
                    "r": "reload state",
                    "d": "delete schema",
                    "e": "exit",
                })
                if sel == "r":
                    return True
                elif sel == "d":
                    if ask_confirm():
                        delete_schema(self.folder)
                        return True
                elif sel == "e":
                    return False
                print_flush()
        elif state == "clear":
            while True:
                sel = ask_variants("No schema found.\n", {
                    "r": "reload state",
                    "m": "make schema",
                    "e": "exit",
                })
                if sel == "r":
                    return True
                elif sel == "m":
                    if self.get_state() != state:
                        return True
                    return self.make()
                elif sel == "e":
                    return False
                print_flush()
        return False


if __name__ == "__main__":
    main()
