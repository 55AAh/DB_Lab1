import os

DATA_FOLDER = "data"
SCHEMA_FILE = "SCHEMA.csv"
ENCODINGS = ["utf-8-sig", "cp1251", "utf-8"]


def read_file(path):
    b_enc, path_enc = os.path.splitext(os.path.splitext(path)[0])
    path_enc = path_enc[1:]
    guess_encodings = ENCODINGS
    if path_enc in ENCODINGS:
        guess_encodings = [path_enc] + [enc for enc in ENCODINGS if enc != path_enc]
    for encoding in guess_encodings:
        try:
            with open(path, "r", encoding=encoding) as f:
                lines = f.readlines()
            if encoding != path_enc:
                os.rename(path, f"{b_enc}.{encoding}.csv")
            return lines
        except UnicodeError as e:
            pass
    raise UnicodeError(f"Cannot decode file, tried encodings: {', '.join(ENCODINGS)}")


def get_file_encoding(path):
    return os.path.splitext(os.path.splitext(path)[0])[1][1:]


def get_file_size(path):
    return os.path.getsize(path)


def format_file_size(b):
    if b < 1024:
        return f"{b} B"
    b /= 1024
    if b < 1024:
        return f"{b:.1f} KB"
    b /= 1024
    if b < 1024:
        return f"{b:.1f} MB"
    b /= 1024
    return f"{b:.1f} GB"


def parse_year(filename):
    fn = ""
    for c in filename:
        if c.isdigit():
            fn += c
        else:
            fn += " "
    numbers = [n for n in fn.split(' ') if len(n) > 0]
    if len(numbers) != 1 or len(numbers[0]) != 4:
        return None
    return int(numbers[0])


def get_datafiles_list(path):
    data_files, data_files_years = [], dict()
    for file in os.listdir(path):
        if file.endswith(".csv") and file.upper() != SCHEMA_FILE.upper():
            year = parse_year(os.path.splitext(os.path.splitext(file)[0])[0])
            if year is None:
                data_files.append(os.path.join(path, file))
            else:
                data_files_years[os.path.join(path, file)] = year
    return [kv for kv in sorted(data_files_years.items(), key=lambda kv: kv[1])] + \
           [(file, None) for file in data_files]


def save_schema(path, names, types):
    with open(os.path.join(path, SCHEMA_FILE), "w", encoding="utf-8") as f:
        f.writelines([";".join(names), '\n', ";".join(types)])


def check_schema(path):
    return os.path.exists(os.path.join(path, SCHEMA_FILE))


def delete_schema(path):
    try:
        os.remove(os.path.join(path, SCHEMA_FILE))
    except FileNotFoundError:
        pass


def load_schema(path):
    schema_file = os.path.join(path, SCHEMA_FILE)
    if not os.path.exists(schema_file):
        return None
    with open(schema_file) as f:
        schema_lines = f.readlines()
    if len(schema_lines) != 2:
        return None
    names = list(strip_arr(schema_lines[0].split(';')))
    types = list(strip_arr(schema_lines[1].split(';')))
    if len(names) != len(types):
        return None
    return dict(zip(names, types))


def strip(text):
    return text.strip("'\n\" ")


def strip_arr(arr):
    return map(strip, arr)
