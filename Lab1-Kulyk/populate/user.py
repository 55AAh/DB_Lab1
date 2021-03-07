import os
import sys


def print_flush(*args, **kwargs):
    print(*args, **kwargs, flush=True)


env_vars = dict()


def use_env_files():
    if os.environ.get("DB_HOST") == "db":
        return
    lines = []
    with open("../db-auth.env", "r") as f:
        lines.extend(f.readlines())
    with open("../populate_conf.env", "r") as f:
        lines.extend(f.readlines())
    for line in lines:
        name, value = line.split('=')
        env_vars[name.strip()] = value.strip()


def get_env(var_name):
    value = os.environ.get(var_name)
    if value is None:
        if var_name not in env_vars:
            panic(f"Environment variable '{var_name}' is not defined!", PANIC_ENV_VAR_NOT_DEFINED)
        return env_vars[var_name]
    return value


def print_err(message):
    print_flush(message, file=sys.stderr)


PANIC = [False]


def is_panic():
    return PANIC[0]


PANIC_ENV_VAR_NOT_DEFINED = 1
PANIC_DATA_FOLDER_DOESNT_EXIST = 2
PANIC_DB_LOCKED = 3
PANIC_DB_ERROR_OCCURRED = 4
PANIC_DB_RETRIED_ERROR = 5


def panic(message, exitcode):
    PANIC[0] = True
    print_err(message)
    sys.exit(exitcode)


def command_error(name, e, command, data):
    panic(f"Error occurred during execution of command '{name}':\n"
          f"\n{e}\n"
          f"{type(e)}\n\n"
          f"\tCommand:\n"
          f"{command}\n\n"
          f"\tData:\n"
          f"{data}",
          PANIC_DB_ERROR_OCCURRED)


def ask_variants(prompt, variants):
    variants_str = '\n'.join([f"\t{c}: {e}" for c, e in variants.items()])
    sel = None
    while sel not in variants:
        sel = input(f"\t{prompt}\nSelect action:\n{variants_str}\n> ")
        if sel not in variants:
            print_flush()
    return sel


def ask_yn(prompt):
    sel = None
    while sel not in ["y", "n"]:
        sel = input(f"{prompt} (y/n): ")
    return sel == "y"


def ask_confirm(): return ask_yn("Are you sure?")
