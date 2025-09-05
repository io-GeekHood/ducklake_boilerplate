import argparse
import os

version = os.getenv("CLI_VERSION","v1")
if version == "v1":
    from lake.cli.v1 import test
elif version == "v2":
    # from lake.cli.v2 import test
    exit()
else:
    print(f"Error importing cli version:{version} ")

def main():
    """
    Main function to set up and parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        prog="validator",  # The name of your CLI tool
        description="Agent - Agent cli runner",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0", # Fetches version from prog
        help="Show program's version number and exit.",
    )

    subparsers = parser.add_subparsers(
        title="Commands",
        dest="command",
        help="Available sub-commands",
        required=True, # Ensures a subcommand is always provided
    )
    parse_migrate = subparsers.add_parser(
        "migrate",
        help="run test func!.",
    )
    parse_migrate.add_argument(
        "--database",
        "-d",
        type=str,
        required=True,
        choices=['fms_db','basket_db','oms_db'],
        help="database Name to be initialized"
    )
    parser_run = subparsers.add_parser(
        "run",
        help="initiate all tables in empty database",
    )

    # parser_run.add_argument(
    #     "--database",
    #     "-d",
    #     type=str,
    #     required=True,
    #     choices=['fms_db','basket_db','oms_db'],
    #     help="database Name to be initialized"
    # )
    args = parser.parse_args()
    if args.command == 'run':
        test()
    elif args.command == "init_db":
        pass
       

if __name__ == "__main__":
    main()
