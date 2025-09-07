import argparse
import os


from lake.connector import connect


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

    parser_connect = subparsers.add_parser(
        "connect",
        help="create simple circuite to ducklake",
    )
    parser_exec = subparsers.add_parser(
        "exec",
        help="execute a custom command on lake",
    )
    parser_exec.add_argument(
        "--cmd",
        "-x",
        type=str,
        required=True,
        help="the command to execute."
    )
    parser_exec.add_argument(
        "--config",
        "-c",
        type=str,
        required=True,
        default='resources/config.yml',
        help="path to config file included SRC/DEST"
    )
    parser_exec.add_argument(
        "--src",
        "-s",
        type=str,
        required=True,
        choices=['kafka','s3','postgres'],
        help="the source you want this runtime to read from..."
    )
    parser_connect.add_argument(
        "--src",
        "-s",
        type=str,
        required=True,
        choices=['kafka','s3','postgres'],
        help="the source you want this runtime to read from..."
    )
    parser_connect.add_argument(
        "--config",
        "-c",
        type=str,
        required=True,
        default='resources/config.yml',
        help="path to config file included SRC/DEST"
    )
    args = parser.parse_args()
    if args.command == 'connect':
        cnn = connect(args.src,args.config)
        if args.src == 'kafka':
            cnn.attach()
    elif args.command == 'exec':
        cnn = connect(args.src,args.config)
        output = cnn.exec(args.cmd).df()
        print(output)
       

if __name__ == "__main__":
    main()
