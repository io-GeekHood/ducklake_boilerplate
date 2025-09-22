import argparse
import os
from lake.pages import load
from lake.render import serve

def main():
    """
    Main function to set up and parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        prog="integrator(lake)",  # The name of your CLI tool
        description="Ducklake(DataLake) + Bi Dashboards(Panel)",
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
    parser_serve = subparsers.add_parser(
        "serve",
        help="execute a custom command on lake",
    )
    parser_serve.add_argument(
        "--config",
        "-c",
        type=str,
        required=True,
        default='resources/config.yml',
        help="path to config file included SRC/DEST"
    )
    parser_exec = subparsers.add_parser(
        "exec",
        help="execute a custom command on lake",
    )
    parser_exec.add_argument(
        "--config",
        "-c",
        type=str,
        required=True,
        default='resources/config.yml',
        help="path to config file included SRC/DEST"
    )
    parser_attach = subparsers.add_parser(
        "attach",
        help="attack ducklake to message broker",
    )
    parser_attach.add_argument(
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
        help="the source you want this runtime to read from..."
    )
    args = parser.parse_args()
    if args.command == 'exec':
        cnn = load(args.src,args.config)
    if args.command == 'attach':
        cnn = load("kafka",args.config)
        cnn.attach()
    if args.command == 'serve':
        serve()



if __name__ == "__main__":
    main()
