"""Commandline interface for upload_rest_api package"""
from __future__ import print_function

import argparse

from upload_rest_api.cleanup import clean_disk, clean_mongo

# ANSI escape sequences for different colors
SUCCESSC = '\033[92m'
FAILC = '\033[91m'
ENDC = '\033[0m'


def _parse_args():
    """Parse command line arguments.

    :returns: Parsed arguments
    """
    # Parse commandline arguments
    parser = argparse.ArgumentParser(description="upload_rest_api CLI")

    # Add the alternative commands
    subparsers = parser.add_subparsers(title='Available commands')
    _setup_cleanup_args(subparsers)

    # Define arguments common to all commands
    parser.add_argument(
        '--config',
        default='/etc/upload_rest_api.conf',
        metavar='config_file',
        help="path to configuration file"
    )

    # Parse arguments and return the arguments
    return parser.parse_args()


def _setup_cleanup_args(subparsers):
    """Define cleanup subparser and its arguments."""
    parser = subparsers.add_parser(
        'cleanup', help='Clean files from disk or identfiers from mongo.'
    )
    parser.set_defaults(func=_cleanup)
    parser.add_argument('location', help="mongo or disk")


def _cleanup(args):
    """Generate technical metadata for the dataset"""
    if args.location == "disk":
        deleted_count = clean_disk()
    elif args.location == "mongo":
        deleted_count = clean_mongo()
    else:
        raise ValueError("Unsupported location: %s" % args.location)

    print("Cleaned %d files" % deleted_count)


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    # Parse arguments and call function defined by chosen subparser.
    args = _parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
