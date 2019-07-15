"""Install upload-rest-api package"""
from setuptools import setup, find_packages

from version import get_version


def main():
    """Install upload-rest-api"""
    setup(
        name='upload-rest-api',
        packages=find_packages(exclude=['tests', 'tests.*']),
        include_package_data=True,
        version=get_version(),
        install_requires=[
            "flask",
            "pymongo",
            "file-magic",
            "six",
            "metax_access@git+https://gitlab.csc.fi/dpres/"
            "metax-access.git@develop",
            "archive_helpers@git+https://gitlab.csc.fi/dpres/"
            "archive-helpers.git@develop"
        ],
        entry_points={
            "console_scripts": [
                "filestorage-cleanup = upload_rest_api.cleanup:main",
                "filestorage-init = upload_rest_api.database:init_db"
            ]
        }
    )


if __name__ == '__main__':
    main()
