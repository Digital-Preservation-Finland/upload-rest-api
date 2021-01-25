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
            "redis",
            "rq",
            "futures; python_version == '2.7'",
            "metax_access@git+https://gitlab.csc.fi/dpres/"
            "metax-access.git@develop",
            "archive_helpers@git+https://gitlab.csc.fi/dpres/"
            "archive-helpers.git@develop"
        ],
        entry_points={
            "console_scripts": [
                "upload-rest-api = upload_rest_api.__main__:main",
            ]
        }
    )


if __name__ == '__main__':
    main()
