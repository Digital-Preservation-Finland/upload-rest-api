"""Install upload-rest-api package"""
from setuptools import setup, find_packages

from version import get_version


def main():
    """Install upload-rest-api"""
    setup(
        name='upload-rest-api',
        packages=find_packages(exclude=['tests', 'tests.*']),
        version=get_version(),
        install_requires=[
            "flask",
            "pymongo",
            "file-magic",
            "metax_access",
        ],
        dependency_links=[
            'git+https://gitlab.csc.fi/dpres/metax-access.git'
            '@develop#egg=metax-access-0.0'
        ],
        entry_points={
            "console_scripts": [
                "filestorage-cleanup = upload_rest_api.cleanup:main"
            ]
        }
    )

if __name__ == '__main__':
    main()
