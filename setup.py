"""Install upload-rest-api package"""
from setuptools import setup, find_packages


def main():
    """Install upload-rest-api"""
    setup(
        name='upload-rest-api',
        packages=find_packages(exclude=['tests', 'tests.*']),
        include_package_data=True,
        setup_requires=['setuptools_scm'],
        use_scm_version=True,
        install_requires=[
            "flask",
            "pymongo",
            "file-magic",
            "redis",
            "rq",
            "python-dateutil",
            "rehash",
            "click",
            "mongoengine",
        ],
        entry_points={
            "console_scripts": [
                "upload-rest-api = upload_rest_api.__main__:cli",
            ]
        }
    )


if __name__ == '__main__':
    main()
