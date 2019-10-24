from importlib.machinery import SourceFileLoader
import io
import os.path

from setuptools import setup

parquetry = SourceFileLoader(
    "parquetry", "./parquetry/__init__.py"
).load_module()

with io.open(os.path.join(os.path.dirname(__file__), "README.md"), encoding="utf-8") as f:
    long_description = f.read()

package_data = {"": ["README.md"]}

setup(
    name="parquetry",
    description="Dump parquet files to sql",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=parquetry.__version__,
    license="Apache 2.0",
    author="source{d}",
    author_email="production-machine-learning@sourced.tech",
    url="https://github.com/src-d/parquetry",
    download_url="https://github.com/src-d/parquetry",
    keywords=["dashboard_server"],
    install_requires=[
        "pandas",
        "sqlalchemy",
        "fastparquet",
        "python-snappy",
        "psycopg2-binary",
    ],
    package_data=package_data,
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries",
    ],
)
