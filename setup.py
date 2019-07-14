import os
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

if os.environ.get('CI_COMMIT_TAG'):
    version = os.environ['CI_COMMIT_TAG']
else:
    version = os.environ['CI_JOB_ID']

setup(
    name="amaxa",
    version=version,
    description="Load and extract data from multiple Salesforce objects in a single "
    "operation, preserving links and network structure.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="David Reed",
    author_email="david@ktema.org",
    url="https://gitlab.com/davidmreed/amaxa",
    license="GNU GPLv3",
    packages=["amaxa", "amaxa.loader"],
    python_requires=">=3.6",
    install_requires=[
        "pyyaml",
        "simple_salesforce",
        "salesforce_bulk",
        "cerberus",
        "requests",
        "pyjwt",
        "cryptography",
    ],
    entry_points={"console_scripts": ["amaxa = amaxa.__main__:main"]},
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Environment :: Console",
    ],
)
