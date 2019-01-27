from setuptools import setup

setup(
    name='amaxa',
    version='0.9.0',
    description='Load and extract data from multiple Salesforce objects in a single operation, preserving links and network structure.',
    author='David Reed',
    author_email='david@ktema.org',
    license='GNU GPLv3',
    packages=['amaxa'],
    python_requires='>=3.6',
    requires=['pyyaml', 'simple_salesforce', 'salesforce_bulk', 'cerberus'],
    tests_require=['pytest', 'pytest-cov', 'codecov', 'wheel'],
    entry_points={
        'console_scripts': [
            'amaxa = amaxa.__main__:main'
        ]
    },
)