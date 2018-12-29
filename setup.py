from setuptools import setup

setup(
    name='amaxa',
    version='0.1.0',
    description='Load and extract data from multiple Salesforce objects in a single operation, preserving links and network structure.',
    author='David Reed',
    author_email='david@ktema.org',
    license='MIT License',
    packages=['amaxa'],
    requires=['pyyaml', 'simple_salesforce', 'cerberus'],
    tests_require=['pytest', 'pytest-cov', 'codecov', 'wheel'],
    entry_points={
        'console_scripts': [
            'amaxa = amaxa.__main__:main'
        ]
    },
)