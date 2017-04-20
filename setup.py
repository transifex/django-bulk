from setuptools import setup

setup(
    name='django-bulk-compat',
    version='0.1',
    author='Kevin Mahoney',
    author_email='kevin.mahoney@maplecroft.com',
    packages=['djangobulk'],
    install_requires=['Django >= 1.5', 'psycopg2'],
    )
