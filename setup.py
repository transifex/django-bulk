import versioneer

from setuptools import setup

setup(
    name='django-bulk-compat',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author='Kevin Mahoney',
    author_email='kevin.mahoney@maplecroft.com',
    packages=['djangobulk'],
    install_requires=['Django >= 1.5', 'psycopg2'],
    )
