from distutils.core import setup

setup(
    name='django-bulk',
    version='0.1.2',
    author='Kevin Mahoney',
    author_email='kevin.mahoney@maplecroft.com',
    packages=['djangobulk'],
    install_requires=['Django >= 1.5', 'psycopg2'],
    )
