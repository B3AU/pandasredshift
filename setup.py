from setuptools import setup

setup(
    name='redpanda',
    packages=['redpanda'],
    version='3.0.0',
    description='Load data from redshift into a pandas DataFrame and vice versa. Forked from pandas_redshift by agawronski',
    author='Beau Piccart',
    author_email='beau.piccart@gmail.com',
    url = 'https://github.com/b3au/redpanda',
    python_requires='>=3',
    install_requires=['psycopg2-binary',
                      'pandas',
                      'boto3'],
    include_package_data=True
)
