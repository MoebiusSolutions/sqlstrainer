"""
SQLStrainer


"""
from distutils.core import setup

setup(
    name='SQLStrainer',
    version='0.0.1',
    author='Douglas MacDougall',
    author_email='douglas.macdougall@moesol.com',
    packages=['sqlstrainer'],
    scripts=[],
    url='http://pypi.python.org/pypi/sqlstrainer/',
    license='LICENSE',
    description='Easily filter SQLAlchemy queries by any related property.',
    long_description=open('README.rst').read(),
    install_requires=[
        "SQLAlchemy >= 0.8.0"
    ],
)