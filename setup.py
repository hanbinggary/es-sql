# -*- coding: utf-8 -*-

from setuptools import setup


packages = ['es-sql']

requires = [
    'elasticsearch',
    'ply'
]

setup(
    name = "es-sql-python",
    version = "0.1",
    author = "yasinasama",
    author_email = "yasinasama01@gmail.com",
    description = "Elasticsearch SQL parser.",
    license = "MIT",
    keywords = "Elasticsearch SQL",
    install_requires = requires,
    packages=packages
)