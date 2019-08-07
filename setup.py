# -*- coding: utf-8 -*-

from setuptools import setup


packages = ['es_sql1', 'es_sql2', 'es_sql3', 'es_sql4', 'es_sql5']

requires = [
    'ply'
]

setup(
    name="es_sql",
    version="0.1",
    author="yasinasama",
    author_email="yasinasama01@gmail.com",
    description="SQL FOR Elasticsearch",
    license="MIT",
    keywords="Elasticsearch SQL",
    install_requires=requires,
    packages=packages
)