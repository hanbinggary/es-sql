# -*- coding: utf-8 -*-

from setuptools import setup


packages = ['es_sql2']

requires = [
    'elasticsearch>=2.0.0,<3.0.0',
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