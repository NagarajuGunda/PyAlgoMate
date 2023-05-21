#!/usr/bin/env python

# PyAlgoMate
#
# Copyright 2023 Nagaraju Gunda
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='PyAlgoMate',
    version='1',
    description='Python Algorithmic Trading based on pyalgotrade',
    long_description='Python library for backtesting stock trading strategies.',
    author='Nagaraju Gunda',
    author_email='gunda.nagaraju92@gmail.com',
    url='https://github.com/NagarajuGunda/PyAlgoMate/',
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    packages=[
        'pyalgomate',
        'pyalgomate.backtesting',
        'pyalgomate.brokers',
        'pyalgomate.strategies',
        'pyalgomate.strategy',
        'pyalgomate.utils',
    ],
    install_requires=[
        "matplotlib",
        "numpy",
        "python-dateutil",
        "pytz",
        "requests",
        "retrying",
        "scipy",
        "six",
        "tornado",
        "tweepy",
        "ws4py>=0.3.4",
        "pyalgotrade @ git+https://git@github.com/NagarajuGunda/pyalgotrade@master",
        "websocket",
        "pyyaml",
        "https://raw.githubusercontent.com/Shoonya-Dev/ShoonyaApi-py/master/dist/NorenRestApiPy-0.0.22-py2.py3-none-any.whl",
        "pyotp",
        "websocket_client",
        "six",
        "fastparquet",
        "pendulum",
        "streamlit",
        "pyzmq",
        "aiohttp",
        "plotly",
        "py_vollib",
        "streamlit-aggrid",
        "streamlit_plotly_events",
        "sqlalchemy",
        "kiteconnect",
        "py_vollib_vectorized",
        "talipp",
        "click"
    ],
)
