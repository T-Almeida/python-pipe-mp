from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

import pipemp

setup(
    name='pipemp',
    packages=find_packages(include=['pipemp*']),
    version=pipemp.__version__,
    description='Lightweight multiprocessing pipeline for python',
    author='Tiago Almeida',
    author_email='tiagomeloalmeida@ua.pt',
    license='Apache License 2.0',
    install_requires=requirements,
)