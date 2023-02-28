from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

def get_version(path):
    with open(path) as f:
        first_line = f.readline()
        if first_line.startswith("__version__"):
            return first_line[13:-2]
        else:
            raise RuntimeError("Unable to read version string.")

setup(
    name='pipemp',
    packages=find_packages(include=['pipemp*']),
    version=get_version("pipemp/__init__.py"),
    description='Lightweight multiprocessing pipeline for python',
    author='Tiago Almeida',
    author_email='tiagomeloalmeida@ua.pt',
    license='Apache License 2.0',
    install_requires=requirements,
)