import os
import re
import sys
from setuptools import setup, find_packages

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []


def find_version(*paths):
    fname = os.path.join(*paths)
    with open(fname) as fhandler:
        version_file = fhandler.read()
        version_match = re.search(r"^__VERSION__ = ['\"]([^'\"]*)['\"]",
                                  version_file, re.M)

    if not version_match:
        raise RuntimeError("Unable to find version string in %s" % (fname,))

    version = version_match.group(1)
    return version


version = find_version('pymesos', '__init__.py')

setup(
    name='pymesos',
    version=version,
    description="A pure python implementation of Mesos scheduler and executor",
    packages=find_packages(),
    platforms=['POSIX'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
    ],
    author="Zhongbo Tian",
    author_email="tianzhongbo@douban.com",
    url="https://github.com/douban/pymesos",
    download_url=(
        'https://github.com/douban/pymesos/archive/%s.tar.gz' % version
    ),
    install_requires=['six', 'http-parser', 'addict'],
    setup_requires=pytest_runner,
    tests_require=[
        'pytest-cov',
        'pytest-randomly',
        'pytest-mock',
        'pytest'],
)
