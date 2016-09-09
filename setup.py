import os
import re
from setuptools import setup, find_packages

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

def find_long_description(*paths):
    fname = os.path.join(*paths)
    with open(fname) as fhandler:
        return fhandler.read()

version = find_version('pymesos', '__init__.py')

setup(
    name='pymesos',
    version=version,
    description="A pure python implementation of Mesos scheduler and executor",
    long_description=find_long_description('README'),
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
    download_url = 'https://github.com/douban/pymesos/archive/%s.tar.gz' % version,
    install_requires=['six', 'http-parser'],
)
