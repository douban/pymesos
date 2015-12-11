from setuptools import setup, find_packages

version = '0.0.9'

setup(
    name='pymesos',
    version=version,
    description="A pure python implementation of Mesos scheduler and executor",
    packages=find_packages(),
    install_requires=['mesos.interface>=0.22.0,<0.22.1.2'],
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
)
