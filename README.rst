PyMesos
========

.. image:: https://img.shields.io/travis/douban/pymesos.svg
   :target: https://travis-ci.org/douban/pymesos

.. image:: https://img.shields.io/pypi/dm/pymesos.svg
   :target: https://pypi.python.org/pypi/pymesos

A pure python implementation of Apache Mesos scheduler and executor.

Note:
------

Since ``PyMesos 0.2.0``, ``Apache Mesos`` HTTP API is implemented instead of the ``Protobuf`` API.

Users of ``PyMesos`` would be able to get rid of the unnecessary dependence of ``Protobuf``.

Meanwhile, ``PyMesos`` user need to migrate original code to use plain ``Python`` dicts instead of ``Protobuf`` objects to use ``PyMesos >= 0.2.0``.

For more detail, please refer to `Scheduler HTTP API Document <http://mesos.apache.org/documentation/latest/scheduler-http-api/>`_ and `Executor HTTP API Document <http://mesos.apache.org/documentation/latest/executor-http-api/>`_
