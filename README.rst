PyMesos
========

.. image:: https://badges.gitter.im/douban/pymesos.svg
   :alt: Join the chat at https://gitter.im/douban/pymesos
   :target: https://gitter.im/douban/pymesos?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

.. image:: https://img.shields.io/travis/douban/pymesos.svg
   :target: https://travis-ci.org/douban/pymesos


A pure python implementation of Apache Mesos scheduler and executor.

Note:
------

Since ``PyMesos 0.2.0``, ``Apache Mesos`` HTTP API is implemented instead of the ``Protobuf`` API.

Users of ``PyMesos`` would be able to get rid of the unnecessary dependence of ``Protobuf``.

Meanwhile, ``PyMesos`` user need to migrate original code to use plain ``Python`` dicts instead of ``Protobuf`` objects to use ``PyMesos >= 0.2.0``.

For more detail, please refer to `Scheduler HTTP API Document <http://mesos.apache.org/documentation/latest/scheduler-http-api/>`_ and `Executor HTTP API Document <http://mesos.apache.org/documentation/latest/executor-http-api/>`_
