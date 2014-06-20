"""SQLStrainer filters SQLAlchemy Query objects based any column or hybrid property of a related model.

..  code::

    from sqlstrainer import strainer
    strainer = sqlstrainer(mydb)
    query.filter(strainer.strain(request.args))


mapper
------

.. automodule:: sqlstrainer.mapper

strainer
--------

.. automodule:: sqlstrainer.strainer

handler
-------

.. automodule:: sqlstrainer.handler

match
-------

.. automodule:: sqlstrainer.match

"""

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

