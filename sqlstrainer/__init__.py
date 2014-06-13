"""SQLStrainer
===========

SQLStrainer filters SQLAlchemy Query objects based any column or hybrid property of a related model.

    from sqlstrainer import strainer
    strainer = sqlstrainer(mydb)
    query.filter(strainer.strain(request.args))


mapper
-----

.. automodule:: sqlstrainer.mapper

"""

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

