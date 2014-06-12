===========
SQLStrainer
===========

SQLStrainer filters SQLAlchemy Query objects based any column or hybrid property of a related model.

    from sqlstrainer import strainer
    strainer = sqlstrainer(mydb)
    query.filter(strainer.strain(request.args))

Query
=====

Need to know the query

Relations
=========

Automatically build a relationship map


Contributors:
=============

Douglas MacDougall <douglas.macdougall@moesol.com>

see @AUTHORS
