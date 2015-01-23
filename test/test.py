from sqlalchemy.orm import create_session
import pytest

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

from sqlstrainer.strainer import Strainer
from sqlalchemy import create_engine
session = None
import models as m

def setup():
    global session
    engine = create_engine('sqlite:///:memory:')
    session = create_session(bind=engine)
    # build test schema
    m.Model.metadata.create_all(engine)
    # populate test data
    m.build_fake_data(session)


@pytest.fixture
def strainer():
    strainer = Strainer(m.Customer, strict=True)
  #  strainer.group.nest(m.Customer.parent)
    return strainer



def teardown():
    pass
#    m.dump(session)


def test_something(strainer):
    from sqlstrainer.mapper import StrainerMap
    sm = StrainerMap()
    rs = sm.relations_of(sm.to_mapper(m.Customer))
    mm = sm.to_mapper(rs.keys()[0])
    print rs
    args = [ { 'name': 'first_name', 'values': ['b', 'c', 'd'] }
             ]
    strainer.load(args)
    q = session.query(m.Customer)
    c1 = q.count()
    q = strainer.apply(q)
    c2 = strainer.apply(q).count()
    print c1, c2
    assert(c1 > c2)

    args = [ { 'name': 'first_name', 'values': ['b', 'c', 'd'] },
             { 'name': 'customer_id', 'values': ['46'], 'action': 'gt' },
             { 'name': 'parent.first_name', 'values': ['b', 'c', 'd'] }
             ]
#    strainer.relate('parent', 'parent')
    strainer.load(args)
    c3 = strainer.apply(q).count()
    print c1, c2, c3
    assert(c2 > c3)


# def test_something2():
#
#     assert(len(session.query(m.Product).all()) > 0)
#
#
# def test_something3(strainer):
#     assert(len(strainer.columns) > 0)
