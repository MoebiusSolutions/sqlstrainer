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
    return Strainer(m.Customer, strict=True, all_relatives=True)


def teardown():
    pass
#    m.dump(session)


def test_something(strainer):
    args = [ { 'name': 'first_name', 'values': ['b', 'c', 'd'] } ]
    strainer.strain(args)
    q = session.query(m.Customer)
    c1 = q.count()
    q = strainer.apply(q)
    c2 = q.count()
    print c1, c2
    assert(c1 > c2)


# def test_something2():
#
#     assert(len(session.query(m.Product).all()) > 0)
#
#
# def test_something3(strainer):
#     assert(len(strainer.columns) > 0)
