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
    m.dump(session)


def test_something(strainer):
    strainer.strain([
        {'name': 'first_name', 'action': 'notcontains', 'value': ['b', 'c', 'd']},
       # {'name': 'parent.first_name', 'action': 'contains', 'value': ['a']}
    ])
    q = session.query(m.Customer)
    print list(strainer._dbmap.viewable(m.Customer.__mapper__))
    assert(q.count() > 0)



# def test_something2():
#
#     assert(len(session.query(m.Product).all()) > 0)
#
#
# def test_something3(strainer):
#     assert(len(strainer.columns) > 0)
