from sqlalchemy.orm import create_session

__author__ = 'Douglas MacDougall <douglas.macdougall@moesol.com>'

from sqlstrainer.strainer import Strainer
from sqlalchemy import create_engine
session = None
strainer = None
import models as m

def setup():
    global session
    engine = create_engine('sqlite:///:memory:')
    session = create_session(bind=engine)
    # build test schema
    m.Model.metadata.create_all(engine)
    # populate test data
    m.build_fake_data(session)


def teardown():
    m.dump(session)

def test_something():
    strainer = Strainer(m.Product)
    assert(len(strainer.columns) > 0)
    strainer2 = Strainer(m.Product, all_relatives=True)
    assert(len(strainer2.columns) > len(strainer.columns))


def test_something2():

    assert(len(session.query(m.Product).all()) > 0)


def test_something3():
    c = session.query(m.Product).count()
    assert(c > 9999990)
