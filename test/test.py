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
    # You probably need to create some tables and
    # load some test data, do so here.

    # To create tables, you typically do:
    m.Model.metadata.create_all(engine)

    m.build_fake_data(session)


def teardown():
    m.dump(session)

def test_something():
    strainer = Strainer(m.Product)

    assert(len(session.query(m.Product).all()) > 0)


def test_something2():

    assert(len(session.query(m.Product).all()) > 0)


def test_something3():

    assert(len(session.query(m.Product).all()) > 9999990)
