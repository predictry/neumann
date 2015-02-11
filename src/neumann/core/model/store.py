__author__ = 'guilherme'

import os

import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.exc

Base = sqlalchemy.ext.declarative.declarative_base()


REL_SESSION_TO_AGENT = "FROM"
REL_SESSION_TO_USER = "BY"
REL_ITEM_LOCATION = "AVAILABLE_AT"

LABEL_SESSION = "Session"
LABEL_AGENT = "Agent"
LABEL_USER = "User"
LABEL_ITEM = "Item"
LABEL_SEARCH = "Search"
LABEL_LOCATION = "Location"
LABEL_LOCATION_COUNTRY = "Country"
LABEL_LOCATION_CITY = "City"

REL_ACTION_TYPE_SEARCH = "SEARCH"
REL_ACTION_TYPE_VIEW = "VIEW"
REL_ACTION_TYPE_ADD_TO_CART = "ADD_TO_CART"
REL_ACTION_TYPE_BUY = "BUY"
REL_ACTION_TYPE_STARTED_CHECKOUT = "STARTED_CHECKOUT"
REL_ACTION_TYPE_STARTED_PAYMENT = "STARTED_PAYMENT"


class Site(Base):
    """

    """

    __tablename__ = "sites"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String(32), nullable=False)

    def __repr__(self):

        return "{0}[id={1},name={2}]".format(self.__class__.__name__,
                                                         self.id, self.name)

    def __hash__(self):

        return hash(self.__repr__())


class Widget(Base):
    """

    """

    __tablename__ = "widgets"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    site_id = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    reco_type = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)

    def __repr__(self):

        return "{0}[id={1},site_id={2},reco_type={3}]".format(self.__class__.__name__,
                                                              self.id, self.site_id, self.reco_type)

    def __hash__(self):

        return hash(self.__repr__())