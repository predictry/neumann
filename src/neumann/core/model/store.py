__author__ = 'guilherme'

import os

import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.exc

Base = sqlalchemy.ext.declarative.declarative_base()


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