__author__ = 'guilherme'


class InvalidLabelError(Exception):
    """Invalid Neo4j label.
    """
    pass


class InvalidRelationshipTypeError(Exception):
    """Invalid Neo4j Relationship Type
    """
    pass


class UnknownRecommendationOption(Exception):
    pass


class UndefinedConfiguration(Exception):
    pass