__author__ = 'guilherme'


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


class Item(object):

    def __init__(self, tenant, id):

        self.tenant = tenant
        self.id = id

    def __repr__(self):

        return "Item[tenant={0},id={1}]".format(self.tenant, self.id)

    def __hash__(self):

        return hash(self.__repr__())


class User(object):

    def __init__(self, tenant, id):

        self.tenant = tenant
        self.id = id

    def __repr__(self):

        return "User[tenant={0},id={1}]".format(self.tenant, self.id)

    def __hash__(self):

        return hash(self.__repr__())


class Agent(object):

    def __init__(self, tenant, id):

        self.tenant = tenant
        self.id = id

    def __repr__(self):

        return "Agent[tenant={0},id={1}]".format(self.tenant, self.id)

    def __hash__(self):

        return hash(self.__repr__())


class Session(object):

    def __init__(self, tenant, id):

        self.tenant = tenant
        self.id = id

    def __repr__(self):

        return "Session[tenant={0},id={1}]".format(self.tenant, self.id)

    def __hash__(self):

        return hash(self.__repr__())