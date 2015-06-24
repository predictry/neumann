class Session(object):

    def __init__(self, id, tenant, timestamp, fields):

        self.id = id
        self.tenant = tenant
        self.timestamp = timestamp
        self.fields = fields

    def __key(self):

        return self.id, self.tenant

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):

        if not isinstance(other, self.__class__):
            return False

        return self.id == other.id and self.tenant == other.tenant

    def __ne__(self, other):

        return not self.__eq__(other)

    def __str__(self):

        return 'Session(id={0}, tenant={1}, timestamp={2})'.format(
            self.id, self.tenant, self.timestamp
        )

    @property
    def properties(self):

        return self.__dict__


class User(object):

    def __init__(self, id, tenant, timestamp, fields):

        self.id = id
        self.tenant = tenant
        self.timestamp = timestamp
        self.fields = fields

    def __key(self):

        return self.id, self.tenant

    def __hash__(self):

        return hash(self.__key())

    def __eq__(self, other):

        if not isinstance(other, self.__class__):
            return False

        return self.id == other.id and self.tenant == other.tenant

    def __ne__(self, other):

        return not self.__eq__(other)

    def __str__(self):

        return 'User(id={0}, tenant={1}, timestamp={2})'.format(
            self.id, self.tenant, self.timestamp
        )

    @property
    def properties(self):

        return self.__dict__


class Agent(object):

    def __init__(self, id, tenant, timestamp, fields):

        self.id = id
        self.tenant = tenant
        self.timestamp = timestamp
        self.fields = fields

    def __key(self):

        return self.id, self.tenant

    def __hash__(self):

        return hash(self.__key())

    def __eq__(self, other):

        if not isinstance(other, self.__class__):
            return False

        return self.id == other.id and self.tenant == other.tenant

    def __ne__(self, other):

        return not self.__eq__(other)

    def __str__(self):

        return 'Agent(id={0}, tenant={1}, timestamp={2})'.format(
            self.id, self.tenant, self.timestamp
        )

    @property
    def properties(self):

        return self.__dict__


class Item(object):

    def __init__(self, id, tenant, timestamp, fields):

        self.id = id
        self.tenant = tenant
        self.timestamp = timestamp
        self.fields = fields

    def __key(self):

        return self.id, self.tenant

    def __hash__(self):

        return hash(self.__key())

    def __eq__(self, other):

        if not isinstance(other, self.__class__):
            return False

        return self.id == other.id and self.tenant == other.tenant

    def __ne__(self, other):

        return not self.__eq__(other)

    def __str__(self):

        return 'Item(id={0}, tenant={1}, timestamp={2})'.format(
            self.id, self.tenant, self.timestamp
        )

    @property
    def properties(self):

        return self.__dict__


class Action(object):

    def __init__(self, name, tenant, user, agent, session, item, timestamp, fields):

        self.name = name
        self.tenant = tenant
        self.user = user
        self.agent = agent
        self.session = session
        self.item = item
        self.timestamp = timestamp
        self.fields = fields

    def __key(self):

        return self.name, self.tenant, self.session, self.item, self.timestamp

    def __hash__(self):

        return hash(self.__key())

    def __eq__(self, other):

        if not isinstance(other, self.__class__):
            return False

        return self.name == other.name and self.tenant == other.tenant and self.session == other.session and \
               self.item == other.item and self.timestamp == other.timestamp

    def __ne__(self, other):

        return not self.__eq__(other)

    def __str__(self):

        return 'Action(name={0}, tenant={1}, user={2}, agent={3}, session={4}, item={5}, timestamp={6})'.format(
            self.name, self.tenant, self.user, self.agent, self.session, self.item, self.timestamp
        )

    @property
    def properties(self):

        return self.__dict__
