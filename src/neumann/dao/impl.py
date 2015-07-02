from neumann.ops import ITaskCard
from neumann.ops import IOperator


class XMLTaskCard(ITaskCard):

    @classmethod
    def save(cls, task):
        raise NotImplementedError

    @classmethod
    def read(cls, id):
        raise NotImplementedError

    @classmethod
    def delete(cls, task):
        raise NotImplementedError

    @classmethod
    def exists(cls, id):
        raise NotImplementedError


