class ITask(object):

    # def __init__(self, id, status, parameters):
    #
    #     self.metadata = dict(
    #         id=id,
    #         module=self.__module__,
    #         cls=self.__class__.__name__
    #     )
    #
    #     self.status = status
    #     self.parameters = parameters

    def run(self):
        raise NotImplementedError


class IOperator(object):

    def run(self):
        raise NotImplementedError

    def update(self, progress):
        raise NotImplementedError

    def progress(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class ITaskCard(object):

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

