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


class ITask(object):

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

# TODO: implement task card (id, status, params)
# TODO: wk: operator uses luigi, which uses the task card

