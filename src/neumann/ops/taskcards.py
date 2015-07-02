# import os
# import os.path
# import json
#
# from neumann.ops.interfaces import ITaskCard, ITask
# from neumann.utils import config
#
#
# class JsonTaskCard(ITaskCard):
#
#     @classmethod
#     def save(cls, task):
#
#         assert isinstance(task, ITask)
#
#         directory = config.get('taskcard', 'path')
#         directory = os.path.join(config.PROJECT_BASE, directory)
#
#         if os.path.exists(directory) is False:
#             os.makedirs(directory)
#
#         filename = '.'.join([str(task.metadata['id']), 'json'])
#         filepath = os.path.join(directory, filename)
#
#         with open(filepath, 'w') as fp:
#             json.dump(task.__dict__, fp)
#
#         return filepath
#
#     @classmethod
#     def read(cls, id):
#
#         directory = config.get('taskcard', 'path')
#         directory = os.path.join(config.PROJECT_BASE, directory)
#         filename = '.'.join([id, 'json'])
#         path = os.path.join(directory, filename)
#
#         # TODO: missing file
#
#         with open(path, 'r') as fp:
#
#             obj = json.load(fp)
#             print(obj)
#             module = __import__(obj['metadata']['module'])
#             print(module)
#             _class = getattr(module, obj['metadata']['cls'])
#             # task = _class(status=obj['status'], **obj['metadata'], **obj['parameters'])
#
#             return task
#
#     @classmethod
#     def delete(cls, task):
#         raise NotImplementedError
#
#     @classmethod
#     def exists(cls, id):
#         raise NotImplementedError