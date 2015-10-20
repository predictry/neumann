from luigi.mock import MockTarget
from io import BytesIO
import sys


class LocalFileTargetMock(MockTarget):

    def __init__(self, filename):
        super().__init__(filename)

    def open(self, mode):
        fn = self._fn
        mock_target = self

        class Buffer(BytesIO):
            _write_line = True
            wrapper = None

            def set_wrapper(self, new_wrapper):
                self.wrapper = new_wrapper

            def write(self, data):
                stderrbytes = sys.stderr

                if mock_target._mirror_on_stderr:
                    if self._write_line:
                        sys.stderr.write(fn + ": ")
                    stderrbytes.write(data)
                    if (data[-1]) == '\n':
                        self._write_line = True
                    else:
                        self._write_line = False
                super(Buffer, self).write(data)

            def close(self):
                if mode == 'w':
                    try:
                        mock_target.wrapper.flush()
                    except AttributeError:
                        pass
                    mock_target.fs.get_all_data()[fn] = self.getvalue()
                super(Buffer, self).close()

            def __exit__(self, exc_type, exc_val, exc_tb):
                if not exc_type:
                    self.close()

            def __enter__(self):
                return self

            def readable(self):
                return mode == 'r'

            def writeable(self):
                return mode == 'w'

            def seekable(self):
                return False

        if mode == 'w':
            wrapper = self.format.pipe_writer(Buffer())
            wrapper.set_wrapper(wrapper)
            return wrapper
        else:
            return self.format.pipe_reader(Buffer(self.fs.get_all_data()[fn]))

    def makedirs(self):
        pass
