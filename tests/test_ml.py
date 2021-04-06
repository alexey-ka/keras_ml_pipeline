import sys
import pandas as pd
from operators.MLmodel import MLModel

# Attempt to use back-ported unittest2 for Python 2.6 and earlier
# However, it is strongly recommended to use Python 2.7 or 3.<latest>
try:
    if sys.version_info < (2, 7):
        import unittest2
    else:
        raise ImportError()
except ImportError:
    import unittest


class TestPreprocessing(unittest.TestCase):
    """
    To be implemented
    """


if __name__ == '__main__':
    unittest.main()
