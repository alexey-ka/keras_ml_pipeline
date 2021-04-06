import sys
import pandas as pd
from operators.preprocess import Preprocessing

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
    def test_dataset_preprocess_shape(self):
        # Check shape of the preprocessing function
        preprocess = Preprocessing(None, None, None)
        preprocess.dataset = pd.DataFrame({'col1': [1, 2, 3], 'col2': [1, None, 1], 'col3': [2, 2, 1]})
        assert preprocess.preprocess(
            {'col_name': 'col3', 'rule': {1: 'USA', 2: 'Europe'}, 'prefix': '', 'prefix_sep': ''}).shape == (2, 4)

    def test_dataset_split_shape(self):
        # Check shape of the train subset
        preprocess = Preprocessing(None, None, {'target': 'col3'})
        preprocess.dataset = pd.DataFrame({'col1': [1, 2, 3, 4, 5], 'col4': [1, 2, 3, 4, 5], 'col3': [2, 2, 1, 1, 4]})
        preprocess.split({'frac': 0.2, 'random_state': 0})
        assert preprocess.train_features.shape == (1, 2)


if __name__ == '__main__':
    unittest.main()
