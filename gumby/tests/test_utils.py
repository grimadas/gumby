import unittest

from gumby.util import Dist


class TestStatisticsParser(unittest.TestCase):

    def test_uniform_dist(self):
        self.assertFalse(False)
        d = Dist('uniform', '(1, 1)')
        val = d.get()
        self.assertLessEqual(val, 2)
        self.assertGreaterEqual(val, 1)
