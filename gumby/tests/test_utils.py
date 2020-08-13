import unittest

from gumby.util import Dist


class TestStatisticsParser(unittest.TestCase):

    def test_uniform_dist(self):
        d = Dist('uniform', '(1, 1)')
        val = d.get()
        self.assertLessEqual(val, 2)
        self.assertGreaterEqual(val, 1)

    def test_parse_from_str_const(self):
        d = Dist.from_raw_str('1')
        self.assertEqual(d.get(), 1)

    def test_parse_from_str_norm(self):
        d = Dist.from_raw_str('uniform,(1,1)')
        self.assertLessEqual(d.get(), 2)
        self.assertGreaterEqual(d.get(), 1)

    def test_parse_from_str_sample(self):
        d = Dist.from_raw_str('sample,[1,2,3]')
        self.assertLessEqual(d.get(), 3)
        self.assertGreaterEqual(d.get(), 1)

    def test_seed(self):
        d = Dist.from_raw_str('uniform,(1,1)')
        val1 = d.get(seed=1)
        val2 = d.get(seed=1)

        self.assertEqual(val2, val1)

    def test_generate(self):
        d = Dist.from_raw_str('planck,(0.34,1)')
        val1 = d.generate(n=100, seed=10)
        print(val1)






