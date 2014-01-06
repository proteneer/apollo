import unittest
import apollo
import redis
import sys

redis_client = redis.Redis()

class Person(apollo.Entity):
    prefix = 'person'
    fields = {'age': int,
              'ssn': str,
              'emails': {str},
              'tasks': (str)
              }


class Cats(apollo.Entity):
    prefix = 'cat'
    fields = {'age': int}


class TestApollo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = redis_client
        cls.db.flushdb()

    def test_create_person(self):
        joe = Person.create('joe', self.db)
        print('JF',joe.fields)
        joe.hset('age', 25)
        joe.hset('ssn', '123-45-6789')

    @classmethod
    def tearDownClass(cls):
        #assert(cls.db.keys('*') == [])
        #cls.db.shutdown()
        pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)
