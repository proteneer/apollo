import unittest
import apollo
import redis
import sys

redis_client = redis.Redis(decode_responses=True)


class Person(apollo.Entity):
    prefix = 'person'
    fields = {'age': int,
              'ssn': str,
              'emails': {str},
              'tasks': (str)
              }


class Cat(apollo.Entity):
    prefix = 'cat'
    fields = {'age': int}

apollo.relate(Person, 'cats', {Cat}, 'owner')

#print(Person.fields)
#print(Person.relations)
#print(Cat.fields)
#print(Cat.relations)

class TestApollo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = redis_client
        cls.db.flushdb()

    def tearDown(self):
        self.db.flushdb()

    def test_hset_hget(self):
        joe = Person.create('joe', self.db)
        age = 25
        ssn = '123-45-6789'
        joe.hset('age', 25)
        self.assertEqual(joe.hget('age'), age)
        joe.hset('ssn', '123-45-6789')
        self.assertEqual(joe.hget('ssn'), ssn)

    def test_basic_relation(self):
        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        joe.sadd('cats', sphinx)
        self.assertEqual(sphinx.hget('owner'),'joe')

    @classmethod
    def tearDownClass(cls):
        #assert(cls.db.keys('*') == [])
        #cls.db.shutdown()
        pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)
