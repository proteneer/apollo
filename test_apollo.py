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
apollo.relate({Person}, 'cats_to_feed', {Cat}, 'caretakers')

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

    def test_one_to_n_relations(self):
        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        joe.sadd('cats', sphinx)
        self.assertEqual(sphinx.hget('owner'), 'joe')
        polly = Cat.create('polly', self.db)
        polly.hset('owner', joe)
        self.assertEqual(polly.hget('owner'), 'joe')
        self.assertSetEqual(joe.smembers('cats'), {'sphinx', 'polly'})
        # change of ownership
        bob = Person.create('bob', self.db)
        sphinx.hset('owner', bob)
        self.assertEqual(sphinx.hget('owner'), 'bob')
        self.assertEqual(joe.smembers('cats'), {'polly'})
        self.assertEqual(bob.smembers('cats'), {'sphinx'})
        bob.sadd('cats', polly)
        self.assertEqual(bob.smembers('cats'), {'sphinx', 'polly'})
        self.assertEqual(joe.smembers('cats'), set())

    def test_n_to_n_relations(self):
        pass

    @classmethod
    def tearDownClass(cls):
        #assert(cls.db.keys('*') == [])
        #cls.db.shutdown()
        pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)
