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
apollo.relate({Person}, 'friends', {Person}, 'friends')
apollo.relate(Person, 'single_cat', Cat, 'single_owner')

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

    def test_one_to_one_relations(self):
        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        joe.hset('single_cat', sphinx)
        joe.hget('single_cat')

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

    def test_remove_item(self):
        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        sphinx.hset('owner', joe)
        # test deletion
        joe.srem('cats', sphinx)
        self.assertEqual(joe.smembers('cats'), set())
        self.assertEqual(sphinx.hget('owner'), None)

    def test_n_to_n_relations(self):
        joe = Person.create('joe', self.db)
        bob = Person.create('bob', self.db)
        sphinx = Cat.create('sphinx', self.db)
        polly = Cat.create('polly', self.db)

        sphinx.sadd('caretakers', joe)
        polly.sadd('caretakers', bob)
        self.assertSetEqual(sphinx.smembers('caretakers'), {'joe'})
        self.assertSetEqual(polly.smembers('caretakers'), {'bob'})
        self.assertSetEqual(joe.smembers('cats_to_feed'), {'sphinx'})
        self.assertSetEqual(bob.smembers('cats_to_feed'), {'polly'})

        sphinx.sadd('caretakers', bob)
        self.assertSetEqual(joe.smembers('cats_to_feed'), {'sphinx'})
        self.assertSetEqual(bob.smembers('cats_to_feed'), {'polly', 'sphinx'})
        self.assertSetEqual(polly.smembers('caretakers'), {'bob'})
        self.assertSetEqual(sphinx.smembers('caretakers'), {'joe', 'bob'})

        sphinx.srem('caretakers', bob)
        self.assertSetEqual(sphinx.smembers('caretakers'), {'joe'})
        self.assertSetEqual(bob.smembers('cats_to_feed'), {'polly'})

    def test_self_reference(self):
        joe = Person.create('joe', self.db)
        bob = Person.create('bob', self.db)

        joe.sadd('friends', bob)
        self.assertSetEqual(joe.smembers('friends'), {'bob'})
        self.assertSetEqual(bob.smembers('friends'), {'joe'})

        joe.srem('friends', bob)
        self.assertSetEqual(joe.smembers('friends'), set())
        self.assertSetEqual(bob.smembers('friends'), set())

    def test_delete_entity(self):
        joe = Person.create('joe', self.db)
        joe.delete()
        self.assertListEqual(self.db.keys('*'), [])

        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        polly = Cat.create('polly', self.db)

        joe.sadd('cats', sphinx)
        joe.sadd('cats', polly)
        self.assertSetEqual(joe.smembers('cats'), {'sphinx', 'polly'})

        polly.delete()
        self.assertSetEqual(joe.smembers('cats'), {'sphinx'})
        self.assertEqual(sphinx.hget('owner'), 'joe')

        joe.delete()
        self.assertEqual(sphinx.hget('owner'), None)
        self.assertListEqual(self.db.keys('*'), ['cats'])

    @classmethod
    def tearDownClass(cls):
        #assert(cls.db.keys('*') == [])
        #cls.db.shutdown()
        pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)
