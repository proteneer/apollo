#apollo
[![Build Status](https://travis-ci.org/proteneer/apollo.png?branch=master)](https://travis-ci.org/proteneer/apollo)

A library to help describe simple entities and relations in redis. Currently supports only python 3.3+

##Lookups

```python

db = redis.Redis(decode_responses=True)

# Declaration
class Person(apollo.Entity):
    prefix = 'person'
    fields = {'age': int,
              'ssn': str,
              'favorite_foods': {str}
              }
              
    # ssn lookups return a single person
    Person.add_lookup('ssn')
    # favorite_food lookups return multiple persons
    Person.add_lookup('favorite_food', injective=False)

joe = Person.create('joe', db)  # Create joe on the db
joe.hset('ssn', '123-45-6789')  # set the ssn of joe
joe.hget('ssn')  # get the ssn of joe
joe.hset('bad_field')  # bad fields raise exceptions
Person.lookup('ssn', '123-45-6789', db)  # find which person the ssn belongs to:


joe.hset('favorite_food', 'pizza')  # add pizza to list of joe's favorite foods
bob = Person.create('bob', db)  # make a new person bob
bob.hset('favorite_food', 'pizza')  # bob also likes pizza
Person.lookup('favorite_food', 'pizza', db)  # returns {'bob','joe'}

```

##Relations

```python
class Person(apollo.Entity):
    prefix = 'person'

class Cat(apollo.Entity):
    prefix = 'cat'
    

apollo.relate(Person, 'cats', {Cat}, 'owner')  # 1 to N relationship
apollo.relate({Person}, 'cats_to_feed', {Cat}, 'caretakers')  # N to N relationship
apollo.relate(Person, 'single_cat', Cat, 'single_owner')  # 1 to 1 relationship

# self relations are also possible
apollo.relate({Person}, 'friends', {Person}, 'friends')
apollo.relate(Person, 'best_friend', Person, 'best_friend')

joe = Person.create('joe', db)
sphinx = Cat.create('sphinx', db)
joe.sadd('cats', sphinx)
sphinx.hget('owner')  # returns 'joe'

polly = Cat.create('polly', db)
polly.hset('owner', joe)
polly.hget('owner')  # returns 'joe'
joe.smembers('cats')  # returns {'sphinx', 'polly'}

```
