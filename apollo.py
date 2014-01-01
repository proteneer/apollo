from functools import wraps
import types

def check_field(func):
    @wraps(func)
    def _wrapper(self_cls, field, *args, **kwargs):
        if not field in self_cls.fields:
            raise TypeError('invalid field')
        return func(self_cls, field, *args, **kwargs)
    return _wrapper

relations = {}

def _instance_delete(instance):
    print instance
    print 'instance_delete_called'
    pass

class Object(object):
    ''' 
    class Person(apollo.Object):
        prefix = 'person'
        fields = {'ssn' : str,  
                  'email' : str,
                  'age' : int,
                  'minions' : {Person},
                  'boss' : Person,
                  'favorite_people' : (Person),
                  'cats_to_feed' : {Cat}
                 }
        lookups = {'ssn','email'}

    class Cat(apollo.Object):
        prefix = 'cat'
        fields = {'age' : int,
                  'favorite_food' : str,
                  'biometric_id' : int,
                  'caretakers' : {Person}
                 }
        lookups = {'biometric_id'}

    # 1-to-n relationship
    apollo.relate(Person,'minions',Person,'boss')

    # n-to-n relationship
    apollo.relate(Person,'cats',Cat,'owner')
    
    # usage:
    joe = Person.create('joe')
    joe['ssn'] = '123-45-6789'
    joe['email'] = 'joe@gmail.com'
    
    jamie = Person.create('jamie')
    jack = Person.create('jack')

    joe['minions'] = {'jamie','jack'}
    jamie['boss'] # returns 'joe'
    jack['boss'] # returns 'joe'
    '''

    @classmethod
    def create(cls,id,db):
        if isinstance(id,bytes):
            raise TypeError('id must be a string')
        if cls.exists(id,db):
            raise KeyError(id,'already exists')
        db.sadd(cls.prefix+'s',id)
        return cls(id,db)

    @classmethod
    def instance(cls,*args,**kwargs):
        return cls(id,*args,**kwargs)

    @classmethod
    def delete(cls,id,db):
        print cls,id,db
        pass

    def __init__(self,id,db,verify=False):
        self._db = db
        self._id = id
        self.delete = types.MethodType(_instance_delete,self)
        # overhead
        if verify:
            if not self.__class__.exists(id,db):
                raise KeyError(id,'has not been created yet')
        self.__dict__['_id'] = id
