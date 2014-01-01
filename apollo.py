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
    print(instance)
    print('instance_delete_called')
    pass

def relate(objA, fieldA, objB, fieldB):
    objA.relations[fieldA] = (objB, fieldB)
    objB.relations[fieldB] = (objA, fieldA)

class myMc(type):
    def __new__(cls,clsname,bases,attrs):
        for key in attrs['fields']:
            print(key)
            attrs['fields'][key]=Base._str_to_class(attrs['fields'][key])
        return super(myMc, cls).__new__(cls, clsname, bases, attrs)

class Base(metaclass=myMc):
    @classmethod
    def _str_to_class(cls,class_string):
        for some_subclass in cls.__subclasses__():
            if some_subclass.__name__ == class_string:
                return some_subclass
    
    fields = {}
    
class Test(Base):
    fields = {'case' : 'Test'}
    
class Object(metaclass=):
    ''' 
    class Person(apollo.Object):
        prefix = 'person'
        fields = {'ssn' : str,  
                  'email' : str,
                  'age' : int,
                  'minions' : {Person},
                  'boss' : Person,
                  'favorite_people' : (Person),
                  'cats_to_feed' : {Cat},
                  'cat_soulmate' : Cat
                 }
        lookups = {'ssn','email'}

    class Cat(apollo.Object):
        prefix = 'cat'
        fields = {'age' : int,
                  'favorite_food' : str,
                  'biometric_id' : int,
                  'caretakers' : {Person},
                  'person_soulmate' : Person
                 }
        lookups = {'biometric_id'}

    # 1-to-1 relationship
    apollo.relate(Cat,'person_soulmate',Person,'cat_soulmate')

    # relate creates the following mapping
    #   Cat.relations.append('person_soulmate' : (Person, 'cat_soulmate'))
    #   Person.relations.append('cat_soulmate' : (Cat, 'person_soulmate'))
    
    # sphinx['person_soulmate'] = 'joe'
    # the following commands are executed
    #   if 'person_soulmate' in Cat.relations:
    #       ObjectType = Cat.relations['person_soulmate'][0]
    #       FieldName = Cat.relations['person_soulmate'][1]
    #       FieldType = ObjectType.fields[FieldName]
    #       instance = ObjectType.instance('joe')
    #       if not FieldType in (set,tuple,list):
    #           instance[FieldName] = sphinx.id
    #       elif FieldType is set:
    #           instance.sadd('FieldName',sphinx.id)
    #       elif FieldType

    # 1-to-n relationship
    apollo.relate(Person,'minions',Person,'boss')

    # n-to-n relationship
    apollo.relate(Person,'cats_to_feed',Cat,'caretakers')
    
    # usage:
    joe = Person.create('joe')
    jamie = Person.create('jamie')
    jack = Person.create('jack')

    joe['minions'] = {'jamie','jack'}
    jamie['boss'] # returns 'joe'
    jack['boss'] # returns 'joe'

    sphinx = Cat.create('sphinx')
    sphinx.caretakers = {'joe','jamie'}
    felix = Cat.create('felix')
    felix.caretakers = {'jamie','jack'}

    joe['cats_to_feed'] # returns {'sphinx'}
    jamie['cats_to_feed'] # returns {'sphinx','felix'}
    jack['cats_to_feed'] # returns {'felix'}
    '''

    lookups = []
    relations = {}

    def _str_to_class(class_string):
        for some_subclass in self.__subclasses__():
            if some_subclass.__name__ == class_string:
                return some_subclass
    
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
        print('classmethod delete:',cls,id,db)
        pass

    @check_field 
    def __getitem__(self, field):
        if self.fields[field] is set:
            return self._db.smembers(self.__class__.prefix+':'+self._id+':'+field)
        elif self.fields[field] is list:
            pass
        elif self.fields[field] is tuple:
            pass
        else:
            return self.__class__.fields[field](self._db.hget(self.__class__.prefix+':'+self._id, field))

    @check_field
    def __setitem__(self, field, value):
        if self.fields[field] is set:
            pass
        elif self.fields[field] is list:
            pass
        elif self.fields[field] is tuple:
            pass
        else:
            self._db.hset(self.__class__.prefix+':'+self._id, field, value)

    def __init__(self,id,db,verify=False):
        self._db = db
        self._id = id
        self.delete = types.MethodType(_instance_delete,self)
        # overhead
        if verify:
            if not self.__class__.exists(id,db):
                raise KeyError(id,'has not been created yet')
        self.__dict__['_id'] = id
