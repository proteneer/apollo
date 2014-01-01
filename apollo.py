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

def _transfer(type1, field1, type2):
    if type(field1) is set:
        for val in field1:
            field_name = val
        type1.fields[field_name] = {type2}
    elif type(field1) is list:
        field_name = field1[0]
        type1.fields[field_name] = [type2]
    elif type(field1) is tuple:
        field_name = field1[0]
        type1.fields[field_name] = (type2)
    else:
        field_name = field1
        type1.fields[field_name] = type2
    return field_name
    
def relate(typeA, fieldA, typeB, fieldB=None):
    field_nameA = _transfer(typeA, fieldA, typeB)
    if fieldB:
        field_nameB = _transfer(typeB, fieldB, typeA)
        typeA.relations[field_nameA] = (typeB, field_nameB)
        typeB.relations[field_nameB] = (typeA, field_nameA)

class _modify_derived(type):
    def __new__(cls,clsname,bases,attrs):
        if len(bases) > 0:
            attrs['fields'] = dict()
            attrs['relations'] = dict()
        return super(_modify_derived, cls).__new__(cls, clsname, bases, attrs)
    
class Object(metaclass=_modify_derived):
    ''' 
    class Person(apollo.Object):    
        prefix = 'person'
        fields = {'ssn' : str,  
                  'email' : str,
                  'age' : int
                  }
        lookups = {'ssn','email'}

    class Cat(apollo.Object):
        prefix = 'cat'
        fields = {'age' : int,
                  'favorite_food' : str,
                  'biometric_id' : int
                 }
        lookups = {'biometric_id'}

    # 1-to-1 relationship
    apollo.relate(Cat,'person_soulmate',Person,'cat_soulmate')

    # relate does the following:
    #   Cat.fields['person_soulmate'] = Person
    #   Person.fields['cat_soulmate'] = Cat
    
    # sphinx['person_soulmate'] = 'joe'
    # the following commands are executed
    #   if 'person_soulmate' in Cat.relations:
    #       
    #       ObjectType = Cat.relations['person_soulmate'][0]
    #       FieldName = Cat.relations['person_soulmate'][1]
    #       FieldType = ObjectType.fields[FieldName]
    #       instance = ObjectType.instance('joe')
    #       if not FieldType in (set,tuple,list):
    #           instance[FieldName] = sphinx.id
    #       elif FieldType is set:
    #           instance.sadd('FieldName',sphinx.id)
    #       elif FieldType
    
    # define relations explicitly
    
    # 1-to-n relationship
    # a person's list of minions is related to a person's boss
    apollo.relate(Person,{'minions'},Person,'boss')
    
    # Person.fields['minion'] = {Person}
    # Person.fields['boss'] = Person
    
    # n-to-n relationship
    apollo.relate(Person,{'cats_to_feed'},Cat,{'caretakers'})
    
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
