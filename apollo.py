from functools import wraps
import types
import redis

rc = redis.Redis()
rc.flushdb()

# supports
# object to object relations
# object to primitive relations

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
    
class Entity(metaclass=_modify_derived):
    '''
    An Entity is an entity represented and stored using redis. This class is
    meant to be subclassed using the example template given below. The ids of
    the Entitys that exist are contained in a redis SET. There are three major 
    components to an Entity:

    1. fields - which describes basic features of the Entity using primitives
        such as str,int,float,bool. They can be bracketed in {},[],() to denote
        a redis SET, a redis LIST, and a redis SORTED SET. 
    2. lookups - which are similar to indices in SQL tables, allowing fast
        retrieval of the entity id given a field and the field value. Lookups
        are added via the Entity.add_lookup() method, and is injective by
        default. 
    3. relations - which describe relations between different subclasses of 
        Entity. Relations add additional implicit fields to the Entity

    Example:

    class Person(apollo.Entity):
        prefix = person
        fields = {'age' : int,
                  'income' : int,
                  'ssn' : str,
                  'emails' : {str},
                  'nicknames' : {str}
                 }

    Person.add_lookup('ssn')
    Person.add_lookup('emails')
    Person.add_lookup('nicknames',injective=False)

    class Cat(apollo.Entity):
        prefix = Cat
        fields = {'age' : int,
                  'eye_color' : str,
                  'favorite_foods' : {str},
                  'biochip' : int
                 }

    Cat.add_lookup('biochip')

    # forward map:
    a Person.'cats' field maps to a set of cats
    # inverse map:
    a Cat.'owner' field maps to a single person
    # apollo.relate(Person,{'cats'},Cat,'owner')
    # forward function: Person,'cats',{Cat}
    # inverse function: Cat,'owner',Person
    # (Person,'cats',{Cat},'owner')

    # cyclic form
    # ({Person},'cats_to_feed',{Cat},'care_takers')

    # (Person,'email',{str})

    # Get a person's emails: Person.instance('joe')['emails']
    # Usage, Person.lookup('emails','some_email@mail.com')

    # forward function:
    a Person.'email' field maps to a set of strings
    # inverse map
    Person.lookup('email')

    Person.set_lookup('email',{str})

    # Without lookups


    # forward map:
    a Perso

    class Person(apollo.Object):    
        prefix = 'person'
        fields = {'ssn' : str,
                  'age' : int,
                  'random_hobbies' : {str},
                  'emails' : (str, injective_lookup)
                  }

        no_lookups

        injective_lookups

        non_injective_lookups

        ssn = field(str,lookup=True,backref=True)

        _relations = {'cats' : (Cat, owner),
                      'emails' : str}      


    rel = Person.fwd_relation('kids',{Person})
    rel.back_relation('parents',{Person})

    Person.fwd_relation('cats',{Cat})
    Cat.back_relation('owner',Person)

    Person.fwd_relation('emails',{str},injective=False)
    apollo.relate({Person},'emails',{str})

    # with lookup
    apollo.relate(Person,'emails',{str})
    # no lookup
    Person.create_field('cats',{Cat})

    Person.lookup()


    # 1-to-n
    apollo.relate(Person,'emails',{str})
    # 1-to-1
    apollo.relate(Person,'email',str)
    
    class Cat(apollo.Object):
        prefix = 'cat'
        fields = {'age' : int,
                  'favorite_food' : str,
                  'biometric_id' : int
                 }
        lookups = {'biometric_id'}

    # self relations (implied lookup)
    # given two person a,b
    # a \in b['neighbors'] <=> b \in a['neighbors']
    apollo.relate(Person,{'neighbors'},Person,{'neighbors'})

    # lookup a person given ssn or email
    Person.lookup('ssn','123-45-6789')
    Person.lookup('email','fantasy@gmail.com')

    # 1-to-1 relationship)
    apollo.relate(Cat,'person_soulmate',Person,'cat_soulmate')
    
    # 1-to-1 relationship
    apollo.relate(Person,'husband',Person,'wife')

    # 1-to-n relationship
    apollo.relate(Person,{'employees'},Person,'boss')

    # 1-to-n with no reverse lookup needed
    apollo.relate(Person,{'random_cats'},Cat)

    # n-to-n relationship
    apollo.relate(Person,{'cats_to_feed'},Cat,{'caretakers'})

    '''

    def _str_to_class(class_string):
        for some_subclass in self.__subclasses__():
            if some_subclass.__name__ == class_string:
                return some_subclass
            
    @classmethod
    def exists(cls,id,db):
        return db.sismember(cls.prefix+'s',id)
  
    @classmethod
    def create(cls,id,db):
        if isinstance(id,bytes):
            raise TypeError('id must be a string')
        if cls.exists(id,db):
            raise KeyError(id,'already exists')
        db.sadd(cls.prefix+'s',id)
        return cls(id,db)

    @classmethod
    def instance(cls,id,db):
        return cls(id,db)

    @classmethod
    def delete(cls,id,db):
        print('classmethod delete:',cls,id,db)
        pass

    @property
    def id(self):
        return self._id

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
        ''' Set the object's field equal to that of value.

            Logic:
                                        |
                              is field a container?
                                |               |
                               yes              no
                                |               |
                              is field a primitive?
                                |               |
                               yes              no
                                |               |
                does a previous lookup/relation already exist?
                                |               |
                               yes              no
                                |               |
                        is it a lookup?  is it a relation?
                        |       |               |       |
                       yes      no             yes      no 
                        |                       |
                      1-to-n      is the relation a container?
                                        |               |
                                       yes              no
                                        |               |
                                      n-to-n          1-to-n
                                                

                        
        '''
        # is field a container?
        if type(self.fields[field]) in (set,list,tuple):
            pass
        else:
            # is field a primitive?
            if self.fields[field] in (str,float,int,bool):
                self._db.hset(self.prefix+':'+self._id,field,value)
                # is it a lookup?
                if field in self.lookups:
                    
                    # is the referenced lookup a container?
                    if self.fields[field]
                else:
                    pass
            elif issubclass(self.fields[field],Object):
                if isinstance(value,Object):
                    value_id = value.id
                else:
                    value_id = value
                self._db.hset(self.prefix+':'+self._id,field,value_id)
                # is it a lookup?
            else:
                raise TypeError('unknown field mapping') 

        if self.fields[field] is set:
            pass
        elif self.fields[field] is list:
            pass
        elif self.fields[field] is tuple:
            pass
        elif issubclass(self.fields[field],Object):
            assert isinstance(value,str) or isinstance(value,Object)
            if field in self.relations:
                object_type = self.relations[field][0]
                field_name = self.relations[field][1]
                field_type = object_type.fields[field_name]
                if isinstance(value,Object):
                    object_id = value.id
                if not object_type.exists(object_id, self._db):
                    raise ValueError('object '+object_id+' does not exist in '+object_type)
                if not field_type in (set,tuple,list):
                    self._db.hset(object_type.prefix+':'+value,field_name,self._id)
                    # infinite loops instance[field_name] = self._id
                elif field_type is set:
                    pass
                elif field_type is tuple:
                    pass
            self._db.hset(self.__class__.prefix+':'+self._id, field, value)
        else:
            print('debug')
            self._db.hset(self.__class__.prefix+':'+self._id, field, value)
            
    def __init__(self,id,db):
        self._db = db
        self._id = id
        self.delete = types.MethodType(_instance_delete,self)
        # overhead
        print('instancing ', id)
        
        if not self.__class__.exists(id,db):
            raise KeyError(id,'has not been created yet')
        self.__dict__['_id'] = id