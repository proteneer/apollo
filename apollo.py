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
            attrs['lookups'] = dict()
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
        Entity. Relations add additional implicit fields to the Entity that can
        be queried. 

    So when should you use a lookup, and when should you create another entity
    and define a relation?

    Use lookups when you don't care about being able to list the entire set and
    test existence of a value in constant time. The 'age' field should be a 
    lookup because we almost never need to see if a given age in a set of all
    existing ages, in constant time, though we could certainly iterate over all 
    the person's ages in O(N) time. The lifetime of a lookup field is tied
    directly to the lifetime of the underlying object. In a lookup, there is no 
    set of 'ages' used to keep track of all the existing ages, just a bunch of 
    key value stores.

    N-to-N Relations between different sets are a tricky business. For examples,
    mappings from sets to sets make natural and intuitive sense, so does sets to
    sorted sets and possibly sets to lists. However, sorted sets to sorted sets
    are seemingly nonsensical, as are sorted sets to lists, and lists to lists.
    For this reason, sorted sets and lists can only map to either single objects
    or sets, but not to other sorted sets or lists.

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


    # A Person's set of cats map to a Cat's owner
    apollo.relate(Person,'cats',{Cat},'owner')
    apollo.relate({Person},'cats_to_feed',{Cat},'caretakers')
    
    # 1 to 1 with no lookup
    apollo.relate(Person,'favorite_cat',Cat)
    # 1 to N with no lookup
    apollo.relate(Person,'favorite_cats',{Cat})
    # N to N with no lookup (makes no sense)

    # 1 to 1 with lookup
    apollo.relate(Person,'cat_buddy',Cat,'person_buddy')
    # 1 to N with lookup
    apollo.relate(Person,'cats_owned',{Cat},'owner')
    # N to N with lookup
    apollo.relate({Person},'cats_to_feed',{Cat},'persons_feeding_me')
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
    def add_lookup(cls,field,injective=True):
        cls.lookups[field]=injective

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
    def sadd(self, field, *values):
        assert type(self.fields[field]) == set
        self._db.sadd(self.prefix+':'+self._id+':'+field, *values)
        if field in self.lookups:
            for value in values:
                if self.lookups[field]:
                    self._db.hset(field+':'+value,self.prefix,self.id)
                else:
                    self._db.sadd(field+':'+value+':'+self.prefix,self.id)
        elif field in self.relations:
            for value in values:
                foreign_object_type = self.relations[field][0]
                foreign_field_name = self.relations[field][1]
                foreign_type = foreign_object_type.fields[foreign_field_name]
            if foreign_type is set:
                self._db.sadd(field)
            elif foreign_type in (str,int,bool,float):


    @check_field
    def __setitem__(self, field, value):
        ''' Set the object's field equal to that of value.

            Logic:

                              is field a container?
                                |               |
                               yes              no
                                |               |
                              is field a primitive?
                                |               |
                               yes              no
                

                       is the field a lookup or a relation?
                                |               |
                               yes              no
                                |               |
                        is it a lookup?  is it a relation?
                        |       |               |       |
                       yes      no             yes      no 
                        |                       |
                   injective?     is the relation a container?
                    |      |            |               |
                   yes     no          yes              no
        '''

        # [ set local key ]

        # is field a container?
        if type(self.fields[field]) is set:
            assert type(value) == self.fields[field]
            item_field_type = iter(self.fields[field]).next()
            if type(item_field_type) in (str,int,float,bool):
                
                if value in (str,float,int,bool):
                    self._db.sadd(self.prefix+':'+self._id,field,value)
            if issubclass(item_field_type,Object):
                if isinstance(value,self.fields[field]):
                    self._db.hset(self.prefix+':'+self._id,field,value.prefix)
                elif isinstance(value,str):
                    self._db.hset(self.prefix+':'+self._id,field,value)
                else:
                    raise TypeError('value type not compatible with field type')
        elif type(self.fields[field]) is list:
                raise TypeError('Not implemented')
        elif type(self.fields[field]) is tuple:
                raise TypeError('Not implemented')
        # not a container        
        elif type(self.fields[field]) in (str,int,float,bool):
            assert type(value) is self.fields[field]
            if value in (str,float,int,bool):
                self._db.hset(self.prefix+':'+self._id,field,value)
        elif issubclass(self.fields[field],Object):
            if isinstance(value,self.fields[field]):
                self._db.hset(self.prefix+':'+self._id,field,value.prefix)
            elif isinstance(value,str):
                self._db.hset(self.prefix+':'+self._id,field,value)
            else:
                raise TypeError('value type not compatible with field type')
        else:
            raise TypeError('Unknown field type',type(self.fields[field]))

        # [ set foreign key ]

        if field in self.lookups:
            # check if referenced lookup is injective
            if self.fields[field]:
                self._db.hset(field+':'+value,self.prefix,value)
            else:
                self._db.sadd(field+':'+value+':'+self.prefix,value)
        elif field in self.relations:
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