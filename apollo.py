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
        Entity. Relations add additional implicit fields to the Entity that can
        be queried. 

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