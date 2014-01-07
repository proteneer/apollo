from functools import wraps
import types
import redis

rc = redis.Redis()
rc.flushdb()


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


def _set_relation(entity1, field1, entity2):
    if type(entity1) is set:
        for element in entity1:
            entity = element
    elif type(entity1) in (list, tuple):
        entity = entity1[0]
    elif issubclass(entity1, Entity):
        entity = entity1
    else:
        raise TypeError('Unknown entity type')
    if field1 in entity.fields:
        raise KeyError('Cannot add relation to existing field')
    entity.fields[field1] = entity2
    return entity


def relate(entityA, fieldA, entityB, fieldB=None):
    ''' Relate entityA's fieldA with that of entityB's fieldB. fieldA and
        fieldB are new fields.

        Container semantics can be used to denote many to many relationships*.

        Example:

        # 1 to N relationship between a person and cats
        relate(Person,'cats',{Cat},'owner'}

        # N to 1 relationship (equivalent to above)
        relate({Cat},'owner',Person,'cats')

        # N to N relationship
        relate({Person},'cats_to_feed',{Cat},'people_who_feed_me')
        # this is equivalent to the following fun
        forward_mapping(Person,'cats_to_feed',{Cat})
        inverse_mapping(Cat,'people_who_feed_me',{Person})

        # N to N relationship between self fields
        relate({Person},'friends',{Person},'friends')

        *Note that not all n-to-n relationships are sensible.
        '''

    entity1 = _set_relation(entityA, fieldA, entityB)
    if fieldB:
        entity2 = _set_relation(entityB, fieldB, entityA)
        entity1.relations[fieldA] = (entity2, fieldB)
        entity2.relations[fieldB] = (entity1, fieldA)


class _entity_metaclass(type):

    def __new__(cls, clsname, bases, attrs):
        # create these only for derived classes of Entity
        if len(bases) > 0:
            mandatory_fields = ('fields', 'relations', 'lookups')
            for field in mandatory_fields:
                if not field in attrs:
                    attrs[field] = dict()
        return super(_entity_metaclass, cls).__new__(
            cls, clsname, bases, attrs)


class Entity(metaclass=_entity_metaclass):
    ''' An Entity is an entity represented and stored using redis. This class
    is meant to be subclassed using the example template given below. Entities
    are indexed using an id, similar to the primary key in SQL. These ids are
    are contained in a redis SET for book-keeping purposes. There are three
    major components to an Entity:

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
    directly to the lifetime of the underlying object. The life time of lookups
    are bound to the lifetime of the entity.

    Relations on the otherhand, are used to describe relationships between two
    entities. It is similar to how SQL relates between two tables. Even if a
    related field is deleted, the entity itself still exists in the set of
    managed entities.

    N-to-N Relations between different sets are a tricky business. For example,
    mappings from sets to sets can make intuitive sense, so does sets to sorted
    sets and possibly sets to lists. However, sorted sets to sorted sets are
    seemingly nonsensical, as are sorted sets to lists, and lists to lists.
    For this reason, sorted sets and lists can only map to either single
    objects or sets, but not to other sorted sets or lists.

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

    @classmethod
    def exists(cls, id, db):
        return db.sismember(cls.prefix+'s', id)

    @classmethod
    def create(cls, id, db):
        if isinstance(id, bytes):
            raise TypeError('id must be a string')
        if cls.exists(id, db):
            raise KeyError(id, 'already exists')
        db.sadd(cls.prefix+'s', id)
        return cls(id, db)

    @classmethod
    def add_lookup(cls, field, injective=True):
        cls.lookups[field] = injective

    @classmethod
    def instance(cls, id, db):
        return cls(id, db)

    @classmethod
    def delete(cls, id, db):
        print('classmethod delete:', cls, id, db)
        pass

    @property
    def id(self):
        return self._id

    @check_field
    def __getitem__(self, field):
        if self.fields[field] is set:
            return self._db.smembers(self.prefix+':'+self._id+':'+field)
        elif self.fields[field] is list:
            pass
        elif self.fields[field] is tuple:
            pass
        else:
            return self.fields[field](
                self._db.hget(self.prefix+':'+self._id, field))

    def _remove_old_relations(self, field, value):
        other_entity = self.relations[field][0]
        other_field_name = self.relations[field][1]
        other_field_type = other_entity.fields[other_field_name]
        if type(self.fields[field]) is set:
            # removing relations from sets is a really hairy business.

            # N to N
            if type(other_field_type) is set:
                self._db.srem(other_entity.prefix + ':' + value + ':' +
                              other_field_name, self.id)
            # N to 1
            elif issubclass(other_field_type, Entity):
                old_owner = self._db.hget(other_entity.prefix+':'+value,
                                          other_field_name)
                if old_owner:
                    self._db.srem(self.prefix+':'+old_owner+':'+field, value)
                self._db.hdel(other_entity.prefix+':'+value, other_field_name,
                              self.id)
        # if the field is not in a container, then the relationship is
        # 1 to N or 1 to 1
        else:
            old_value = self._db.hget(self.prefix+':'+self.id, field)
            if old_value:
                if type(other_field_type) is set:
                    self._db.srem(other_entity.prefix+':'+old_value
                                  + ':' + other_field_name, self.id)
                elif issubclass(other_field_type, Entity):
                    self._db.hdel(other_entity.prefix+':'+old_value,
                                  other_field_name, self.id)
                else:
                    raise TypeError('Unsupported type')

    def _add_new_relations(self, field, value):
        other_entity = self.relations[field][0]
        other_field_name = self.relations[field][1]
        other_field_type = other_entity.fields[other_field_name]
        if type(other_field_type) is set:
            self._db.sadd(other_entity.prefix+':'+value
                          + ':' + other_field_name, self.id)
        elif issubclass(other_field_type, Entity):
            self._db.hset(other_entity.prefix+':'+value, other_field_name,
                          self.id)
        else:
            raise TypeError('Unsupported type')

    def _check_lookup_or_relation(self, field, value):
        ''' not pipeable '''
        # set lookup/relations
        assert type(value) in (str, int, bool)
        if field in self.lookups:
            # injective lookup
            if self.lookups[field]:
                self._db.hset(field+':'+value, self.prefix, self.id)
            else:
                self._db.sadd(field+':'+value+':'+self.prefix, self.id)
        elif field in self.relations:
            self._remove_old_relations(field, value)
            self._add_new_relations(field, value)

    @check_field
    def hset(self, field, value):
        ''' Set a hash field.
        '''
        # set local value
        assert (self.fields[field] in (str, int, bool, float) or
                issubclass(self.fields[field], Entity))
        if isinstance(value, Entity):
            value = value.id

        self._check_lookup_or_relation(field, value)
        self._db.hset(self.prefix+':'+self._id, field, value)

    @check_field
    def hget(self, field):
        ''' Get a hash field
        '''
        field_type = self.fields[field]
        if (field_type in (str, int, bool, float)):
            return field_type(self._db.hget(self.prefix+':'+self._id, field))
        elif issubclass(field_type, Entity):
            return self._db.hget(self.prefix+':'+self._id, field)
        else:
            raise TypeError('Unknown type')

    @check_field
    def smembers(self, field):
        ''' Return members of a set '''
        if type(self.fields[field]) != set:
            raise KeyError('called smembers on non-set field')
        set_values = set()
        for member in self._db.smembers(self.prefix+':'+self._id+':'+field):
            for primitive_type in self.fields[field]:
                if issubclass(primitive_type, Entity):
                    set_values.add(member)
                elif primitive_type in (str, int, bool, float):
                    set_values.add(primitive_type(member))
                else:
                    raise TypeError('Unknown field type')
        return set_values

    @check_field
    def srem(self, field, *values):
        assert type(self.fields[field]) == set
        carbon_copy_values = []
        for value in values:
            if isinstance(value, Entity):
                carbon_copy_values.append(value.id)
            else:
                carbon_copy_values.append(value)

        for value in values:
            if isinstance(value, Entity):
                value = value.id
            self._remove_old_relations(field, value)

        self._db.srem(self.prefix+':'+self._id+':'+field, *carbon_copy_values)

    @check_field
    def sadd(self, field, *values):
        assert type(self.fields[field]) == set
        carbon_copy_values = []
        for value in values:
            if isinstance(value, Entity):
                carbon_copy_values.append(value.id)
            else:
                carbon_copy_values.append(value)
        for value in carbon_copy_values:
            self._check_lookup_or_relation(field, value)
        self._db.sadd(self.prefix+':'+self._id+':'+field, *carbon_copy_values)

    @check_field
    def __setitem__(self, field, value):
        ''' Set the object's field equal to that of value.

            this is a convenience method that basically calls one of:

            sadd (for adding to sets)
            hset (for setting a field value)
            srem (for removing from a set)

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
            if type(item_field_type) in (str, int, float, bool):
                if value in (str, float, int, bool):
                    self._db.sadd(self.prefix+':'+self._id, field, value)
            if issubclass(item_field_type, Object):
                if isinstance(value, self.fields[field]):
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

    def __init__(self, id, db):
        self._db = db
        self._id = id
        self.delete = types.MethodType(_instance_delete, self)

        # overhead
        if not self.__class__.exists(id, db):
            raise KeyError(id, 'has not been created yet')
        self.__dict__['_id'] = id
