from functools import wraps


def check_field(func):
    @wraps(func)
    def _wrapper(self_cls, field, *args, **kwargs):
        if not field in self_cls.fields:
            raise TypeError('invalid field')
        return func(self_cls, field, *args, **kwargs)
    return _wrapper


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
    if (field1 in entity.fields):
        if (entity1 != entity2):
            raise KeyError('Cannot add relation to existing field')
    entity.fields[field1] = entity2
    return entity


def relate(entityA, fieldA, entityB, fieldB=None):
    """ Relate entityA's fieldA with that of entityB's fieldB. fieldA and
    fieldB are new fields to be defined.

    Container semantics can be used to denote many to many relationships*.

    Example:

    # 1 to N relationship between a person and cats
    relate(Person,'cats',{Cat},'owner'}

    # N to 1 relationship (equivalent to above)
    relate({Cat},'owner',Person,'cats')

    # N to N relationship
    relate({Person},'cats_to_feed',{Cat},'people_who_feed_me')
    # this is equivalent to the imaginary function definition
    forward_map(Person,'cats_to_feed',{Cat})
    inverse_map(Cat,'people_who_feed_me',{Person})

    # N to N relationship between self fields
    relate({Person},'friends',{Person},'friends')

    *Note that not all n-to-n relationships are sensible.

    """

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
    """ An Entity is an entity represented and stored using redis. This class
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

    """

    @classmethod
    def exists(cls, id, db):
        """ Returns true if an entity with id id exists on the db """
        return db.sismember(cls.prefix + 's', id)

    @classmethod
    def create(cls, id, db):
        """ Create an object with identifier id on the redis client db """
        if isinstance(id, bytes):
            raise TypeError('id must be a string')
        if cls.exists(id, db):
            raise KeyError(id, 'already exists')
        db.sadd(cls.prefix + 's', id)
        return cls(id, db)

    @classmethod
    def add_lookup(cls, field, injective=True):
        cls.lookups[field] = injective

    @classmethod
    def instance(cls, id, db):
        return cls(id, db)

    def delete(self):
        """ Remove this entity from the db, all associated fields and related
            fields will also be cleaned up

        """
        for field_name, field_type in self.fields.items():
            if type(field_type) is set:
                if field_name in self.relations:
                    for member in self._db.smembers(self.prefix+':'+self.id
                                                    +':'+field_name):
                        self.srem(field_name, member)
                self._db.delete(self.prefix+':'+self.id+':'+field_name)
            elif issubclass(field_type, Entity):
                self.hdel(field_name)
            elif field_type in (set, int, bool, float):
                # for lookups possibly in the future
                pass
        self._db.delete(self.prefix+':'+self.id)
        self._db.srem(self.prefix+'s', self.id)

    @property
    def id(self):
        return self._id

    @check_field
    def hincrby(self, field, count=1):
        """ Increment the field by count, field must be declared int """
        if self.fields[field] != int:
            raise TypeError('cannot call hincrby on a non-int field')
        return self._db.hincrby(self.prefix + ':' + self._id, field, count)

    @check_field
    def hset(self, field, value):
        """ Set a hash field equal to value """
        # set local value
        assert (self.fields[field] in (str, int, bool, float) or
                issubclass(self.fields[field], Entity))
        if field in self.relations:
            assert isinstance(value, Entity)
            other_entity = self.relations[field][0]
            other_field_name = self.relations[field][1]
            other_field_type = other_entity.fields[other_field_name]

            if type(other_field_type) is set:
                self._db.sadd(other_entity.prefix+':'+value.id+':'+
                              other_field_name, self.id)
            elif issubclass(other_field_type, Entity):
                value.hdel(other_field_name)
                self._db.hset(other_entity.prefix + ':' + value.id,
                              other_field_name, self.id)
            self.hdel(field)
        if isinstance(value, Entity):
            value = value.id
        self._db.hset(self.prefix + ':' + self._id, field, value)

    @check_field
    def hdel(self, field):
        """ Delete a hash field and its related fields and lookups """
        assert (self.fields[field] in (str, int, bool, float) or
                issubclass(self.fields[field], Entity))

        if field in self.relations:
        #if issubclass(self.fields[field], Entity):
            other_entity = self.relations[field][0]
            other_field_name = self.relations[field][1]
            other_field_type = other_entity.fields[other_field_name]
            other_entity_id = self._db.hget(self.prefix+':'+self.id, field)
            if other_entity_id:
                if type(other_field_type) is set:
                    self._db.srem(other_entity.prefix+':'+other_entity_id+':'+
                                  other_field_name, self.id)
                elif issubclass(other_field_type, Entity):
                    self._db.hdel(other_entity.prefix+':'+other_entity_id,
                                  other_field_name)
        self._db.hdel(self.prefix+':'+self.id, field)

    @check_field
    def hget(self, field):
        """ Get a hash field """
        field_type = self.fields[field]
        if (field_type in (str, int, bool, float)):
            val = self._db.hget(self.prefix + ':' + self._id, field)
            if val:
                return field_type(val)
            else:
                return val
        elif issubclass(field_type, Entity):
            return self._db.hget(self.prefix+':'+self._id, field)
        else:
            raise TypeError('Unknown type')

    @check_field
    def smembers(self, field):
        """ Return members of a set """
        if type(self.fields[field]) != set:
            raise KeyError('called smembers on non-set field')
        set_values = set()
        for member in self._db.smembers(self.prefix + ':' + self._id + ':' +
                                        field):
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
        """ Remove values from the set field """
        assert type(self.fields[field]) == set
        carbon_copy_values = []
        for value in values:
            if isinstance(value, Entity):
                carbon_copy_values.append(value.id)
            else:
                carbon_copy_values.append(value)

        if field in self.relations:
            for value in carbon_copy_values:
                other_entity = self.relations[field][0]
                other_field_name = self.relations[field][1]
                other_field_type = other_entity.fields[other_field_name]

                if type(other_field_type) is set:
                    self._db.srem(other_entity.prefix+':'+value+':'+
                                  other_field_name, self.id)
                elif issubclass(other_field_type, Entity):
                    self._db.hdel(other_entity.prefix+':'+value,
                                  other_field_name)

        self._db.srem(self.prefix+':'+self._id+':'+field, *carbon_copy_values)

    @check_field
    def sadd(self, field, *values):
        """ Add values to the field. If the field expects Entities, then values
        can either be a list of strings, a list of Entities, or a mix of both.

        """
        assert type(self.fields[field]) == set
        for key in self.fields[field]:
            derived_entity = key
        carbon_copy_values = []
        # convert all values to strings first
        for value in values:
            if isinstance(value, derived_entity) and isinstance(value, Entity):
                carbon_copy_values.append(value.id)
            elif type(value) == str:
                carbon_copy_values.append(value)
            else:
                raise TypeError('Bad sadd type')

        if field in self.relations:
            other_entity = self.relations[field][0]
            other_field_name = self.relations[field][1]
            other_field_type = other_entity.fields[other_field_name]
            for value in carbon_copy_values:
                if type(other_field_type) is set:
                    self._db.sadd(other_entity.prefix+':'+value+':'+
                                  other_field_name, self.id)
                elif issubclass(other_field_type, Entity):
                    other_entity(value, self._db).hdel(other_field_name)
                    self._db.hset(other_entity.prefix+':'+value,
                                  other_field_name, self.id)

        self._db.sadd(self.prefix+':'+self._id+':'+field, *carbon_copy_values)

    def __init__(self, id, db):
        self._db = db
        self._id = id
        # overhead
        if not self.__class__.exists(id, db):
            raise KeyError(id, 'has not been created yet')
        self.__dict__['_id'] = id
