var objects = require('ringo/utils/objects');
var {addHostObject} = require('ringo/engine');
var {bindArguments} = require('ringo/functional');
var {equalKeys, updateEntity, getProperties, getKey, getId, isEntity, isKey, createKey, BaseTransaction}
                = require('ringo/storage/storeutils');

var redis =  new (require('ngv/redis').Redis) ();

var log = require('ringo/logging').getLogger(module.id);

export('Store');

addHostObject(org.ringojs.wrappers.Storable);

/**
 * Redis Store class
 * @param path the database directory
 */
function Store(host, port) {

    // map of type to current id tip
    var idMap = {};
    var data = {};
    var registry = {};

    this.dump = function() {
        print(data.toSource());
    };

    var proxy = {
        all: all,
        get: get,
        query: query,
        create: create,
        save: save,
        remove: remove,
        getEntity: getEntity,
        getKey: getKey,
        getProperties: getProperties,
        getId: getId,
        equalKeys: equalKeys
    };

    this.defineEntity = function(type) {
        var ctor = registry[type];
        if (!ctor) {
            ctor = registry[type] = Storable.defineEntity(proxy, type);
            ctor.all = bindArguments(all, type);
            ctor.get = bindArguments(get, type);
            ctor.query = bindArguments(query, type);
        }
        return ctor;
    };

    function create(type, key, entity) {
        var ctor = registry[type];
        if (!ctor) {
            throw new Error('Entity "' + type + '" is not defined');
        }
        return ctor.createInstance(key, entity);
    }

    function all(type) {
        return retrieveAll(type);
    }

    function get(type, id) {
        return retrieve(type, id);
    }

    function save(props, entity, txn) {
        if (!txn) {
            txn = new BaseTransaction();
        }
        if (updateEntity(props, entity, txn)) {
            store(entity, txn);
        }
    }


    function query(type) {
        return new BaseQuery(type);
    }


    function getEntity(type, arg) {
        if (isKey(arg)) {
            var [type, id] = arg.$ref.split(":");
            return load(type, id);
        } else if (isEntity(arg)) {
            return arg;
        } else if (arg instanceof Object) {
            var entity = objects.clone(arg);
            Object.defineProperty(entity, "_key", {
                value: createKey(type, generateId(type))
            });
            return entity;
        }
        return null;
    }

    function store(entity, txn) {
        try {
        	redis.set(entity._key.$ref, JSON.stringify(entity));
        	var [type, id] = entity._key.$ref.split(':');
        	redis.sadd(type+':ids', id);
        } catch(e) {
            //TODO
        }
    }

    function load(type, id) {
        var redisValue = redis.get([type,id].join(":"));
        if (redisValue) {
            var entity = JSON.parse(redisValue);
            Object.defineProperty(entity, "_key", {
                value: createKey(type, id)
            });
        }
        return entity;
    }

    function retrieve(type, id) {
        var entity = load(type, id);
        if (entity) {
            return create(type, entity._key, entity);
        }
        return null;
    }

    function retrieveAll(type) {
        try {
            var keys = redis.smembers([type, 'ids'].join(':'));
        } catch(e) {
            //TODO
        }
        var list = [];
        for (var i=0; i < keys.length; i++) {
            list.push(retrieve(type, keys[i]));
        }
        return list;
    }

    function remove(key, txn) {
        if (!isKey(key)) {
            throw new Error("Invalid key object: " + key);
        }
        var [type, id] = key.$ref.split(":");
        try {
            redis.del([key.$ref, 'json'].join(':'));
            redis.srem([type,'ids'].join(':'), id);
        } catch(e) {
            //TODO
        }
    }

    function generateId(type) {
          try {
              return redis.incr(type+':maxId');
          } catch(e) {
              //TODO
          }
    }
}

function dump() {
    datastore.dump();
}


function evaluateQuery(query, property, options) {
    var result = [];
    var type = query.getKind();
    var prepared = datastore.prepare(query);
    var i = options ? prepared.asIterator(options) : prepared.asIterator();
    while (i.hasNext()) {
        var entity = i.next();
        var s = create(type, entity.getKey(), entity);
        result.push(property ? s[property] : s);
    }
    return result;
}


function BaseQuery(type) {
    var query = new Query(type);
    var options;

    this.select = function(property) {
        return evaluateQuery(query, property, options);
    };

    this.equals = function(property, value) {
        query.addFilter(property, EQUAL, value);
        return this;
    };

    this.greater = function(property, value) {
        query.addFilter(property, GREATER_THAN, value);
        return this;
    };

    this.greaterEquals = function(property, value) {
        query.addFilter(property, GREATER_THAN_OR_EQUAL, value);
        return this;
    };

    this.less = function(property, value) {
        query.addFilter(property, LESS_THAN, value);
        return this;
    };

    this.lessEquals = function(property, value) {
        query.addFilter(property, LESS_THAN_OR_EQUAL, value);
        return this;
    };

    this.orderBy = function(value, direction) {
        direction = /desc/i.test(direction) ? DESCENDING : ASCENDING;
        query.addSort(value, direction);
        return this;
    };

    this.limit = function(value) {
        options = options ? options.limit(value) : FetchOptions.Builder.withLimit(value);
        return this;
    };

    this.offset = function(value) {
        options = options ? options.offset(value) : FetchOptions.Builder.withOffset(value);
        return this;
    };
}
