require('core/object');
require('core/array');

include('ringo/engine');
include('ringo/functional');
include('ringo/storage/storeutils');
include('ringo/storage/querysupport');


var redis =  new (require('ngv/redis').Redis) ();

var log = require('ringo/logging').getLogger(module.id);

export('Store');

addHostObject(org.ringojs.wrappers.Storable);

/**
 * Redis Store class
 * @param path the database directory
 */

/**
 * Redis Store class
 */
function Store() {

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

    function save(props, entity) {
        if (updateEntity(props, entity)) {
            store(entity);
        }
    }

    function query(type) {
        return new BaseQuery(bindArguments(all, type));
    }


    function getEntity(type, arg) {
        if (isKey(arg)) {
            var [type, id] = arg.$ref.split(":");
            return load(type, id);
        } else if (isEntity(arg)) {
            return arg;
        } else if (arg instanceof Object) {
            var entity = arg.clone();
            Object.defineProperty(entity, "_key", {
                value: createKey(type, generateId(type))
            });
            return entity;
        }
        return null;
    }

    function store(entity) {
        var [type, id] = entity._key.$ref.split(":");

        var redisKey = makeRedisKey(type, id);

        if(log.isDebugEnabled())
            log.debug("Storing object: " + entity.toSource());

        redis.set(JSON.stringify(entity));
    }

    function load(type, id) {
        var redisKey = makeRedisKey(type, id);

        if (!redis.exists(redisKey)) {
            return null;
        }
        var entity = JSON.parse(redis.get(redisKey));
        Object.defineProperty(entity, "_key", {
            value: createKey(type, id)
        });
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
        var dir = data[type];
        var list = [];
        for (var id in dir) {
            list.push(create(type, createKey(type, id)));
        }
        return list;
    }

    function remove(key) {
        if (!isKey(key)) {
            throw new Error("Invalid key object: " + key);
        }
        var [type, id] = key.$ref.split(":");
        var redisKey = makeRedisKey(type, id);

        redis.del(redisKey);
    }

    function makeRedisKey(type, id) {
        return type + ":" + id + ":json";
    }

    function generateId(type) {
        return redis.incr(type+":id")
    }
}

function dump() {
    datastore.dump();
}
