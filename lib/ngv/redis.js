export('connect', 'decode', 'DefaultCodec');

var {DefaultCodec} = org.jredis.ri.alphazero.support;


//Commands supported by Redis (as of June, 2009).
//Note: 'sort' and 'quit' are handled as special cases.

var inlineCommands = {
auth:1,        get:1,         mget:1,        incr:1,        incrby:1,
decr:1,        decrby:1,      exists:1,      del:1,         type:1,
keys:1,        randomkey:1,   rename:1,      renamenx:1,    dbsize:1,
expire:1,      ttl:1,         llen:1,        lrange:1,      ltrim:1,
lindex:1,      lpop:1,        rpop:1,        scard:1,       sinter:1,
sinterstore:1, sunion:1,      sunionstore:1, smembers:1,    select:1,
move:1,        flushdb:1,     flushall:1,    save:1,        bgsave:1,
lastsave:1,    shutdown:1,    info:1,        ping:1
};

var bulkCommands = {
set:1,         getset:1,      setnx:1,       rpush:1,       lpush:1,
lset:1,        lrem:1,        sadd:1,        srem:1,        smove:1,
sismember:1
};


function connect() {
	return new Packages.org.jredis.ri.alphazero.JRedisClient();
}


function decode(l) {
	var result;
	if (l) {
		result = [];
		for (var iterator = l.iterator(); iterator.hasNext();) {
			result.push(DefaultCodec.toStr(iterator.next()));
		}
	}
	return result;
}

function close() {

}

//Creates a function to send a command to the redis server.

function createCommandSender(commandName) {
  return function() {

    var commandArgs = arguments;

    if (conn.readyState != "open") {
      debug('Connection is not open (' + conn.readyState + ')');
      conn.close();
      _connect(function() {
        exports[commandName].apply(exports, commandArgs);
      });
      return;
    }

    // last arg (if any) should be callback function.

    var callback = null;
    var numArgs = commandArgs.length;

    if (typeof(commandArgs[commandArgs.length - 1]) == 'function') {
      callback = commandArgs[commandArgs.length - 1];
      numArgs = commandArgs.length - 1;
    }

    // Format the command and send it.

    var cmd;

    if (inlineCommands[commandName]) {
      cmd = formatInline(commandName, commandArgs, numArgs);
    } else if (bulkCommands[commandName]) {
      cmd = formatBulk(commandName, commandArgs, numArgs);
    } else {
      fatal('unknown command ' + commandName);
    }

    debug('> ' + cmd);

    // Always push something, even if its null.
    // We need received replies to match number of entries in `callbacks`.

    callbacks.push({ cb:callback, cmd:commandName.toLowerCase() });
    conn.send(cmd);
  };
}

// Create command senders for all commands.

for (var commandName in inlineCommands)
  exports[commandName] = createCommandSender(commandName);

for (var bulkCommand in bulkCommands)
  exports[bulkCommand] = createCommandSender(bulkCommand);
