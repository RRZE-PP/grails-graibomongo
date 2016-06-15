Array.prototype.empty = function(){
	return this.length === 0;
}

var inheritsFrom = function (child, parent) {
    child.prototype = Object.create(parent.prototype);
};


/* in the mongo source code, "client" means "database". */
var MaxDatabaseNameLen = 128 - 1;  // 128 = max str len for the db name, including null char

/**
 * foo = true
 * foo. = false
 * foo.a = false
 */
function nsIsFull(ns){
	var tmp = ns.split(".");
	return tmp.length > 1 && tmp[1] !== "";
}

// "database.a.b.c" -> "a.b.c"
function nsToCollectionSubstring(ns){
	return ns.split(".").slice(1).join(".");
}

// "database.a.b.c" -> "database"
function nsToDatabaseSubstring(ns) {
    var tmp = ns.split(".");
    if(tmp.length == 1){
        assert(ns.length < MaxDatabaseNameLen, "nsToDatabase: db too long");
        return ns;
    }
    assert(tmp[0].length < MaxDatabaseNameLen, "nsToDatabase: db too long");

    return tmp[0];
}

var ResultFlagType = {
    /* returned, with zero results, when getMore is called but the cursor id
       is not valid at the server. */
    ResultFlag_CursorNotFound: 1,

    /* { $err : ... } is being returned */
    ResultFlag_ErrSet: 2,

    /* Have to update config from the server, usually $err is also set */
    ResultFlag_ShardConfigStale: 4,

    /* for backward compatibility: this let's us know the server supports
       the QueryOption_AwaitData option. if it doesn't, a repl slave client should sleep
    a little between getMore's.
    */
    ResultFlag_AwaitCapable: 8
};

/** the query field 'options' can have these bits set: */
var QueryOptions = {
    /** Tailable means cursor is not closed when the last data is retrieved.  rather, the cursor
     * marks the final object's position.  you can resume using the cursor later, from where it was
       located, if more data were received.  Set on dbQuery and dbGetMore.

       like any "latent cursor", the cursor may become invalid at some point -- for example if that
       final object it references were deleted.  Thus, you should be prepared to requery if you get
       back ResultFlag_CursorNotFound.
    */
    QueryOption_CursorTailable: 1 << 1,

    /** allow query of replica slave.  normally these return an error except for namespace "local".
    */
    QueryOption_SlaveOk: 1 << 2,

    // findingStart mode is used to find the first operation of interest when
    // we are scanning through a repl log.  For efficiency in the common case,
    // where the first operation of interest is closer to the tail than the head,
    // we start from the tail of the log and work backwards until we find the
    // first operation of interest.  Then we scan forward from that first operation,
    // actually returning results to the client.  During the findingStart phase,
    // we release the db mutex occasionally to avoid blocking the db process for
    // an extended period of time.
    QueryOption_OplogReplay: 1 << 3,

    /** The server normally times out idle cursors after an inactivity period to prevent excess
     * memory uses
        Set this option to prevent that.
    */
    QueryOption_NoCursorTimeout: 1 << 4,

    /** Use with QueryOption_CursorTailable.  If we are at the end of the data, block for a while
     * rather than returning no data. After a timeout period, we do return as normal.
    */
    QueryOption_AwaitData: 1 << 5,

    /** Stream the data down full blast in multiple "more" packages, on the assumption that the
     * client will fully read all data queried.  Faster when you are pulling a lot of data and know
     * you want to pull it all down.  Note: it is not allowed to not read all the data unless you
     * close the connection.

        Use the query( stdx::function<void(const BSONObj&)> f, ... ) version of the connection's
        query()
        method, and it will take care of all the details for you.
    */
    QueryOption_Exhaust: 1 << 6,

    /** When sharded, this means its ok to return partial results
        Usually we will fail a query if all required shards aren't up
        If this is set, it'll be a partial result set
     */
    QueryOption_PartialResults: 1 << 7
};
QueryOptions.QueryOption_AllSupported = QueryOptions.QueryOption_CursorTailable | QueryOptions.QueryOption_SlaveOk |
    QueryOptions.QueryOption_OplogReplay | QueryOptions.QueryOption_NoCursorTimeout | QueryOptions.QueryOption_AwaitData |
    QueryOptions.QueryOption_Exhaust | QueryOptions.QueryOption_PartialResults;

QueryOptions.QueryOption_AllSupportedForSharding = QueryOptions.QueryOption_CursorTailable | QueryOptions.QueryOption_SlaveOk |
    QueryOptions.QueryOption_OplogReplay | QueryOptions.QueryOption_NoCursorTimeout | QueryOptions.QueryOption_AwaitData |
    QueryOptions.QueryOption_PartialResults;

/**
 * DBClientCursor is an implementation of a cursor which is heavily oriented at
 * the corresponding C++ class. However, instead of using a connection to the
 * mongodb we connect to an AJAX endpoint.
 */

DBClientCursor = function(///*DBClientBase* */ client,
                   /*const std::string&*/ ns,
                   /*const BSONObj&*/ query,
                   /*long long*/ cursorId,
                   /*int*/ nToReturn,
                   /*int*/ nToSkip,
                   /*const BSONObj* */ fieldsToReturn,
                   /*int*/ queryOptions,
                   /*int*/ batchSize,
                   connection)//this is our "client": the information for the server which mongodb to connect to when forwarding
{
    this._client = null;
    this._originalHost = null; //(_client->getServerAddress());
    this.ns = ns;
    this._isCommand = (nsIsFull(ns) ? nsToCollectionSubstring(ns) === "$cmd" : false);
    this.query = query,
    this.nToReturn = nToReturn;
    this.haveLimit = (nToReturn > 0 && !(queryOptions & QueryOptions.QueryOption_CursorTailable));
    this.nToSkip = nToSkip;
    this.fieldsToReturn = fieldsToReturn;
    this.opts = queryOptions;
    this.batchSize = batchSize == 1 ? 2 : batchSize;
    this.resultFlags = 0;
    this.cursorId = cursorId;
    this._ownCursor = true;
    this.wasError = false;

    this._putBack = [];
    this._ro = false;
    this.batch = {
        nReturned: 0,
        pos:0,
        data: null, //TODO
        m: null //message //TODO
    };

    this.connection = connection;
}


DBClientCursor.prototype.assembleCommandRequest = function(/*BClientWithCommands* */ cli,
                               /*StringData*/ database,
                               /*int*/ legacyQueryOptions,
                               /*BSONObj*/ legacyQuery) {

    window.foobar = arguments;

    var upconvertedCommand = {};
    var upconvertedMetadata = {};

    var tuple = rpc.upconvertRequestMetadata(legacyQuery, legacyQueryOptions);
    upconvertedCommand = tuple[0];
    upconvertedMetadata = tuple[1];

    return {
        database: database,
        commandName: Object.keys(upconvertedCommand)[0],
        commandArgs: upconvertedCommand, //this is stringified later in runCommand
        metadata: upconvertedMetadata
    }
}

DBClientCursor.prototype.assembleQueryRequest = function(/*const string& */ns,
                          /*BSONObj*/ query,
                          /*int*/ nToReturn,
                          /*int*/ nToSkip,
                          /*const BSONObj* */ fieldsToReturn,
                          /*int*/ queryOptions,
                          /*Message&*/ toSend) {
    return {
        opts: queryOptions,
        ns: ns,
        nToSkip: nToSkip,
        nToReturn: nToReturn,
        query: JSON.stringify(query),
        fieldsToReturn: JSON.stringify(fieldsToReturn)
    }
}


DBClientCursor.prototype._assembleInit = function() {
    // If we haven't gotten a cursorId yet, we need to issue a new query or command.
    if (!this.cursorId) {
        // HACK:
        // Unfortunately, this code is used by the shell to run commands,
        // so we need to allow the shell to send invalid options so that we can
        // test that the server rejects them. Thus, to allow generating commands with
        // invalid options, we validate them here, and fall back to generating an OP_QUERY
        // through assembleQueryRequest if the options are invalid.

        var hasValidNToReturnForCommand = (this.nToReturn === 1 || this.nToReturn === -1);
        var hasValidFlagsForCommand = !(this.opts & QueryOptions.QueryOption_Exhaust);

        if (this._isCommand && hasValidNToReturnForCommand && hasValidFlagsForCommand) {
            return this.assembleCommandRequest(this._client, nsToDatabaseSubstring(this.ns), this.opts, this.query);
        }
        return this.assembleQueryRequest(this.ns, this.query, this.nextBatchSize(), this.nToSkip, this.fieldsToReturn, this.opts, this.toSend);
    }
    // Assemble a legacy getMore request.
    //TODO: Das muesste dann eigentlich an den Get-More-Endpoint gehen :(
    return {
        opts: this.opts,
        ns: this.ns,
        nToReturn: this.nToReturn,
        cursorId: this.cursorId
    }
}

DBClientCursor.prototype.dataReceived = function(/*bool&*/ retry, /*string&*/ host) {
    // If this is a reply to our initial command request.
    if (this._isCommand && this.cursorId == 0) {
        this.commandDataReceived();
        return;
    }

    var qr = this.batch.m;
    this.resultFlags = qr.resultFlags;

    if (qr.resultFlags & ResultFlagType.ResultFlag_ErrSet) {
        wasError = true;
    }

    if (qr.resultFlags & ResultFlagType.ResultFlag_CursorNotFound) {
        // cursor id no longer valid at the server.

        if (!(this.opts & QueryOptions.QueryOption_CursorTailable)) {
            throw Error( "cursor id " + this.cursorId + " didn't exist on server.");
        }

        // 0 indicates no longer valid (dead)
        this.cursorId = 0;
    }

    if (this.cursorId == 0 || !(this.opts & QueryOptions.QueryOption_CursorTailable)) {
        // only set initially: we don't want to kill it on end of data
        // if it's a tailable cursor
        this.cursorId = qr.cursorId;
    }

    this.batch.nReturned = qr.nReturned;
    this.batch.pos = 0;
    this.batch.data = qr.data;

    //TODO: figure out, what this does and if we need it
    //_client->checkResponse(batch.data, batch.nReturned, &retry, &host);  // watches for "not master"

    if (qr.resultFlags & ResultFlagType.ResultFlag_ShardConfigStale) {
        throw Error("Some Error occured, whose handling has not yet been implemented");
        // BSONObj error;
        // verify(peekError(&error));
        // throw RecvStaleConfigException(
        //     (string) "stale config on lazy receive" + causedBy(getErrField(error)), error);
    }
}

DBClientCursor.prototype.commandDataReceived = function() {
    // var op = this.batch.m.operation();
    // assert(op == opReply || op == dbCommandReply);

    this.batch.nReturned = 1;
    this.batch.pos = 0;
    window.foobar = this.batch

    var commandReply = this.batch.m;

    if(commandReply.ok == ErrorCodes.SendStaleConfig){
        throw new Error("stale config in DBClientCursor::dataReceived()" + JSON.stringify(commandReply))
    }

    if(!commandReply.ok){
        this.wasError = true;
    }

    this.batch.data = [this.batch.m]
}

DBClientCursor.prototype.init = function(){
    var toSend = this._assembleInit();

    if(this._isCommand){
        //hack to reuse the code from runCommand
        this.batch.m = Mongo.prototype.runCommand.call(
                            { connectionData: this.connection, getConnectionData: Mongo.prototype.getConnectionData },
                            nsToDatabaseSubstring(this.ns), toSend.commandArgs, this.opts);
        this.dataReceived();
        return;
    }

    toSend.connection = this.connection;

    var self = this;
    //TODO: Refactor this into port_client.js
    $.ajax("/shell/initCursor", {
                async: false,
                data: JSON.stringify(toSend),
                method: "POST",
                contentType: "application/json; charset=utf-8"
            })
            .done(function(data){
                self.batch.m = data;
                if(typeof data !== "object" || data === {}){
                    throw Error("DBClientCursor::init message from call() was empty or invalid");
                }
                self.dataReceived();
            })
            .fail(function(){
                throw Error("DBClientCursor::init failed");
            });

}

DBClientCursor.prototype.more = function (){
    if (!this._putBack.empty())
        return true;

    if (this.haveLimit && this.batch.pos >= this.nToReturn)
        return false;

    if (this.batch.pos < this.batch.nReturned)
        return true;

    if (this.cursorId === 0 || (this.cursorId instanceof NumberLong && this.cursorId.isZero()))
        return false;

    this.requestMore();

    return this.batch.pos < this.batch.nReturned;
}

DBClientCursor.prototype.next = function(){
    if (!this._putBack.empty()) {
        return this._putBack.pop();
    }

    assert(this.batch.pos < this.batch.nReturned, "DBClientCursor next() called but more() is false");

    // throw Error("Not completely implemented yet");

    return this.batch.data[this.batch.pos++];

    // BSONObj o(batch.data);
    // batch.data += o.objsize();
    //  todo would be good to make data null at end of batch for safety
    // return o;
}


DBClientCursor.prototype.objsLeftInBatch = function(){
    return _putBack.length + batch.nReturned - batch.pos;
}

DBClientCursor.prototype.setBatchSize = function(newBatchSize){
    this.batchSize = newBatchSize;
}

DBClientCursor.prototype.requestMore = function(){
    // console.warn("WARNING: Request more is not implemented yet but cannot throw because it's (unnecessarily?) used!")
    // return;


    /*
    BufBuilder b;
    b.appendNum(opts);
    b.appendStr(ns);
    b.appendNum(nextBatchSize());
    b.appendNum(cursorId);

    Message toSend;
    toSend.setData(dbGetMore, b.buf(), b.len());
    Message response;

    if (_client) {
        _client->call(toSend, response);
        this->batch.m = std::move(response);
        dataReceived();
    } else {
        verify(_scopedHost.size());
        ScopedDbConnection conn(_scopedHost);
        conn->call(toSend, response);
        _client = conn.get();
        ON_BLOCK_EXIT([this] { _client = nullptr; });
        this->batch.m = std::move(response);
        dataReceived();
        conn.done();
    }
    */

    assert(this.cursorId && this.batch.pos == this.batch.nReturned);

    if (this.haveLimit) {
        this.nToReturn -= this.batch.nReturned;
        assert(this.nToReturn > 0);
    }

    var toSend = {
        opts: this.opts,
        ns: this.ns,
        nToReturn: this.nextBatchSize(),
        cursorId: this.cursorId
    }

    toSend.connection = this.connection;

    var self = this;
    $.ajax("/shell/requestMore", {
                async: false,
                data: JSON.stringify(toSend),
                method: "POST",
                contentType: "application/json; charset=utf-8"
            })
            .done(function(data){
                self.batch.m = data;
                if(typeof data !== "object" || data === {}){
                    throw Error("DBClientCursor::init message from call() was empty or invalid");
                }
                self.dataReceived();
            })
            .fail(function(){
                throw Error("DBClientCursor::init failed");
            });
}


DBClientCursor.prototype.nextBatchSize = function() {
    if (this.nToReturn == 0)
        return this.batchSize;

    if (this.batchSize == 0)
        return this.nToReturn;

    return this.batchSize < this.nToReturn ? this.batchSize : this.nToReturn;
}



/**
 * Cursor is our port of the wrapper around DBClientCursor (mozjs/cursor.h)
 */
Cursor = function(){
    if(arguments.length == 5)
        return Cursor.fiveArgsConstructor.apply(this, arguments);
    if(arguments.length == 8)
        return Cursor.eightArgsConstructor.apply(this, arguments);
    return DBClientCursor.apply(this, arguments);
}

Cursor.eightArgsConstructor = function(ns, query, nToReturn, nToSkip, fieldsToReturn, queryOptions, batchSize, connection){
    return DBClientCursor.call(this, ns, query, 0 /*cursorId*/, nToReturn, nToSkip, fieldsToReturn, queryOptions, batchSize, connection);
}

Cursor.fiveArgsConstructor = function(ns, cursorId, nToReturn, queryOptions, connection){
    return DBClientCursor.call(this, ns, {} /*query*/, cursorId, nToReturn, 0 /*nToSkip*/, null /*fieldsToReturn*/, queryOptions, 0 /*batchSize*/, connection);
}

inheritsFrom(Cursor, DBClientCursor);

Cursor.prototype.close = function(){
    throw Error("Not implemented yet");
}

Cursor.prototype.readOnly = function(){
    this._ro = true;

    return this;
}

Cursor.prototype.hasNext = DBClientCursor.prototype.more;
