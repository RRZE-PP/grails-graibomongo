Array.prototype.empty = function(){
	return this.length === 0;
}

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

var QueryOption_CursorTailable = 1 << 1;

Cursor = function(ns, cursorId, nToReturn, queryOptions){
	this._client = null;
	this._originalHost = null; //(_client->getServerAddress());
	this.ns = ns;
	this._isCommand = (nsIsFull(ns) ? nsToCollectionSubstring(ns) === "$cmd" : false);
	this.query = {}; // BSONObj();
	this.nToReturn = nToReturn;
	this.haveLimit = (nToReturn > 0 && !(queryOptions & QueryOption_CursorTailable));
	this.nToSkip = 0;
	this.fieldsToReturn = null;
	this.opts = queryOptions;
	this.batchSize = 0;
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
		message: null //TODO
	};
}


Cursor.prototype.close = function(){
	throw Error("Not implemented yet");
}

Cursor.prototype.hasNext = function(){
	return this.dbcc_more();
}

Cursor.prototype.next = function(){
    var bson = this.dbcc_next();
    var ro = this._ro;

    // getOwned because cursor->next() gives us unowned bson from an internal
    // buffer and we need to make a copy
    //ValueReader(cx, args.rval()).fromBSON(bson.getOwned(), nullptr, ro);

    //TODO: Is this correct?
    return bson;
}

Cursor.prototype.objsLeftInBatch = function(){
    return _putBack.length + batch.nReturned - batch.pos;
}

Cursor.prototype.readOnly = function(){
	this._ro = true;

	return this;
}

Cursor.prototype.setBatchSize = function(newBatchSize){
	this.batchSize = newBatchSize;
}


/**
 * Methods of DBClientCursor for convenience also in Cursor
 */
Cursor.prototype.dbcc_more = function (){
    if (!this._putBack.empty())
        return true;

    if (this.haveLimit && this.batch.pos >= this.nToReturn)
        return false;

    if (this.batch.pos < this.batch.nReturned)
        return true;

    if (this.cursorId === 0)
        return false;

    this.requestMore();
    return this.batch.pos < this.batch.nReturned;
}

Cursor.prototype.requestMore = function(){
	console.warn("WARNING: Request more is not implemented yet but cannot throw because it's (unnecessarily?) used!")
	return;

	throw Error("Not completely implemented yet");

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

    assert(verify(cursorId && batch.pos == batch.nReturned));

    if (this.haveLimit) {
        this.nToReturn -= this.batch.nReturned;
        assert(this.nToReturn > 0);
    }
}


Cursor.prototype.dbcc_next = function(){
    if (!this._putBack.empty()) {
        return this._putBack.pop();
    }

    assert(this.batch.pos < this.batch.nReturned, "DBClientCursor next() called but more() is false");

	throw Error("Not completely implemented yet");

    this.batch.pos++;

    // BSONObj o(batch.data);
    // batch.data += o.objsize();
    //  todo would be good to make data null at end of batch for safety
    // return o;
}