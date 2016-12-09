DB.prototype.constructor = function DB(mongo, name){
	this._mongo = mongo;
	this._name = name;

	var collectionNames = this.getCollectionNames();

	//TODO: use a Proxy to simulate this, as soon as its widely available, because this is
	//hellishly ugly. toJSON can be removed then, too.
	for(var i=0; i < collectionNames.length; i++){
		this[collectionNames[i]] = this.getCollection(collectionNames[i]);
	}
}


DB.prototype.toJSON = function(){
	var keys = Object.keys(this);
	var tmp = {};
	for(var i = 0; i < keys.length; i++){
		var key = keys[i];
		if(!(this[key] instanceof DBCollection)){
			tmp[key] = this[key];
		}
	}

	return JSON.stringify(tmp);
}


/* Overwrite this function to support older versions of mongodb wrapped */
DB.prototype._getCollectionInfosCommand = function(filter) {
    filter = filter || {};
    try{
	    var res = this.runCommand({listCollections: 1, filter: filter});
	    if (res.code == 59) {
	        // command doesn't exist, old mongod
	        return null;
	    }

	    if (!res.ok) {
	        if (res.errmsg && res.errmsg.startsWith("no such cmd")) {
	            return null;
	        }

	        throw _getErrorWithCode(res, "listCollections failed: " + tojson(res));
	    }

	    return new DBCommandCursor(this._mongo, res).toArray().sort(compareOn("name"));
	}catch(e){
		if(e.message.indexOf("no such cmd: listCollections") !== -1)
			return null;
		// console.log(e.stack); // Stack gets lost on chrome on rethrow, so comment this in to investigate the stack
		throw e;
	}
};