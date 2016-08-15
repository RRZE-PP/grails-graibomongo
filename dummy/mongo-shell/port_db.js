function DB(mongo, name){
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