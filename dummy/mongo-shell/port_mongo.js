Mongo.prototype.init = function(host) {
	console.log("test")

	//this._writeMode = "compatability"; //Disable sending update, insert and remove via command
};

Mongo.prototype.getMinWireVersion = function(){
	return 0;
}

Mongo.prototype.getMaxWireVersion = function(){
	return 10;
}

Mongo.prototype.runCommand = function(database, cmdObj, options){
	print("====RUNCOMMAND")
	assert(typeof database === "string", "the database parameter to runCommand must be a string");
	assert(typeof cmdObj === "object", "the cmdObj parameter to runCommand must be an object");
	assert(typeof options === "number", "the options parameter to runCommand must be a number");

	var result = null;
	$.ajax("http://localhost:8080/shell/runCommand", {
			async: false,
			data: {database: database, command: JSON.stringify(cmdObj), options: options}
		})
		.done(function(data){
			result = data;
		});
	return result;

	print("Normally would send to server. For development returning a listCollections result");
	return {
		"cursor" : {
			"id" : NumberLong(0),
			"ns" : "test.$cmd.listCollections",
			"firstBatch" : [
			{
				"name" : "asdfasdf",
				"options" : {

				}
			},
			{
				"name" : "foobar",
				"options" : {

				}
			},
			{
				"name" : "restaurants",
				"options" : {

				}
			},
			{
				"name" : "system.indexes",
				"options" : {

				}
			}
			]
		},
		"ok" : 1
	};
}

Mongo.prototype.cursorFromId = function(ns, cursorId, batchSize){
	assert(arguments.length == 2 || arguments.length == 3, "cursorFromId needs 2 or 3 args");
	assert(typeof arguments[0] === "string", "ns must be a string");
	assert(arguments[1] instanceof NumberLong || typeof arguments[1] === "number", "2nd arg must be a NumberLong");
	assert(typeof arguments[2] === "number" || typeof arguments[2] === "undefined", "3rd arg must be a js Number");

	if(typeof cursorId === "number")
		cursorId = NumberLong(cursorId);
    	//TODO: Javascript Integer gehen nur bis 2^53-1 => eigentlich muessen wir ueberall BSON Longs verwenden
    	//(cursorID kann schon kaputt sein!)

	var cursor = new Cursor(ns, cursorId, 0, 0);

	if(typeof batchSize !== "undefined")
		cursor.setBatchSize(batchSize);

	return cursor;
}

Mongo.prototype.find = function(ns, query, fields, nToReturn, nToSkip, batchSize, options) {
   	assert(arguments.length === 7, "find needs 7 args");
   	assert(typeof arguments[1] === "object", "needs to be an object");

	var cursor = new Cursor(ns, query, nToReturn, nToSkip, fields, options, batchSize);
	//init is normally called from the connection, which we don't have
	cursor.init();
	return cursor;
};

Mongo.prototype.insert = function(ns, obj) {
	console.log(arguments);
    throw Error("insert not implemented");
};

Mongo.prototype.remove = function(ns, pattern) {
	console.log(arguments);
    throw Error("remove not implemented");
};

Mongo.prototype.update = function(ns, query, obj, upsert) {
	console.log(arguments);
	throw Error("non-command mode not implemented on server-side");

	// below code should work, though:
    // assert(arguments.length >= 3, "update needs at least 3 args");
    // assert(typeof arguments[1] === "object", "1st param to update has to be an object");
    // assert(typeof arguments[2] === "object", "2nd param to update has to be an object");

    // assert(this.readOnly !== true, "js db in read only mode");

    // //normally data is wrapped in a bson object and directly sent over the connection,
    // //we send to an AJAX endpoint

    // var toSend = {
    // 	ns: ns,
    // 	query: JSON.stringify(query),
    // 	obj: JSON.stringify(obj),
    // 	upsert: arguments.length > 3 && arguments[3] === true,
    // 	multi: arguments.length > 4 && arguments[4] === true
    // }

    // foobar = toSend;

    // $.ajax("http://localhost:8080/shell/update", {
    //             async: false,
    //             data: toSend
    //         })
    //         .fail(function(jqXHR, textStatus, errorThrown){
    //             throw Error("update failed, due to an AJAX error: " + textStatus);
    //         });

};