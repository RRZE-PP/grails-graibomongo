Mongo.prototype.init = function(host) {
	console.log("test")
};

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