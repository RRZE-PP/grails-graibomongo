
var BSON = bson().BSON;
// var NumberLong = bson().Long;
var ObjectId = bson().ObjectID;
var BinData = bson().Binary;
var MinKey = bson().MinKey;
var MaxKey = bson().MaxKey;
var Timestamp = bson().Timestamp;

var NumberInt=function(){};

Object.bsonsize = function(){
	BSON.calculateObjectSize.apply(this, arguments);
}

function NumberLong(){
	if(arguments.length == 1 && typeof arguments[0] === "string")
		return bson().Long.fromString(arguments[0]);
	else
		bson().Long.apply(this, arguments);
}
NumberLong.prototype = bson().Long.prototype;


function jsObjectToJSObjectWithBsonValues(object){
	if(typeof object !== "object" || object === null)
		return object;

	function constructFromExtendedJSON(obj){
		if(typeof obj !== "object" || obj === null)
			return obj;

		switch(Object.keys(obj)[0]){
			case "$oid": return ObjectId(obj["$oid"]);
			case "$numberLong": return NumberLong(obj["$numberLong"]);
			case "$maxKey": return new MaxKey();
			case "$minKey": return new MinKey();
			case "$regex": return new RegExp(obj["$regex"], obj["$options"]);
			case "$undefined": return undefined;
			case "$date": return new Date(obj["$date"])
			default: return obj;
		}
	}

	function isBSONObject(obj){
		if(typeof obj !== "object" || obj === null)
			return false;

		switch(Object.keys(obj)[0]){
			case "$oid":
			case "$numberLong":
			case "$maxKey":
			case "$minKey":
			case "$date":
			case "$undefined":
			case "$regex": return true;
			case "$binary":
			case "$timestamp":
			case "$ref": throw new Error("This datatype is not yet supported: " + Object.keys(obj)[0]);
			default: return false;
		}
	}

	var keys = Object.keys(object);
	for(var i = 0; i < keys.length; i++){
		var key = keys[i];
		if(isBSONObject(object[key]))
			object[key] = constructFromExtendedJSON(object[key]);
		else
			object[key] = jsObjectToJSObjectWithBsonValues(object[key]);
	}

	return object;
}


//To port:
//     Binary
// x   Date
// x   Timestamp
//   x Regular_expression
// x x OID
//     DB Reference
//     Undefined
// x x MinKey
// x x MaxKey
// x x NumberLong
// TODO: Copy RegExp into Namespace, because we change its prototype, which should not be visible outside of namespace
// caution: we have to handle user inputted regexps then!


/**
 * Output in MongoDB Extended JSON Strict mode
 */
ObjectId.prototype.toJSON = function() {
	return { "$oid": this.toHexString() };
}

NumberLong.prototype.toJSON = function() {
	return { "$numberLong": this.toString()};
}

MaxKey.prototype.toJSON = function() {
	return { "$maxKey": 1 };
}

MinKey.prototype.toJSON = function() {
	return { "$minKey": 1 };
}

RegExp.prototype.toJSON = function(){
	return { "$regex": this.source, "$options": this.flags };

}