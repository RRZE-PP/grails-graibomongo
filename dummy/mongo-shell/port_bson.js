
var BSON = bson().BSON;
var NumberLong = bson().Long;
var ObjectId = bson().ObjectID;
var BinData = bson().Binary;
var MinKey = bson().MinKey;
var MaxKey = bson().MaxKey;
var Timestamp = bson().Timestamp;

var NumberInt=function(){};

Object.bsonsize = function(){
	BSON.calculateObjectSize.apply(this, arguments);
}
