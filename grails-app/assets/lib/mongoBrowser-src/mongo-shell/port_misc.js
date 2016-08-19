
function print(){
	if(typeof MongoNS !== "undefined" && typeof MongoNS.__namespacedPrint !== "undefined"){
		MongoNS.__namespacedPrint.apply(this, arguments);
		return;
	}
	console.log.apply(console, arguments);
}

function version(){
	return "42.1337";
}

/**
 * This function executes a given query. Does some very basic break-out
 * prevention. The idea is to wrap the whole Mongo-stuff in it's own namespace
 * and use this function to execute code, which needs direct access to the
 * methods in here.
 *
 * @param {object} context - within the code 'this' will point to context
 * @param {DB} db - within the code 'db' will point to this parameter
 * @param {string} code - the code to execute
 * @return {object} the result of the code execution
 */
function execute(context, db, code) {

	function evaluate(db, code, window, document){
		return eval(code);
	}

	return evaluate.call(context, db, code);
}