/*
 * Within the CPP-Part of the mongo-shell there is a connection which is basicly the
 * socket to the database (what we call database-connection). This module is trying to mimic its functionality
 * but on a server-connection level.
 *
 * One huge problem: runCommand in db.js and hasNext() in port_cursor (which is used in non-ported code)
 * due to it's usage of requestMore is not designed to be used in an asynchronous
 * context. Therefore we issue synchronous ajax (UGH!) calls here until we figure out a way to
 * deal with this reasonably.
 * The best solution would probably be to wrap the whole mess in a WebWorker
 *
 * This Module only provides connections to an AJAX backend. If you want another kind of
 * backend (e.g. use the mongodb REST-API), consider overwriting the Connection Namespace
 */

function ServerConnectionError(message) {
	this.name = 'ServerConnectionError';
	this.message = message || "An error occured on the HTTP-connection level. That's probably an issue with the backend-server (not the mongodb-server!)";
	this.stack = (new Error()).stack;
}
ServerConnectionError.prototype = Object.create(Error.prototype);
ServerConnectionError.prototype.constructor = ServerConnectionError;

function DatabaseConnectionError(message) {
	this.name = 'DatabaseConnectionError';
	this.message = message || "An error occured on the database-connection level. That's probably an issue with the mongodb-server (not the backend-server!)";
	this.stack = (new Error()).stack;
}
DatabaseConnectionError.prototype = Object.create(Error.prototype);
DatabaseConnectionError.prototype.constructor = DatabaseConnectionError;



var Connection = (function(){
	var backendURLs = {
		initCursor:  "/shell/initCursor",
		requestMore: "/shell/requestMore",
		runCommand:  "/shell/runCommand"
	}

	function handleConnectionFails(jqXHR){
		if(typeof jqXHR.responseJSON !== "undefined" && typeof jqXHR.responseJSON.error !== "undefined")
			//We have a connection and the backend server issued a valid JSON response => It seems to be running
			throw new DatabaseConnectionError(jqXHR.responseJSON.error);

		//we did not receive a valid JSON response => backend server broken?
		throw new ServerConnectionError("An Error occured connecting to the backend: " + jqXHR.statusText);
	}

	/**
	 * Initialises a cursor on the backend. Must be overridden, when replacing this namespace!
	 *
	 * @param {object} data - the data to send to the backend. These have been created by
	 *                        DBClientCursor.prototype.assembleQueryRequest
	 * @param {function} completionCallback - the function to call upon completion. It is passed
	 *                                        an object as used for qr in DBClientCursor.prototype.dataReceived
	 * @param {DBClientCursor} callee - the callee to allow switching between server-connections based on database-connection
	 */
	function initCursor(data, completionCallback, callee){
		$.ajax(backendURLs.initCursor, {
				data: JSON.stringify(data),
				async: false,
				method: "POST",
				contentType: "application/json; charset=utf-8"
			})
			.done(completionCallback)
			.fail(handleConnectionFails);
	}

	/**
	 * Requests more from a cursor on the backend. Must be overridden, when replacing this namespace!
	 *
	 * @param {object} data - the data to send to the backend. These have been created by
	 *                       DBClientCursor.prototype.requestMore
	 * @param {function} completionCallback - the function to call upon completion. It is passed
	 *                                        an object as used for qr in DBClientCursor.prototype.dataReceived
	 * @param {DBClientCursor} callee - the callee to allow switching between server-connections based on database-connection
	 */
	function requestMore(data, completionCallback, callee){
		$.ajax(backendURLs.requestMore, {
				data: JSON.stringify(data),
				async: false,
				method: "POST",
				contentType: "application/json; charset=utf-8"
			})
			.done(completionCallback)
			.fail(handleConnectionFails);
	}



	/**
	 * Requests more from a cursor on the backend. Must be overridden, when replacing this namespace!
	 *
	 * @param {object} data - the data to send to the backend. These have been created by
	 *                       DBClientCursor.prototype.requestMore
	 * @param {function} [completionCallback] - the function to call upon completion. It is passed
	 *                                          the result of this operation. PLEASE NOTE: This parameter is optional ONLY
	 *                                          IN THIS FUNCTION. If it is left out this function will make a synchronous
	 *                                          AJAX call and return the result of the operation instead of passing it to the
	 *                                          callback
	 * @param {DBClientCursor} callee - the callee to allow switching between server-connections based on database-connection
	 */
	function runCommand(data, completionCallback, callee){
		var async = true;
		var result = null;
		if(typeof callee === "undefined"){
			callee = completionCallback;
			completionCallback = function(data){
				result = data;
			}
			async = false;
		}

		$.ajax(backendURLs.runCommand, {
			async: async,
			data: JSON.stringify(data),
            method: "POST",
            contentType: "application/json; charset=utf-8"
		})
		.done(completionCallback)
		.fail(handleConnectionFails);

		if(!async){
			return result;
		}
	}

	/**
	 * Initiate the URLs to use to connect to the backend. If none are set,
	 * the Connection will try to connect to http://localhost:8080/shell/<functionName>
	 *
	 * @param {string} initCursorURL - the URL to send initCursor-requests to
	 * @param {string} requestMoreURL - the URL to send requestMore-requests to
	 * @param {string} runCommandURL - the URL to send runCommand-requests to
	 */
	function initServerConnection(initCursorURL, requestMoreURL, runCommandURL){
		backendURLs.initCursor = initCursorURL;
		backendURLs.requestMore = requestMoreURL;
		backendURLs.runCommand = runCommandURL;
	}

	return {
			initCursor: initCursor,
			requestMore: requestMore,
			runCommand: runCommand,
			initServerConnection: initServerConnection
		}
 })();