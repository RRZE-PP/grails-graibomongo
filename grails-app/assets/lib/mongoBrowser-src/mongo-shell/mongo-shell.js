var MongoNS = (function(){
	"use strict";

	// set these variables to undefined
	// thus they are not defined on the global NS, when assigned later without var
	var doassert,
		assert,
		sortDoc,
		aliases,
		DBClientCursor,
		Cursor,
		DBCollection,
		MapReduceResult,
		PlanCache,
		module,
		WriteConcern,
		WriteResult,
		BulkWriteResult,
		BulkWriteError,
		WriteCommand,
		WriteCommandError,
		getActiveCommands,
		Mongo,
		connect,
		MR,
		DBQuery,
		QueryPlan,
		ToolTest,
		ReplTest,
		allocatePort,
		allocatePorts,
		ISODate,
		gc,
		tojsononeline,
		tojson,
		tojsonObject,
		printjson,
		printjsononeline,
		isString,
		isNumber,
		isObject,
		__quiet,
		__magicNoPrint,
		__callLastError,
		_verboseShell,
		chatty,
		friendlyEqual,
		printStackTrace,
		setVerboseShell,
		_barFormat,
		compare,
		compareOn,
		shellPrint,
		TestData,
		jsTestName,
		jsTestOptions,
		setJsTestOption,
		jsTestLog,
		jsTest,
		defaultPrompt,
		replSetMemberStatePrompt,
		isMasterStatePrompt,
		_useWriteCommandsDefault,
		_writeMode,
		_readMode,
		shellPrintHelper,
		shellAutocomplete,
		shellHelper,
		Geo,
		rs,
		_awaitRSHostViaRSMonitor,
		help,
		sh,
		__autocomplete__;

	var typedResult; //this is a very hacky one: someone forgot var in bul_api.js:1211, but we don't patch around in their source

// ---- MODULE: assert ---- 
doassert = function(msg, obj) {
    // eval if msg is a function
    if (typeof(msg) == "function")
        msg = msg();

    if (typeof(msg) == "string" && msg.indexOf("assert") == 0)
        print(msg);
    else
        print("assert: " + msg);

    var ex;
    if (obj) {
        ex = _getErrorWithCode(obj, msg);
    } else {
        ex = Error(msg);
    }
    print(ex.stack);
    throw ex;
};

assert = function(b, msg) {
    if (arguments.length > 2) {
        doassert("Too many parameters to assert().");
    }
    if (arguments.length > 1 && typeof(msg) !== "string") {
        doassert("Non-string 'msg' parameters are invalid for assert().");
    }
    if (assert._debug && msg)
        print("in assert for: " + msg);
    if (b)
        return;
    doassert(msg == undefined ? "assert failed" : "assert failed : " + msg);
};

assert.automsg = function(b) {
    assert(eval(b), b);
};

assert._debug = false;

assert.eq = function(a, b, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (a == b)
        return;

    if ((a != null && b != null) && friendlyEqual(a, b))
        return;

    doassert("[" + tojson(a) + "] != [" + tojson(b) + "] are not equal : " + msg);
};

// Sort doc/obj fields and return new sorted obj
sortDoc = function(doc) {

    // Helper to sort the elements of the array
    var sortElementsOfArray = function(arr) {
        var newArr = [];
        if (!arr || arr.constructor != Array)
            return arr;
        for (var i = 0; i < arr.length; i++) {
            newArr.push(sortDoc(arr[i]));
        }
        return newArr;
    };

    // not a container we can sort
    if (!(doc instanceof Object))
        return doc;

    // if it an array, sort the elements
    if (doc.constructor == Array)
        return sortElementsOfArray(doc);

    var newDoc = {};
    var fields = Object.keys(doc);
    if (fields.length > 0) {
        fields.sort();
        for (var i = 0; i < fields.length; i++) {
            var field = fields[i];
            if (doc.hasOwnProperty(field)) {
                var tmp = doc[field];

                if (tmp) {
                    // Sort recursively for Arrays and Objects (including bson ones)
                    if (tmp.constructor == Array)
                        tmp = sortElementsOfArray(tmp);
                    else if (tmp._bson || tmp.constructor == Object)
                        tmp = sortDoc(tmp);
                }
                newDoc[field] = tmp;
            }
        }
    } else {
        newDoc = doc;
    }

    return newDoc;
};

assert.docEq = function(a, b, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (a == b)
        return;

    var aSorted = sortDoc(a);
    var bSorted = sortDoc(b);

    if ((aSorted != null && bSorted != null) && friendlyEqual(aSorted, bSorted))
        return;

    doassert("[" + tojson(aSorted) + "] != [" + tojson(bSorted) + "] are not equal : " + msg);
};

assert.eq.automsg = function(a, b) {
    assert.eq(eval(a), eval(b), "[" + a + "] != [" + b + "]");
};

assert.neq = function(a, b, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);
    if (a != b)
        return;

    doassert("[" + a + "] != [" + b + "] are equal : " + msg);
};

assert.contains = function(o, arr, msg) {
    var wasIn = false;
    if (!Array.isArray(arr)) {
        throw new Error("The second argument to assert.contains must be an array.");
    }

    for (var i = 0; i < arr.length; i++) {
        wasIn = arr[i] == o || ((arr[i] != null && o != null) && friendlyEqual(arr[i], o));
        if (wasIn) {
            break;
        }
    }

    if (!wasIn) {
        doassert(tojson(o) + " was not in " + tojson(arr) + " : " + msg);
    }
};

assert.soon = function(f, msg, timeout /*ms*/, interval) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (msg) {
        if (typeof(msg) != "function") {
            msg = "assert.soon failed, msg:" + msg;
        }
    } else {
        msg = "assert.soon failed: " + f;
    }

    var start = new Date();
    timeout = timeout || 30000;
    interval = interval || 200;
    var last;
    while (1) {
        if (typeof(f) == "string") {
            if (eval(f))
                return;
        } else {
            if (f())
                return;
        }

        diff = (new Date()).getTime() - start.getTime();
        if (diff > timeout) {
            doassert(msg);
        }
        sleep(interval);
    }
};

assert.time = function(f, msg, timeout /*ms*/) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    var start = new Date();
    timeout = timeout || 30000;
    if (typeof(f) == "string") {
        res = eval(f);
    } else {
        res = f();
    }

    diff = (new Date()).getTime() - start.getTime();
    if (diff > timeout)
        doassert("assert.time failed timeout " + timeout + "ms took " + diff + "ms : " + f +
                 ", msg:" + msg);
    return res;
};

assert.throws = function(func, params, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);
    if (params && typeof(params) == "string") {
        throw("2nd argument to assert.throws has to be an array, not " + params);
    }
    try {
        func.apply(null, params);
    } catch (e) {
        return e;
    }
    doassert("did not throw exception: " + msg);
};

assert.doesNotThrow = function(func, params, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);
    if (params && typeof(params) == "string") {
        throw("2nd argument to assert.throws has to be an array, not " + params);
    }
    var res;
    try {
        res = func.apply(null, params);
    } catch (e) {
        doassert("threw unexpected exception: " + e + " : " + msg);
    }
    return res;
};

assert.throws.automsg = function(func, params) {
    assert.throws(func, params, func.toString());
};

assert.doesNotThrow.automsg = function(func, params) {
    assert.doesNotThrow(func, params, func.toString());
};

assert.commandWorked = function(res, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (res.ok == 1)
        return res;
    doassert("command failed: " + tojson(res) + " : " + msg, res);
};

assert.commandFailed = function(res, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (res.ok == 0)
        return res;
    doassert("command worked when it should have failed: " + tojson(res) + " : " + msg);
};

assert.commandFailedWithCode = function(res, code, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    assert(!res.ok,
           "Command result indicates success, but expected failure with code " + code + ": " +
               tojson(res) + " : " + msg);
    assert.eq(res.code,
              code,
              "Expected failure code did not match actual in command result: " + tojson(res) +
                  " : " + msg);
    return res;
};

assert.isnull = function(what, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (what == null)
        return;
    doassert("supposed to be null (" + (msg || "") + ") was: " + tojson(what));
};

assert.lt = function(a, b, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (a < b)
        return;
    doassert(a + " is not less than " + b + " : " + msg);
};

assert.gt = function(a, b, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (a > b)
        return;
    doassert(a + " is not greater than " + b + " : " + msg);
};

assert.lte = function(a, b, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (a <= b)
        return;
    doassert(a + " is not less than or eq " + b + " : " + msg);
};

assert.gte = function(a, b, msg) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if (a >= b)
        return;
    doassert(a + " is not greater than or eq " + b + " : " + msg);
};

assert.between = function(a, b, c, msg, inclusive) {
    if (assert._debug && msg)
        print("in assert for: " + msg);

    if ((inclusive == undefined || inclusive == true) && a <= b && b <= c)
        return;
    else if (a < b && b < c)
        return;
    doassert(b + " is not between " + a + " and " + c + " : " + msg);
};

assert.betweenIn = function(a, b, c, msg) {
    assert.between(a, b, c, msg, true);
};
assert.betweenEx = function(a, b, c, msg) {
    assert.between(a, b, c, msg, false);
};

assert.close = function(a, b, msg, places) {
    if (places === undefined) {
        places = 4;
    }

    // This treats 'places' as digits past the decimal point.
    var absoluteError = Math.abs(a - b);
    if (Math.round(absoluteError * Math.pow(10, places)) === 0) {
        return;
    }

    // This treats 'places' as significant figures.
    var relativeError = Math.abs(absoluteError / b);
    if (Math.round(relativeError * Math.pow(10, places)) === 0) {
        return;
    }

    doassert(a + " is not equal to " + b + " within " + places + " places, absolute error: " +
             absoluteError + ", relative error: " + relativeError + " : " + msg);
};

/**
 * Asserts if the times in millis are not withing delta milliseconds, in either direction.
 * Default Delta: 1 second
 */
assert.closeWithinMS = function(a, b, msg, deltaMS) {
    "use strict";
    if (deltaMS === undefined) {
        deltaMS = 1000;
    }
    var aMS = a instanceof Date ? a.getTime() : a;
    var bMS = b instanceof Date ? b.getTime() : b;
    var actualDelta = Math.abs(Math.abs(aMS) - Math.abs(bMS));
    if (actualDelta <= deltaMS)
        return;

    doassert(a + " is not equal to " + b + " within " + deltaMS + " millis, actual delta: " +
             actualDelta + " millis : " + msg);
};

assert.writeOK = function(res, msg) {

    var errMsg = null;

    if (res instanceof WriteResult) {
        if (res.hasWriteError()) {
            errMsg = "write failed with error: " + tojson(res);
        } else if (res.hasWriteConcernError()) {
            errMsg = "write concern failed with errors: " + tojson(res);
        }
    } else if (res instanceof BulkWriteResult) {
        // Can only happen with bulk inserts
        if (res.hasWriteErrors()) {
            errMsg = "write failed with errors: " + tojson(res);
        } else if (res.hasWriteConcernError()) {
            errMsg = "write concern failed with errors: " + tojson(res);
        }
    } else if (res instanceof WriteCommandError) {
        // Can only happen with bulk inserts
        errMsg = "write command failed: " + tojson(res);
    } else {
        if (!res || !res.ok) {
            errMsg = "unknown type of write result, cannot check ok: " + tojson(res);
        }
    }

    if (errMsg) {
        if (msg)
            errMsg = errMsg + ": " + msg;
        doassert(errMsg, res);
    }

    return res;
};

assert.writeError = function(res, msg) {

    var errMsg = null;

    if (res instanceof WriteResult) {
        if (!res.hasWriteError() && !res.hasWriteConcernError()) {
            errMsg = "no write error: " + tojson(res);
        }
    } else if (res instanceof BulkWriteResult) {
        // Can only happen with bulk inserts
        if (!res.hasWriteErrors() && !res.hasWriteConcernError()) {
            errMsg = "no write errors: " + tojson(res);
        }
    } else if (res instanceof WriteCommandError) {
        // Can only happen with bulk inserts
        // No-op since we're expecting an error
    } else {
        if (!res || res.ok) {
            errMsg = "unknown type of write result, cannot check error: " + tojson(res);
        }
    }

    if (errMsg) {
        if (msg)
            errMsg = errMsg + ": " + msg;
        doassert(errMsg);
    }

    return res;
};

assert.gleOK = function(res, msg) {

    var errMsg = null;

    if (!res) {
        errMsg = "missing first argument, no response to check";
    } else if (!res.ok) {
        errMsg = "getLastError failed: " + tojson(res);
    } else if ('code' in res || 'errmsg' in res || ('err' in res && res['err'] != null)) {
        errMsg = "write or write concern failed: " + tojson(res);
    }

    if (errMsg) {
        if (msg)
            errMsg = errMsg + ": " + msg;
        doassert(errMsg, res);
    }

    return res;
};

assert.gleSuccess = function(dbOrGLEDoc, msg) {
    var gle = dbOrGLEDoc instanceof DB ? dbOrGLEDoc.getLastErrorObj() : dbOrGLEDoc;
    if (gle.err) {
        if (typeof(msg) == "function")
            msg = msg(gle);
        doassert("getLastError not null:" + tojson(gle) + " :" + msg, gle);
    }
    return gle;
};

assert.gleError = function(dbOrGLEDoc, msg) {
    var gle = dbOrGLEDoc instanceof DB ? dbOrGLEDoc.getLastErrorObj() : dbOrGLEDoc;
    if (!gle.err) {
        if (typeof(msg) == "function")
            msg = msg(gle);
        doassert("getLastError is null: " + tojson(gle) + " :" + msg);
    }
};

assert.gleErrorCode = function(dbOrGLEDoc, code, msg) {
    var gle = dbOrGLEDoc instanceof DB ? dbOrGLEDoc.getLastErrorObj() : dbOrGLEDoc;
    if (!gle.err || gle.code != code) {
        if (typeof(msg) == "function")
            msg = msg(gle);
        doassert("getLastError is null or has code other than \"" + code + "\": " + tojson(gle) +
                 " :" + msg);
    }
};

assert.gleErrorRegex = function(dbOrGLEDoc, regex, msg) {
    var gle = dbOrGLEDoc instanceof DB ? dbOrGLEDoc.getLastErrorObj() : dbOrGLEDoc;
    if (!gle.err || !regex.test(gle.err)) {
        if (typeof(msg) == "function")
            msg = msg(gle);
        doassert("getLastError is null or doesn't match regex (" + regex + "): " + tojson(gle) +
                 " :" + msg);
    }
};



// ---- MODULE: bson ---- 
var bson = (function(){

  var pkgmap        = {},
      global        = {},
      nativeRequire = typeof require != 'undefined' && require,
      lib, ties, main, async;

  function exports(){ return main(); };

  exports.main     = exports;
  exports.module   = module;
  exports.packages = pkgmap;
  exports.pkg      = pkg;
  exports.require  = function require(uri){
    return pkgmap.main.index.require(uri);
  };


  ties             = {};

  aliases          = {};


  return exports;

function join() {
  return normalize(Array.prototype.join.call(arguments, "/"));
};

function normalize(path) {
  var ret = [], parts = path.split('/'), cur, prev;

  var i = 0, l = parts.length-1;
  for (; i <= l; i++) {
    cur = parts[i];

    if (cur === "." && prev !== undefined) continue;

    if (cur === ".." && ret.length && prev !== ".." && prev !== "." && prev !== undefined) {
      ret.pop();
      prev = ret.slice(-1)[0];
    } else {
      if (prev === ".") ret.pop();
      ret.push(cur);
      prev = cur;
    }
  }

  return ret.join("/");
};

function dirname(path) {
  return path && path.substr(0, path.lastIndexOf("/")) || ".";
};

function findModule(workingModule, uri){
  var moduleId      = join(dirname(workingModule.id), /\.\/$/.test(uri) ? (uri + 'index') : uri ).replace(/\.js$/, ''),
      moduleIndexId = join(moduleId, 'index'),
      pkg           = workingModule.pkg,
      module;

  var i = pkg.modules.length,
      id;

  while(i-->0){
    id = pkg.modules[i].id;

    if(id==moduleId || id == moduleIndexId){
      module = pkg.modules[i];
      break;
    }
  }

  return module;
}

function newRequire(callingModule){
  function require(uri){
    var module, pkg;

    if(/^\./.test(uri)){
      module = findModule(callingModule, uri);
    } else if ( ties && ties.hasOwnProperty( uri ) ) {
      return ties[uri];
    } else if ( aliases && aliases.hasOwnProperty( uri ) ) {
      return require(aliases[uri]);
    } else {
      pkg = pkgmap[uri];

      if(!pkg && nativeRequire){
        try {
          pkg = nativeRequire(uri);
        } catch (nativeRequireError) {}

        if(pkg) return pkg;
      }

      if(!pkg){
        throw new Error('Cannot find module "'+uri+'" @[module: '+callingModule.id+' package: '+callingModule.pkg.name+']');
      }

      module = pkg.index;
    }

    if(!module){
      throw new Error('Cannot find module "'+uri+'" @[module: '+callingModule.id+' package: '+callingModule.pkg.name+']');
    }

    module.parent = callingModule;
    return module.call();
  };


  return require;
}


function module(parent, id, wrapper){
  var mod    = { pkg: parent, id: id, wrapper: wrapper },
      cached = false;

  mod.exports = {};
  mod.require = newRequire(mod);

  mod.call = function(){
    if(cached) {
      return mod.exports;
    }

    cached = true;

    global.require = mod.require;

    mod.wrapper(mod, mod.exports, global, global.require);
    return mod.exports;
  };

  if(parent.mainModuleId == mod.id){
    parent.index = mod;
    parent.parents.length === 0 && ( main = mod.call );
  }

  parent.modules.push(mod);
}

function pkg(/* [ parentId ...], wrapper */){
  var wrapper = arguments[ arguments.length - 1 ],
      parents = Array.prototype.slice.call(arguments, 0, arguments.length - 1),
      ctx     = wrapper(parents);


  pkgmap[ctx.name] = ctx;

  arguments.length == 1 && ( pkgmap.main = ctx );

  return function(modules){
    var id;
    for(id in modules){
      module(ctx, id, modules[id]);
    }
  };
}


}(this));

bson.pkg(function(parents){

  return {
    'name'         : 'bson',
    'mainModuleId' : 'bson',
    'modules'      : [],
    'parents'      : parents
  };

})({ 'binary': function(module, exports, global, require, undefined){
  /**
 * Module dependencies.
 */
if(typeof window === 'undefined') { 
  var Buffer = require('buffer').Buffer; // TODO just use global Buffer
}

// Binary default subtype
var BSON_BINARY_SUBTYPE_DEFAULT = 0;

/**
 * @ignore
 * @api private
 */
var writeStringToArray = function(data) {
  // Create a buffer
  var buffer = typeof Uint8Array != 'undefined' ? new Uint8Array(new ArrayBuffer(data.length)) : new Array(data.length);
  // Write the content to the buffer
  for(var i = 0; i < data.length; i++) {
    buffer[i] = data.charCodeAt(i);
  }  
  // Write the string to the buffer
  return buffer;
}

/**
 * Convert Array ot Uint8Array to Binary String
 *
 * @ignore
 * @api private
 */
var convertArraytoUtf8BinaryString = function(byteArray, startIndex, endIndex) {
  var result = "";
  for(var i = startIndex; i < endIndex; i++) {
   result = result + String.fromCharCode(byteArray[i]);
  }
  return result;  
};

/**
 * A class representation of the BSON Binary type.
 * 
 * Sub types
 *  - **BSON.BSON_BINARY_SUBTYPE_DEFAULT**, default BSON type.
 *  - **BSON.BSON_BINARY_SUBTYPE_FUNCTION**, BSON function type.
 *  - **BSON.BSON_BINARY_SUBTYPE_BYTE_ARRAY**, BSON byte array type.
 *  - **BSON.BSON_BINARY_SUBTYPE_UUID**, BSON uuid type.
 *  - **BSON.BSON_BINARY_SUBTYPE_MD5**, BSON md5 type.
 *  - **BSON.BSON_BINARY_SUBTYPE_USER_DEFINED**, BSON user defined type.
 *
 * @class Represents the Binary BSON type.
 * @param {Buffer} buffer a buffer object containing the binary data.
 * @param {Number} [subType] the option binary type.
 * @return {Grid}
 */
function Binary(buffer, subType) {
  if(!(this instanceof Binary)) return new Binary(buffer, subType);
  
  this._bsontype = 'Binary';

  if(buffer instanceof Number) {
    this.sub_type = buffer;
    this.position = 0;
  } else {    
    this.sub_type = subType == null ? BSON_BINARY_SUBTYPE_DEFAULT : subType;
    this.position = 0;
  }

  if(buffer != null && !(buffer instanceof Number)) {
    // Only accept Buffer, Uint8Array or Arrays
    if(typeof buffer == 'string') {
      // Different ways of writing the length of the string for the different types
      if(typeof Buffer != 'undefined') {
        this.buffer = new Buffer(buffer);
      } else if(typeof Uint8Array != 'undefined' || (Object.prototype.toString.call(buffer) == '[object Array]')) {
        this.buffer = writeStringToArray(buffer);
      } else {
        throw new Error("only String, Buffer, Uint8Array or Array accepted");
      }
    } else {
      this.buffer = buffer;      
    }
    this.position = buffer.length;
  } else {
    if(typeof Buffer != 'undefined') {
      this.buffer =  new Buffer(Binary.BUFFER_SIZE);      
    } else if(typeof Uint8Array != 'undefined'){
      this.buffer = new Uint8Array(new ArrayBuffer(Binary.BUFFER_SIZE));
    } else {
      this.buffer = new Array(Binary.BUFFER_SIZE);
    }
    // Set position to start of buffer
    this.position = 0;
  }
};

/**
 * Updates this binary with byte_value.
 *
 * @param {Character} byte_value a single byte we wish to write.
 * @api public
 */
Binary.prototype.put = function put(byte_value) {
  // If it's a string and a has more than one character throw an error
  if(byte_value['length'] != null && typeof byte_value != 'number' && byte_value.length != 1) throw new Error("only accepts single character String, Uint8Array or Array");
  if(typeof byte_value != 'number' && byte_value < 0 || byte_value > 255) throw new Error("only accepts number in a valid unsigned byte range 0-255");
  
  // Decode the byte value once
  var decoded_byte = null;
  if(typeof byte_value == 'string') {
    decoded_byte = byte_value.charCodeAt(0);      
  } else if(byte_value['length'] != null) {
    decoded_byte = byte_value[0];
  } else {
    decoded_byte = byte_value;
  }
  
  if(this.buffer.length > this.position) {
    this.buffer[this.position++] = decoded_byte;
  } else {
    if(typeof Buffer != 'undefined' && Buffer.isBuffer(this.buffer)) {    
      // Create additional overflow buffer
      var buffer = new Buffer(Binary.BUFFER_SIZE + this.buffer.length);
      // Combine the two buffers together
      this.buffer.copy(buffer, 0, 0, this.buffer.length);
      this.buffer = buffer;
      this.buffer[this.position++] = decoded_byte;
    } else {
      var buffer = null;
      // Create a new buffer (typed or normal array)
      if(Object.prototype.toString.call(this.buffer) == '[object Uint8Array]') {
        buffer = new Uint8Array(new ArrayBuffer(Binary.BUFFER_SIZE + this.buffer.length));
      } else {
        buffer = new Array(Binary.BUFFER_SIZE + this.buffer.length);
      }      
      
      // We need to copy all the content to the new array
      for(var i = 0; i < this.buffer.length; i++) {
        buffer[i] = this.buffer[i];
      }
      
      // Reassign the buffer
      this.buffer = buffer;
      // Write the byte
      this.buffer[this.position++] = decoded_byte;
    }
  }
};

/**
 * Writes a buffer or string to the binary.
 *
 * @param {Buffer|String} string a string or buffer to be written to the Binary BSON object.
 * @param {Number} offset specify the binary of where to write the content.
 * @api public
 */
Binary.prototype.write = function write(string, offset) {
  offset = typeof offset == 'number' ? offset : this.position;

  // If the buffer is to small let's extend the buffer
  if(this.buffer.length < offset + string.length) {
    var buffer = null;
    // If we are in node.js
    if(typeof Buffer != 'undefined' && Buffer.isBuffer(this.buffer)) {      
      buffer = new Buffer(this.buffer.length + string.length);
      this.buffer.copy(buffer, 0, 0, this.buffer.length);      
    } else if(Object.prototype.toString.call(this.buffer) == '[object Uint8Array]') {
      // Create a new buffer
      buffer = new Uint8Array(new ArrayBuffer(this.buffer.length + string.length))
      // Copy the content
      for(var i = 0; i < this.position; i++) {
        buffer[i] = this.buffer[i];
      }
    }
    
    // Assign the new buffer
    this.buffer = buffer;
  }

  if(typeof Buffer != 'undefined' && Buffer.isBuffer(string) && Buffer.isBuffer(this.buffer)) {
    string.copy(this.buffer, offset, 0, string.length);
    this.position = (offset + string.length) > this.position ? (offset + string.length) : this.position;
    // offset = string.length
  } else if(typeof Buffer != 'undefined' && typeof string == 'string' && Buffer.isBuffer(this.buffer)) {
    this.buffer.write(string, 'binary', offset);
    this.position = (offset + string.length) > this.position ? (offset + string.length) : this.position;
    // offset = string.length;
  } else if(Object.prototype.toString.call(string) == '[object Uint8Array]' 
    || Object.prototype.toString.call(string) == '[object Array]' && typeof string != 'string') {      
    for(var i = 0; i < string.length; i++) {
      this.buffer[offset++] = string[i];
    }    

    this.position = offset > this.position ? offset : this.position;
  } else if(typeof string == 'string') {
    for(var i = 0; i < string.length; i++) {
      this.buffer[offset++] = string.charCodeAt(i);
    }

    this.position = offset > this.position ? offset : this.position;
  }
};

/**
 * Reads **length** bytes starting at **position**.
 *
 * @param {Number} position read from the given position in the Binary.
 * @param {Number} length the number of bytes to read.
 * @return {Buffer}
 * @api public
 */
Binary.prototype.read = function read(position, length) {
  length = length && length > 0
    ? length
    : this.position;
  
  // Let's return the data based on the type we have
  if(this.buffer['slice']) {
    return this.buffer.slice(position, position + length);
  } else {
    // Create a buffer to keep the result
    var buffer = typeof Uint8Array != 'undefined' ? new Uint8Array(new ArrayBuffer(length)) : new Array(length);
    for(var i = 0; i < length; i++) {
      buffer[i] = this.buffer[position++];
    }
  }
  // Return the buffer
  return buffer;
};

/**
 * Returns the value of this binary as a string.
 *
 * @return {String}
 * @api public
 */
Binary.prototype.value = function value(asRaw) {
  asRaw = asRaw == null ? false : asRaw;  
  
  // If it's a node.js buffer object
  if(typeof Buffer != 'undefined' && Buffer.isBuffer(this.buffer)) {
    return asRaw ? this.buffer.slice(0, this.position) : this.buffer.toString('binary', 0, this.position);
  } else {
    if(asRaw) {
      // we support the slice command use it
      if(this.buffer['slice'] != null) {
        return this.buffer.slice(0, this.position);
      } else {
        // Create a new buffer to copy content to
        var newBuffer = Object.prototype.toString.call(this.buffer) == '[object Uint8Array]' ? new Uint8Array(new ArrayBuffer(this.position)) : new Array(this.position);
        // Copy content
        for(var i = 0; i < this.position; i++) {
          newBuffer[i] = this.buffer[i];
        }
        // Return the buffer
        return newBuffer;
      }
    } else {
      return convertArraytoUtf8BinaryString(this.buffer, 0, this.position);
    }
  }
};

/**
 * Length.
 *
 * @return {Number} the length of the binary.
 * @api public
 */
Binary.prototype.length = function length() {
  return this.position;
};

/**
 * @ignore
 * @api private
 */
Binary.prototype.toJSON = function() {
  return this.buffer != null ? this.buffer.toString('base64') : '';
}

/**
 * @ignore
 * @api private
 */
Binary.prototype.toString = function(format) {
  return this.buffer != null ? this.buffer.slice(0, this.position).toString(format) : '';
}

Binary.BUFFER_SIZE = 256;

/**
 * Default BSON type
 *  
 * @classconstant SUBTYPE_DEFAULT
 **/
Binary.SUBTYPE_DEFAULT = 0;
/**
 * Function BSON type
 *  
 * @classconstant SUBTYPE_DEFAULT
 **/
Binary.SUBTYPE_FUNCTION = 1;
/**
 * Byte Array BSON type
 *  
 * @classconstant SUBTYPE_DEFAULT
 **/
Binary.SUBTYPE_BYTE_ARRAY = 2;
/**
 * OLD UUID BSON type
 *  
 * @classconstant SUBTYPE_DEFAULT
 **/
Binary.SUBTYPE_UUID_OLD = 3;
/**
 * UUID BSON type
 *  
 * @classconstant SUBTYPE_DEFAULT
 **/
Binary.SUBTYPE_UUID = 4;
/**
 * MD5 BSON type
 *  
 * @classconstant SUBTYPE_DEFAULT
 **/
Binary.SUBTYPE_MD5 = 5;
/**
 * User BSON type
 *  
 * @classconstant SUBTYPE_DEFAULT
 **/
Binary.SUBTYPE_USER_DEFINED = 128;

/**
 * Expose.
 */
exports.Binary = Binary;


}, 



'binary_parser': function(module, exports, global, require, undefined){
  /**
 * Binary Parser.
 * Jonas Raoni Soares Silva
 * http://jsfromhell.com/classes/binary-parser [v1.0]
 */
var chr = String.fromCharCode;

var maxBits = [];
for (var i = 0; i < 64; i++) {
	maxBits[i] = Math.pow(2, i);
}

function BinaryParser (bigEndian, allowExceptions) {
  if(!(this instanceof BinaryParser)) return new BinaryParser(bigEndian, allowExceptions);
  
	this.bigEndian = bigEndian;
	this.allowExceptions = allowExceptions;
};

BinaryParser.warn = function warn (msg) {
	if (this.allowExceptions) {
		throw new Error(msg);
  }

	return 1;
};

BinaryParser.decodeFloat = function decodeFloat (data, precisionBits, exponentBits) {
	var b = new this.Buffer(this.bigEndian, data);

	b.checkBuffer(precisionBits + exponentBits + 1);

	var bias = maxBits[exponentBits - 1] - 1
    , signal = b.readBits(precisionBits + exponentBits, 1)
    , exponent = b.readBits(precisionBits, exponentBits)
    , significand = 0
    , divisor = 2
    , curByte = b.buffer.length + (-precisionBits >> 3) - 1;

	do {
		for (var byteValue = b.buffer[ ++curByte ], startBit = precisionBits % 8 || 8, mask = 1 << startBit; mask >>= 1; ( byteValue & mask ) && ( significand += 1 / divisor ), divisor *= 2 );
	} while (precisionBits -= startBit);

	return exponent == ( bias << 1 ) + 1 ? significand ? NaN : signal ? -Infinity : +Infinity : ( 1 + signal * -2 ) * ( exponent || significand ? !exponent ? Math.pow( 2, -bias + 1 ) * significand : Math.pow( 2, exponent - bias ) * ( 1 + significand ) : 0 );
};

BinaryParser.decodeInt = function decodeInt (data, bits, signed, forceBigEndian) {
  var b = new this.Buffer(this.bigEndian || forceBigEndian, data)
      , x = b.readBits(0, bits)
      , max = maxBits[bits]; //max = Math.pow( 2, bits );
  
  return signed && x >= max / 2
      ? x - max
      : x;
};

BinaryParser.encodeFloat = function encodeFloat (data, precisionBits, exponentBits) {
	var bias = maxBits[exponentBits - 1] - 1
    , minExp = -bias + 1
    , maxExp = bias
    , minUnnormExp = minExp - precisionBits
    , n = parseFloat(data)
    , status = isNaN(n) || n == -Infinity || n == +Infinity ? n : 0
    ,	exp = 0
    , len = 2 * bias + 1 + precisionBits + 3
    , bin = new Array(len)
    , signal = (n = status !== 0 ? 0 : n) < 0
    , intPart = Math.floor(n = Math.abs(n))
    , floatPart = n - intPart
    , lastBit
    , rounded
    , result
    , i
    , j;

	for (i = len; i; bin[--i] = 0);

	for (i = bias + 2; intPart && i; bin[--i] = intPart % 2, intPart = Math.floor(intPart / 2));

	for (i = bias + 1; floatPart > 0 && i; (bin[++i] = ((floatPart *= 2) >= 1) - 0 ) && --floatPart);

	for (i = -1; ++i < len && !bin[i];);

	if (bin[(lastBit = precisionBits - 1 + (i = (exp = bias + 1 - i) >= minExp && exp <= maxExp ? i + 1 : bias + 1 - (exp = minExp - 1))) + 1]) {
		if (!(rounded = bin[lastBit])) {
			for (j = lastBit + 2; !rounded && j < len; rounded = bin[j++]);
		}

		for (j = lastBit + 1; rounded && --j >= 0; (bin[j] = !bin[j] - 0) && (rounded = 0));
	}

	for (i = i - 2 < 0 ? -1 : i - 3; ++i < len && !bin[i];);

	if ((exp = bias + 1 - i) >= minExp && exp <= maxExp) {
		++i;
  } else if (exp < minExp) {
		exp != bias + 1 - len && exp < minUnnormExp && this.warn("encodeFloat::float underflow");
		i = bias + 1 - (exp = minExp - 1);
	}

	if (intPart || status !== 0) {
		this.warn(intPart ? "encodeFloat::float overflow" : "encodeFloat::" + status);
		exp = maxExp + 1;
		i = bias + 2;

		if (status == -Infinity) {
			signal = 1;
    } else if (isNaN(status)) {
			bin[i] = 1;
    }
	}

	for (n = Math.abs(exp + bias), j = exponentBits + 1, result = ""; --j; result = (n % 2) + result, n = n >>= 1);

	for (n = 0, j = 0, i = (result = (signal ? "1" : "0") + result + bin.slice(i, i + precisionBits).join("")).length, r = []; i; j = (j + 1) % 8) {
		n += (1 << j) * result.charAt(--i);
		if (j == 7) {
			r[r.length] = String.fromCharCode(n);
			n = 0;
		}
	}

	r[r.length] = n
    ? String.fromCharCode(n)
    : "";

	return (this.bigEndian ? r.reverse() : r).join("");
};

BinaryParser.encodeInt = function encodeInt (data, bits, signed, forceBigEndian) {
	var max = maxBits[bits];

  if (data >= max || data < -(max / 2)) {
    this.warn("encodeInt::overflow");
    data = 0;
  }

	if (data < 0) {
    data += max;
  }

	for (var r = []; data; r[r.length] = String.fromCharCode(data % 256), data = Math.floor(data / 256));

	for (bits = -(-bits >> 3) - r.length; bits--; r[r.length] = "\0");

  return ((this.bigEndian || forceBigEndian) ? r.reverse() : r).join("");
};

BinaryParser.toSmall    = function( data ){ return this.decodeInt( data,  8, true  ); };
BinaryParser.fromSmall  = function( data ){ return this.encodeInt( data,  8, true  ); };
BinaryParser.toByte     = function( data ){ return this.decodeInt( data,  8, false ); };
BinaryParser.fromByte   = function( data ){ return this.encodeInt( data,  8, false ); };
BinaryParser.toShort    = function( data ){ return this.decodeInt( data, 16, true  ); };
BinaryParser.fromShort  = function( data ){ return this.encodeInt( data, 16, true  ); };
BinaryParser.toWord     = function( data ){ return this.decodeInt( data, 16, false ); };
BinaryParser.fromWord   = function( data ){ return this.encodeInt( data, 16, false ); };
BinaryParser.toInt      = function( data ){ return this.decodeInt( data, 32, true  ); };
BinaryParser.fromInt    = function( data ){ return this.encodeInt( data, 32, true  ); };
BinaryParser.toLong     = function( data ){ return this.decodeInt( data, 64, true  ); };
BinaryParser.fromLong   = function( data ){ return this.encodeInt( data, 64, true  ); };
BinaryParser.toDWord    = function( data ){ return this.decodeInt( data, 32, false ); };
BinaryParser.fromDWord  = function( data ){ return this.encodeInt( data, 32, false ); };
BinaryParser.toQWord    = function( data ){ return this.decodeInt( data, 64, true ); };
BinaryParser.fromQWord  = function( data ){ return this.encodeInt( data, 64, true ); };
BinaryParser.toFloat    = function( data ){ return this.decodeFloat( data, 23, 8   ); };
BinaryParser.fromFloat  = function( data ){ return this.encodeFloat( data, 23, 8   ); };
BinaryParser.toDouble   = function( data ){ return this.decodeFloat( data, 52, 11  ); };
BinaryParser.fromDouble = function( data ){ return this.encodeFloat( data, 52, 11  ); };

// Factor out the encode so it can be shared by add_header and push_int32
BinaryParser.encode_int32 = function encode_int32 (number, asArray) {
  var a, b, c, d, unsigned;
  unsigned = (number < 0) ? (number + 0x100000000) : number;
  a = Math.floor(unsigned / 0xffffff);
  unsigned &= 0xffffff;
  b = Math.floor(unsigned / 0xffff);
  unsigned &= 0xffff;
  c = Math.floor(unsigned / 0xff);
  unsigned &= 0xff;
  d = Math.floor(unsigned);
  return asArray ? [chr(a), chr(b), chr(c), chr(d)] : chr(a) + chr(b) + chr(c) + chr(d);
};

BinaryParser.encode_int64 = function encode_int64 (number) {
  var a, b, c, d, e, f, g, h, unsigned;
  unsigned = (number < 0) ? (number + 0x10000000000000000) : number;
  a = Math.floor(unsigned / 0xffffffffffffff);
  unsigned &= 0xffffffffffffff;
  b = Math.floor(unsigned / 0xffffffffffff);
  unsigned &= 0xffffffffffff;
  c = Math.floor(unsigned / 0xffffffffff);
  unsigned &= 0xffffffffff;
  d = Math.floor(unsigned / 0xffffffff);
  unsigned &= 0xffffffff;
  e = Math.floor(unsigned / 0xffffff);
  unsigned &= 0xffffff;
  f = Math.floor(unsigned / 0xffff);
  unsigned &= 0xffff;
  g = Math.floor(unsigned / 0xff);
  unsigned &= 0xff;
  h = Math.floor(unsigned);
  return chr(a) + chr(b) + chr(c) + chr(d) + chr(e) + chr(f) + chr(g) + chr(h);
};

/**
 * UTF8 methods
 */

// Take a raw binary string and return a utf8 string
BinaryParser.decode_utf8 = function decode_utf8 (binaryStr) {
  var len = binaryStr.length
    , decoded = ''
    , i = 0
    , c = 0
    , c1 = 0
    , c2 = 0
    , c3;

  while (i < len) {
    c = binaryStr.charCodeAt(i);
    if (c < 128) {
      decoded += String.fromCharCode(c);
      i++;
    } else if ((c > 191) && (c < 224)) {
	    c2 = binaryStr.charCodeAt(i+1);
      decoded += String.fromCharCode(((c & 31) << 6) | (c2 & 63));
      i += 2;
    } else {
	    c2 = binaryStr.charCodeAt(i+1);
	    c3 = binaryStr.charCodeAt(i+2);
      decoded += String.fromCharCode(((c & 15) << 12) | ((c2 & 63) << 6) | (c3 & 63));
      i += 3;
    }
  }

  return decoded;
};

// Encode a cstring
BinaryParser.encode_cstring = function encode_cstring (s) {
  return unescape(encodeURIComponent(s)) + BinaryParser.fromByte(0);
};

// Take a utf8 string and return a binary string
BinaryParser.encode_utf8 = function encode_utf8 (s) {
  var a = ""
    , c;

  for (var n = 0, len = s.length; n < len; n++) {
    c = s.charCodeAt(n);

    if (c < 128) {
	    a += String.fromCharCode(c);
    } else if ((c > 127) && (c < 2048)) {
	    a += String.fromCharCode((c>>6) | 192) ;
	    a += String.fromCharCode((c&63) | 128);
    } else {
      a += String.fromCharCode((c>>12) | 224);
      a += String.fromCharCode(((c>>6) & 63) | 128);
      a += String.fromCharCode((c&63) | 128);
    }
  }

  return a;
};

BinaryParser.hprint = function hprint (s) {
  var number;

  for (var i = 0, len = s.length; i < len; i++) {
    if (s.charCodeAt(i) < 32) {
      number = s.charCodeAt(i) <= 15
        ? "0" + s.charCodeAt(i).toString(16)
        : s.charCodeAt(i).toString(16);        
      process.stdout.write(number + " ")
    } else {
      number = s.charCodeAt(i) <= 15
        ? "0" + s.charCodeAt(i).toString(16)
        : s.charCodeAt(i).toString(16);
        process.stdout.write(number + " ")
    }
  }
  
  process.stdout.write("\n\n");
};

BinaryParser.ilprint = function hprint (s) {
  var number;

  for (var i = 0, len = s.length; i < len; i++) {
    if (s.charCodeAt(i) < 32) {
      number = s.charCodeAt(i) <= 15
        ? "0" + s.charCodeAt(i).toString(10)
        : s.charCodeAt(i).toString(10);

      require('util').debug(number+' : ');
    } else {
      number = s.charCodeAt(i) <= 15
        ? "0" + s.charCodeAt(i).toString(10)
        : s.charCodeAt(i).toString(10);
      require('util').debug(number+' : '+ s.charAt(i));
    }
  }
};

BinaryParser.hlprint = function hprint (s) {
  var number;

  for (var i = 0, len = s.length; i < len; i++) {
    if (s.charCodeAt(i) < 32) {
      number = s.charCodeAt(i) <= 15
        ? "0" + s.charCodeAt(i).toString(16)
        : s.charCodeAt(i).toString(16);
      require('util').debug(number+' : ');
    } else {
      number = s.charCodeAt(i) <= 15
        ? "0" + s.charCodeAt(i).toString(16)
        : s.charCodeAt(i).toString(16);
      require('util').debug(number+' : '+ s.charAt(i));
    }
  }
};

/**
 * BinaryParser buffer constructor.
 */
function BinaryParserBuffer (bigEndian, buffer) {
  this.bigEndian = bigEndian || 0;
  this.buffer = [];
  this.setBuffer(buffer);
};

BinaryParserBuffer.prototype.setBuffer = function setBuffer (data) {
  var l, i, b;

	if (data) {
    i = l = data.length;
    b = this.buffer = new Array(l);
		for (; i; b[l - i] = data.charCodeAt(--i));
		this.bigEndian && b.reverse();
	}
};

BinaryParserBuffer.prototype.hasNeededBits = function hasNeededBits (neededBits) {
	return this.buffer.length >= -(-neededBits >> 3);
};

BinaryParserBuffer.prototype.checkBuffer = function checkBuffer (neededBits) {
	if (!this.hasNeededBits(neededBits)) {
		throw new Error("checkBuffer::missing bytes");
  }
};

BinaryParserBuffer.prototype.readBits = function readBits (start, length) {
	//shl fix: Henri Torgemane ~1996 (compressed by Jonas Raoni)

	function shl (a, b) {
		for (; b--; a = ((a %= 0x7fffffff + 1) & 0x40000000) == 0x40000000 ? a * 2 : (a - 0x40000000) * 2 + 0x7fffffff + 1);
		return a;
	}

	if (start < 0 || length <= 0) {
		return 0;
  }

	this.checkBuffer(start + length);

  var offsetLeft
    , offsetRight = start % 8
    , curByte = this.buffer.length - ( start >> 3 ) - 1
    , lastByte = this.buffer.length + ( -( start + length ) >> 3 )
    , diff = curByte - lastByte
    , sum = ((this.buffer[ curByte ] >> offsetRight) & ((1 << (diff ? 8 - offsetRight : length)) - 1)) + (diff && (offsetLeft = (start + length) % 8) ? (this.buffer[lastByte++] & ((1 << offsetLeft) - 1)) << (diff-- << 3) - offsetRight : 0);

	for(; diff; sum += shl(this.buffer[lastByte++], (diff-- << 3) - offsetRight));

	return sum;
};

/**
 * Expose.
 */
BinaryParser.Buffer = BinaryParserBuffer;

exports.BinaryParser = BinaryParser;

}, 



'bson': function(module, exports, global, require, undefined){
  var Long = require('./long').Long
  , Double = require('./double').Double
  , Timestamp = require('./timestamp').Timestamp
  , ObjectID = require('./objectid').ObjectID
  , Symbol = require('./symbol').Symbol
  , Code = require('./code').Code
  , MinKey = require('./min_key').MinKey
  , MaxKey = require('./max_key').MaxKey
  , DBRef = require('./db_ref').DBRef
  , Binary = require('./binary').Binary
  , BinaryParser = require('./binary_parser').BinaryParser
  , writeIEEE754 = require('./float_parser').writeIEEE754
  , readIEEE754 = require('./float_parser').readIEEE754

// To ensure that 0.4 of node works correctly
var isDate = function isDate(d) {
  return typeof d === 'object' && Object.prototype.toString.call(d) === '[object Date]';
}

/**
 * Create a new BSON instance
 *
 * @class Represents the BSON Parser
 * @return {BSON} instance of BSON Parser.
 */
function BSON () {};

/**
 * @ignore
 * @api private
 */
// BSON MAX VALUES
BSON.BSON_INT32_MAX = 0x7FFFFFFF;
BSON.BSON_INT32_MIN = -0x80000000;

BSON.BSON_INT64_MAX = Math.pow(2, 63) - 1;
BSON.BSON_INT64_MIN = -Math.pow(2, 63);

// JS MAX PRECISE VALUES
BSON.JS_INT_MAX = 0x20000000000000;  // Any integer up to 2^53 can be precisely represented by a double.
BSON.JS_INT_MIN = -0x20000000000000;  // Any integer down to -2^53 can be precisely represented by a double.

// Internal long versions
var JS_INT_MAX_LONG = Long.fromNumber(0x20000000000000);  // Any integer up to 2^53 can be precisely represented by a double.
var JS_INT_MIN_LONG = Long.fromNumber(-0x20000000000000);  // Any integer down to -2^53 can be precisely represented by a double.

/**
 * Number BSON Type
 *
 * @classconstant BSON_DATA_NUMBER
 **/
BSON.BSON_DATA_NUMBER = 1;
/**
 * String BSON Type
 *
 * @classconstant BSON_DATA_STRING
 **/
BSON.BSON_DATA_STRING = 2;
/**
 * Object BSON Type
 *
 * @classconstant BSON_DATA_OBJECT
 **/
BSON.BSON_DATA_OBJECT = 3;
/**
 * Array BSON Type
 *
 * @classconstant BSON_DATA_ARRAY
 **/
BSON.BSON_DATA_ARRAY = 4;
/**
 * Binary BSON Type
 *
 * @classconstant BSON_DATA_BINARY
 **/
BSON.BSON_DATA_BINARY = 5;
/**
 * ObjectID BSON Type
 *
 * @classconstant BSON_DATA_OID
 **/
BSON.BSON_DATA_OID = 7;
/**
 * Boolean BSON Type
 *
 * @classconstant BSON_DATA_BOOLEAN
 **/
BSON.BSON_DATA_BOOLEAN = 8;
/**
 * Date BSON Type
 *
 * @classconstant BSON_DATA_DATE
 **/
BSON.BSON_DATA_DATE = 9;
/**
 * null BSON Type
 *
 * @classconstant BSON_DATA_NULL
 **/
BSON.BSON_DATA_NULL = 10;
/**
 * RegExp BSON Type
 *
 * @classconstant BSON_DATA_REGEXP
 **/
BSON.BSON_DATA_REGEXP = 11;
/**
 * Code BSON Type
 *
 * @classconstant BSON_DATA_CODE
 **/
BSON.BSON_DATA_CODE = 13;
/**
 * Symbol BSON Type
 *
 * @classconstant BSON_DATA_SYMBOL
 **/
BSON.BSON_DATA_SYMBOL = 14;
/**
 * Code with Scope BSON Type
 *
 * @classconstant BSON_DATA_CODE_W_SCOPE
 **/
BSON.BSON_DATA_CODE_W_SCOPE = 15;
/**
 * 32 bit Integer BSON Type
 *
 * @classconstant BSON_DATA_INT
 **/
BSON.BSON_DATA_INT = 16;
/**
 * Timestamp BSON Type
 *
 * @classconstant BSON_DATA_TIMESTAMP
 **/
BSON.BSON_DATA_TIMESTAMP = 17;
/**
 * Long BSON Type
 *
 * @classconstant BSON_DATA_LONG
 **/
BSON.BSON_DATA_LONG = 18;
/**
 * MinKey BSON Type
 *
 * @classconstant BSON_DATA_MIN_KEY
 **/
BSON.BSON_DATA_MIN_KEY = 0xff;
/**
 * MaxKey BSON Type
 *
 * @classconstant BSON_DATA_MAX_KEY
 **/
BSON.BSON_DATA_MAX_KEY = 0x7f;

/**
 * Binary Default Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_DEFAULT
 **/
BSON.BSON_BINARY_SUBTYPE_DEFAULT = 0;
/**
 * Binary Function Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_FUNCTION
 **/
BSON.BSON_BINARY_SUBTYPE_FUNCTION = 1;
/**
 * Binary Byte Array Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_BYTE_ARRAY
 **/
BSON.BSON_BINARY_SUBTYPE_BYTE_ARRAY = 2;
/**
 * Binary UUID Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_UUID
 **/
BSON.BSON_BINARY_SUBTYPE_UUID = 3;
/**
 * Binary MD5 Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_MD5
 **/
BSON.BSON_BINARY_SUBTYPE_MD5 = 4;
/**
 * Binary User Defined Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_USER_DEFINED
 **/
BSON.BSON_BINARY_SUBTYPE_USER_DEFINED = 128;

/**
 * Calculate the bson size for a passed in Javascript object.
 *
 * @param {Object} object the Javascript object to calculate the BSON byte size for.
 * @param {Boolean} [serializeFunctions] serialize all functions in the object **(default:false)**.
 * @return {Number} returns the number of bytes the BSON object will take up.
 * @api public
 */
BSON.calculateObjectSize = function calculateObjectSize(object, serializeFunctions) {
  var totalLength = (4 + 1);

  if(Array.isArray(object)) {
    for(var i = 0; i < object.length; i++) {
      totalLength += calculateElement(i.toString(), object[i], serializeFunctions)
    }
  } else {
		// If we have toBSON defined, override the current object
		if(object.toBSON) {
			object = object.toBSON();
		}

		// Calculate size
    for(var key in object) {
      totalLength += calculateElement(key, object[key], serializeFunctions)
    }
  }

  return totalLength;
}

/**
 * @ignore
 * @api private
 */
function calculateElement(name, value, serializeFunctions) {
  var isBuffer = typeof Buffer !== 'undefined';

  switch(typeof value) {
    case 'string':
      return 1 + (!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1 + 4 + (!isBuffer ? numberOfBytes(value) : Buffer.byteLength(value, 'utf8')) + 1;
    case 'number':
      if(Math.floor(value) === value && value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
        if(value >= BSON.BSON_INT32_MIN && value <= BSON.BSON_INT32_MAX) { // 32 bit
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (4 + 1);
        } else {
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (8 + 1);
        }
      } else {  // 64 bit
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (8 + 1);
      }
    case 'undefined':
      return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (1);
    case 'boolean':
      return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (1 + 1);
    case 'object':
      if(value == null || value instanceof MinKey || value instanceof MaxKey || value['_bsontype'] == 'MinKey' || value['_bsontype'] == 'MaxKey') {
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (1);
      } else if(value instanceof ObjectID || value['_bsontype'] == 'ObjectID') {
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (12 + 1);
      } else if(value instanceof Date || isDate(value)) {
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (8 + 1);
      } else if(typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (1 + 4 + 1) + value.length;
      } else if(value instanceof Long || value instanceof Double || value instanceof Timestamp
          || value['_bsontype'] == 'Long' || value['_bsontype'] == 'Double' || value['_bsontype'] == 'Timestamp') {
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (8 + 1);
      } else if(value instanceof Code || value['_bsontype'] == 'Code') {
        // Calculate size depending on the availability of a scope
        if(value.scope != null && Object.keys(value.scope).length > 0) {
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + 1 + 4 + 4 + (!isBuffer ? numberOfBytes(value.code.toString()) : Buffer.byteLength(value.code.toString(), 'utf8')) + 1 + BSON.calculateObjectSize(value.scope, serializeFunctions);
        } else {
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + 1 + 4 + (!isBuffer ? numberOfBytes(value.code.toString()) : Buffer.byteLength(value.code.toString(), 'utf8')) + 1;
        }
      } else if(value instanceof Binary || value['_bsontype'] == 'Binary') {
        // Check what kind of subtype we have
        if(value.sub_type == Binary.SUBTYPE_BYTE_ARRAY) {
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (value.position + 1 + 4 + 1 + 4);
        } else {
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + (value.position + 1 + 4 + 1);
        }
      } else if(value instanceof Symbol || value['_bsontype'] == 'Symbol') {
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + ((!isBuffer ? numberOfBytes(value.value) : Buffer.byteLength(value.value, 'utf8')) + 4 + 1 + 1);
      } else if(value instanceof DBRef || value['_bsontype'] == 'DBRef') {
        // Set up correct object for serialization
        var ordered_values = {
            '$ref': value.namespace
          , '$id' : value.oid
        };

        // Add db reference if it exists
        if(null != value.db) {
          ordered_values['$db'] = value.db;
        }

        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + 1 + BSON.calculateObjectSize(ordered_values, serializeFunctions);
      } else if(value instanceof RegExp || Object.prototype.toString.call(value) === '[object RegExp]') {
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + 1 + (!isBuffer ? numberOfBytes(value.source) : Buffer.byteLength(value.source, 'utf8')) + 1
            + (value.global ? 1 : 0) + (value.ignoreCase ? 1 : 0) + (value.multiline ? 1 : 0) + 1
      } else {
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + BSON.calculateObjectSize(value, serializeFunctions) + 1;
      }
    case 'function':
      // WTF for 0.4.X where typeof /someregexp/ === 'function'
      if(value instanceof RegExp || Object.prototype.toString.call(value) === '[object RegExp]' || String.call(value) == '[object RegExp]') {
        return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + 1 + (!isBuffer ? numberOfBytes(value.source) : Buffer.byteLength(value.source, 'utf8')) + 1
          + (value.global ? 1 : 0) + (value.ignoreCase ? 1 : 0) + (value.multiline ? 1 : 0) + 1
      } else {
        if(serializeFunctions && value.scope != null && Object.keys(value.scope).length > 0) {
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + 1 + 4 + 4 + (!isBuffer ? numberOfBytes(value.toString()) : Buffer.byteLength(value.toString(), 'utf8')) + 1 + BSON.calculateObjectSize(value.scope, serializeFunctions);
        } else if(serializeFunctions) {
          return (name != null ? ((!isBuffer ? numberOfBytes(name) : Buffer.byteLength(name, 'utf8')) + 1) : 0) + 1 + 4 + (!isBuffer ? numberOfBytes(value.toString()) : Buffer.byteLength(value.toString(), 'utf8')) + 1;
        }
      }
  }

  return 0;
}

/**
 * Serialize a Javascript object using a predefined Buffer and index into the buffer, useful when pre-allocating the space for serialization.
 *
 * @param {Object} object the Javascript object to serialize.
 * @param {Boolean} checkKeys the serializer will check if keys are valid.
 * @param {Buffer} buffer the Buffer you pre-allocated to store the serialized BSON object.
 * @param {Number} index the index in the buffer where we wish to start serializing into.
 * @param {Boolean} serializeFunctions serialize the javascript functions **(default:false)**.
 * @return {Number} returns the new write index in the Buffer.
 * @api public
 */
BSON.serializeWithBufferAndIndex = function serializeWithBufferAndIndex(object, checkKeys, buffer, index, serializeFunctions) {
  // Default setting false
  serializeFunctions = serializeFunctions == null ? false : serializeFunctions;
  // Write end information (length of the object)
  var size = buffer.length;
  // Write the size of the object
  buffer[index++] = size & 0xff;
  buffer[index++] = (size >> 8) & 0xff;
  buffer[index++] = (size >> 16) & 0xff;
  buffer[index++] = (size >> 24) & 0xff;
  return serializeObject(object, checkKeys, buffer, index, serializeFunctions) - 1;
}

/**
 * @ignore
 * @api private
 */
var serializeObject = function(object, checkKeys, buffer, index, serializeFunctions) {
  // Process the object
  if(Array.isArray(object)) {
    for(var i = 0; i < object.length; i++) {
      index = packElement(i.toString(), object[i], checkKeys, buffer, index, serializeFunctions);
    }
  } else {
		// If we have toBSON defined, override the current object
		if(object.toBSON) {
			object = object.toBSON();
		}

		// Serialize the object
    for(var key in object) {
      // Check the key and throw error if it's illegal
      if (key != '$db' && key != '$ref' && key != '$id') {
        // dollars and dots ok
        BSON.checkKey(key, !checkKeys);
      }

      // Pack the element
      index = packElement(key, object[key], checkKeys, buffer, index, serializeFunctions);
    }
  }

  // Write zero
  buffer[index++] = 0;
  return index;
}

var stringToBytes = function(str) {
  var ch, st, re = [];
  for (var i = 0; i < str.length; i++ ) {
    ch = str.charCodeAt(i);  // get char
    st = [];                 // set up "stack"
    do {
      st.push( ch & 0xFF );  // push byte to stack
      ch = ch >> 8;          // shift value down by 1 byte
    }
    while ( ch );
    // add stack contents to result
    // done because chars have "wrong" endianness
    re = re.concat( st.reverse() );
  }
  // return an array of bytes
  return re;
}

var numberOfBytes = function(str) {
  var ch, st, re = 0;
  for (var i = 0; i < str.length; i++ ) {
    ch = str.charCodeAt(i);  // get char
    st = [];                 // set up "stack"
    do {
      st.push( ch & 0xFF );  // push byte to stack
      ch = ch >> 8;          // shift value down by 1 byte
    }
    while ( ch );
    // add stack contents to result
    // done because chars have "wrong" endianness
    re = re + st.length;
  }
  // return an array of bytes
  return re;
}

/**
 * @ignore
 * @api private
 */
var writeToTypedArray = function(buffer, string, index) {
  var bytes = stringToBytes(string);
  for(var i = 0; i < bytes.length; i++) {
    buffer[index + i] = bytes[i];
  }
  return bytes.length;
}

/**
 * @ignore
 * @api private
 */
var supportsBuffer = typeof Buffer != 'undefined';

/**
 * @ignore
 * @api private
 */
var packElement = function(name, value, checkKeys, buffer, index, serializeFunctions) {
  var startIndex = index;

  switch(typeof value) {
    case 'string':
      // Encode String type
      buffer[index++] = BSON.BSON_DATA_STRING;
      // Number of written bytes
      var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
      // Encode the name
      index = index + numberOfWrittenBytes + 1;
      buffer[index - 1] = 0;

      // Calculate size
      var size = supportsBuffer ? Buffer.byteLength(value) + 1 : numberOfBytes(value) + 1;
      // Write the size of the string to buffer
      buffer[index + 3] = (size >> 24) & 0xff;
      buffer[index + 2] = (size >> 16) & 0xff;
      buffer[index + 1] = (size >> 8) & 0xff;
      buffer[index] = size & 0xff;
      // Ajust the index
      index = index + 4;
      // Write the string
      supportsBuffer ? buffer.write(value, index, 'utf8') : writeToTypedArray(buffer, value, index);
      // Update index
      index = index + size - 1;
      // Write zero
      buffer[index++] = 0;
      // Return index
      return index;
    case 'number':
      // We have an integer value
      if(Math.floor(value) === value && value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
        // If the value fits in 32 bits encode as int, if it fits in a double
        // encode it as a double, otherwise long
        if(value >= BSON.BSON_INT32_MIN && value <= BSON.BSON_INT32_MAX) {
          // Set int type 32 bits or less
          buffer[index++] = BSON.BSON_DATA_INT;
          // Number of written bytes
          var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
          // Encode the name
          index = index + numberOfWrittenBytes + 1;
          buffer[index - 1] = 0;
          // Write the int value
          buffer[index++] = value & 0xff;
          buffer[index++] = (value >> 8) & 0xff;
          buffer[index++] = (value >> 16) & 0xff;
          buffer[index++] = (value >> 24) & 0xff;
        } else if(value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
          // Encode as double
          buffer[index++] = BSON.BSON_DATA_NUMBER;
          // Number of written bytes
          var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
          // Encode the name
          index = index + numberOfWrittenBytes + 1;
          buffer[index - 1] = 0;
          // Write float
          writeIEEE754(buffer, value, index, 'little', 52, 8);
          // Ajust index
          index = index + 8;
        } else {
          // Set long type
          buffer[index++] = BSON.BSON_DATA_LONG;
          // Number of written bytes
          var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
          // Encode the name
          index = index + numberOfWrittenBytes + 1;
          buffer[index - 1] = 0;
          var longVal = Long.fromNumber(value);
          var lowBits = longVal.getLowBits();
          var highBits = longVal.getHighBits();
          // Encode low bits
          buffer[index++] = lowBits & 0xff;
          buffer[index++] = (lowBits >> 8) & 0xff;
          buffer[index++] = (lowBits >> 16) & 0xff;
          buffer[index++] = (lowBits >> 24) & 0xff;
          // Encode high bits
          buffer[index++] = highBits & 0xff;
          buffer[index++] = (highBits >> 8) & 0xff;
          buffer[index++] = (highBits >> 16) & 0xff;
          buffer[index++] = (highBits >> 24) & 0xff;
        }
      } else {
        // Encode as double
        buffer[index++] = BSON.BSON_DATA_NUMBER;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
        // Write float
        writeIEEE754(buffer, value, index, 'little', 52, 8);
        // Ajust index
        index = index + 8;
      }

      return index;
    case 'undefined':
      // Set long type
      buffer[index++] = BSON.BSON_DATA_NULL;
      // Number of written bytes
      var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
      // Encode the name
      index = index + numberOfWrittenBytes + 1;
      buffer[index - 1] = 0;
      return index;
    case 'boolean':
      // Write the type
      buffer[index++] = BSON.BSON_DATA_BOOLEAN;
      // Number of written bytes
      var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
      // Encode the name
      index = index + numberOfWrittenBytes + 1;
      buffer[index - 1] = 0;
      // Encode the boolean value
      buffer[index++] = value ? 1 : 0;
      return index;
    case 'object':
      if(value === null || value instanceof MinKey || value instanceof MaxKey
          || value['_bsontype'] == 'MinKey' || value['_bsontype'] == 'MaxKey') {
        // Write the type of either min or max key
        if(value === null) {
          buffer[index++] = BSON.BSON_DATA_NULL;
        } else if(value instanceof MinKey) {
          buffer[index++] = BSON.BSON_DATA_MIN_KEY;
        } else {
          buffer[index++] = BSON.BSON_DATA_MAX_KEY;
        }

        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
        return index;
      } else if(value instanceof ObjectID || value['_bsontype'] == 'ObjectID') {
        // Write the type
        buffer[index++] = BSON.BSON_DATA_OID;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;

        // Write objectid
        supportsBuffer ? buffer.write(value.id, index, 'binary') : writeToTypedArray(buffer, value.id, index);
        // Ajust index
        index = index + 12;
        return index;
      } else if(value instanceof Date || isDate(value)) {
        // Write the type
        buffer[index++] = BSON.BSON_DATA_DATE;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;

        // Write the date
        var dateInMilis = Long.fromNumber(value.getTime());
        var lowBits = dateInMilis.getLowBits();
        var highBits = dateInMilis.getHighBits();
        // Encode low bits
        buffer[index++] = lowBits & 0xff;
        buffer[index++] = (lowBits >> 8) & 0xff;
        buffer[index++] = (lowBits >> 16) & 0xff;
        buffer[index++] = (lowBits >> 24) & 0xff;
        // Encode high bits
        buffer[index++] = highBits & 0xff;
        buffer[index++] = (highBits >> 8) & 0xff;
        buffer[index++] = (highBits >> 16) & 0xff;
        buffer[index++] = (highBits >> 24) & 0xff;
        return index;
      } else if(typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
        // Write the type
        buffer[index++] = BSON.BSON_DATA_BINARY;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
        // Get size of the buffer (current write point)
        var size = value.length;
        // Write the size of the string to buffer
        buffer[index++] = size & 0xff;
        buffer[index++] = (size >> 8) & 0xff;
        buffer[index++] = (size >> 16) & 0xff;
        buffer[index++] = (size >> 24) & 0xff;
        // Write the default subtype
        buffer[index++] = BSON.BSON_BINARY_SUBTYPE_DEFAULT;
        // Copy the content form the binary field to the buffer
        value.copy(buffer, index, 0, size);
        // Adjust the index
        index = index + size;
        return index;
      } else if(value instanceof Long || value instanceof Timestamp || value['_bsontype'] == 'Long' || value['_bsontype'] == 'Timestamp') {
        // Write the type
        buffer[index++] = value instanceof Long ? BSON.BSON_DATA_LONG : BSON.BSON_DATA_TIMESTAMP;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
        // Write the date
        var lowBits = value.getLowBits();
        var highBits = value.getHighBits();
        // Encode low bits
        buffer[index++] = lowBits & 0xff;
        buffer[index++] = (lowBits >> 8) & 0xff;
        buffer[index++] = (lowBits >> 16) & 0xff;
        buffer[index++] = (lowBits >> 24) & 0xff;
        // Encode high bits
        buffer[index++] = highBits & 0xff;
        buffer[index++] = (highBits >> 8) & 0xff;
        buffer[index++] = (highBits >> 16) & 0xff;
        buffer[index++] = (highBits >> 24) & 0xff;
        return index;
      } else if(value instanceof Double || value['_bsontype'] == 'Double') {
        // Encode as double
        buffer[index++] = BSON.BSON_DATA_NUMBER;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
        // Write float
        writeIEEE754(buffer, value, index, 'little', 52, 8);
        // Ajust index
        index = index + 8;
        return index;
      } else if(value instanceof Code || value['_bsontype'] == 'Code') {
        if(value.scope != null && Object.keys(value.scope).length > 0) {
          // Write the type
          buffer[index++] = BSON.BSON_DATA_CODE_W_SCOPE;
          // Number of written bytes
          var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
          // Encode the name
          index = index + numberOfWrittenBytes + 1;
          buffer[index - 1] = 0;
          // Calculate the scope size
          var scopeSize = BSON.calculateObjectSize(value.scope, serializeFunctions);
          // Function string
          var functionString = value.code.toString();
          // Function Size
          var codeSize = supportsBuffer ? Buffer.byteLength(functionString) + 1 : numberOfBytes(functionString) + 1;

          // Calculate full size of the object
          var totalSize = 4 + codeSize + scopeSize + 4;

          // Write the total size of the object
          buffer[index++] = totalSize & 0xff;
          buffer[index++] = (totalSize >> 8) & 0xff;
          buffer[index++] = (totalSize >> 16) & 0xff;
          buffer[index++] = (totalSize >> 24) & 0xff;

          // Write the size of the string to buffer
          buffer[index++] = codeSize & 0xff;
          buffer[index++] = (codeSize >> 8) & 0xff;
          buffer[index++] = (codeSize >> 16) & 0xff;
          buffer[index++] = (codeSize >> 24) & 0xff;

          // Write the string
          supportsBuffer ? buffer.write(functionString, index, 'utf8') : writeToTypedArray(buffer, functionString, index);
          // Update index
          index = index + codeSize - 1;
          // Write zero
          buffer[index++] = 0;
          // Serialize the scope object
          var scopeObjectBuffer = supportsBuffer ? new Buffer(scopeSize) : new Uint8Array(new ArrayBuffer(scopeSize));
          // Execute the serialization into a seperate buffer
          serializeObject(value.scope, checkKeys, scopeObjectBuffer, 0, serializeFunctions);

          // Adjusted scope Size (removing the header)
          var scopeDocSize = scopeSize;
          // Write scope object size
          buffer[index++] = scopeDocSize & 0xff;
          buffer[index++] = (scopeDocSize >> 8) & 0xff;
          buffer[index++] = (scopeDocSize >> 16) & 0xff;
          buffer[index++] = (scopeDocSize >> 24) & 0xff;

          // Write the scopeObject into the buffer
          supportsBuffer ? scopeObjectBuffer.copy(buffer, index, 0, scopeSize) : buffer.set(scopeObjectBuffer, index);
          // Adjust index, removing the empty size of the doc (5 bytes 0000000005)
          index = index + scopeDocSize - 5;
          // Write trailing zero
          buffer[index++] = 0;
          return index
        } else {
          buffer[index++] = BSON.BSON_DATA_CODE;
          // Number of written bytes
          var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
          // Encode the name
          index = index + numberOfWrittenBytes + 1;
          buffer[index - 1] = 0;
          // Function string
          var functionString = value.code.toString();
          // Function Size
          var size = supportsBuffer ? Buffer.byteLength(functionString) + 1 : numberOfBytes(functionString) + 1;
          // Write the size of the string to buffer
          buffer[index++] = size & 0xff;
          buffer[index++] = (size >> 8) & 0xff;
          buffer[index++] = (size >> 16) & 0xff;
          buffer[index++] = (size >> 24) & 0xff;
          // Write the string
          supportsBuffer ? buffer.write(functionString, index, 'utf8') : writeToTypedArray(buffer, functionString, index);
          // Update index
          index = index + size - 1;
          // Write zero
          buffer[index++] = 0;
          return index;
        }
      } else if(value instanceof Binary || value['_bsontype'] == 'Binary') {
        // Write the type
        buffer[index++] = BSON.BSON_DATA_BINARY;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
        // Extract the buffer
        var data = value.value(true);
        // Calculate size
        var size = value.position;
        // Write the size of the string to buffer
        buffer[index++] = size & 0xff;
        buffer[index++] = (size >> 8) & 0xff;
        buffer[index++] = (size >> 16) & 0xff;
        buffer[index++] = (size >> 24) & 0xff;
        // Write the subtype to the buffer
        buffer[index++] = value.sub_type;

        // If we have binary type 2 the 4 first bytes are the size
        if(value.sub_type == Binary.SUBTYPE_BYTE_ARRAY) {
          buffer[index++] = size & 0xff;
          buffer[index++] = (size >> 8) & 0xff;
          buffer[index++] = (size >> 16) & 0xff;
          buffer[index++] = (size >> 24) & 0xff;
        }

        // Write the data to the object
        supportsBuffer ? data.copy(buffer, index, 0, value.position) : buffer.set(data, index);
        // Ajust index
        index = index + value.position;
        return index;
      } else if(value instanceof Symbol || value['_bsontype'] == 'Symbol') {
        // Write the type
        buffer[index++] = BSON.BSON_DATA_SYMBOL;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
        // Calculate size
        var size = supportsBuffer ? Buffer.byteLength(value.value) + 1 : numberOfBytes(value.value) + 1;
        // Write the size of the string to buffer
        buffer[index++] = size & 0xff;
        buffer[index++] = (size >> 8) & 0xff;
        buffer[index++] = (size >> 16) & 0xff;
        buffer[index++] = (size >> 24) & 0xff;
        // Write the string
        buffer.write(value.value, index, 'utf8');
        // Update index
        index = index + size - 1;
        // Write zero
        buffer[index++] = 0x00;
        return index;
      } else if(value instanceof DBRef || value['_bsontype'] == 'DBRef') {
        // Write the type
        buffer[index++] = BSON.BSON_DATA_OBJECT;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
        // Set up correct object for serialization
        var ordered_values = {
            '$ref': value.namespace
          , '$id' : value.oid
        };

        // Add db reference if it exists
        if(null != value.db) {
          ordered_values['$db'] = value.db;
        }

        // Message size
        var size = BSON.calculateObjectSize(ordered_values, serializeFunctions);
        // Serialize the object
        var endIndex = BSON.serializeWithBufferAndIndex(ordered_values, checkKeys, buffer, index, serializeFunctions);
        // Write the size of the string to buffer
        buffer[index++] = size & 0xff;
        buffer[index++] = (size >> 8) & 0xff;
        buffer[index++] = (size >> 16) & 0xff;
        buffer[index++] = (size >> 24) & 0xff;
        // Write zero for object
        buffer[endIndex++] = 0x00;
        // Return the end index
        return endIndex;
      } else if(value instanceof RegExp || Object.prototype.toString.call(value) === '[object RegExp]') {
        // Write the type
        buffer[index++] = BSON.BSON_DATA_REGEXP;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;

        // Write the regular expression string
        supportsBuffer ? buffer.write(value.source, index, 'utf8') : writeToTypedArray(buffer, value.source, index);
        // Adjust the index
        index = index + (supportsBuffer ? Buffer.byteLength(value.source) : numberOfBytes(value.source));
        // Write zero
        buffer[index++] = 0x00;
        // Write the parameters
        if(value.global) buffer[index++] = 0x73; // s
        if(value.ignoreCase) buffer[index++] = 0x69; // i
        if(value.multiline) buffer[index++] = 0x6d; // m
        // Add ending zero
        buffer[index++] = 0x00;
        return index;
      } else {
        // Write the type
        buffer[index++] = Array.isArray(value) ? BSON.BSON_DATA_ARRAY : BSON.BSON_DATA_OBJECT;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Adjust the index
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;
	      var endIndex = serializeObject(value, checkKeys, buffer, index + 4, serializeFunctions);
        // Write size
        var size = endIndex - index;
        // Write the size of the string to buffer
        buffer[index++] = size & 0xff;
        buffer[index++] = (size >> 8) & 0xff;
        buffer[index++] = (size >> 16) & 0xff;
        buffer[index++] = (size >> 24) & 0xff;
        return endIndex;
      }
    case 'function':
      // WTF for 0.4.X where typeof /someregexp/ === 'function'
      if(value instanceof RegExp || Object.prototype.toString.call(value) === '[object RegExp]' || String.call(value) == '[object RegExp]') {
        // Write the type
        buffer[index++] = BSON.BSON_DATA_REGEXP;
        // Number of written bytes
        var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
        // Encode the name
        index = index + numberOfWrittenBytes + 1;
        buffer[index - 1] = 0;

        // Write the regular expression string
        buffer.write(value.source, index, 'utf8');
        // Adjust the index
        index = index + (supportsBuffer ? Buffer.byteLength(value.source) : numberOfBytes(value.source));
        // Write zero
        buffer[index++] = 0x00;
        // Write the parameters
        if(value.global) buffer[index++] = 0x73; // s
        if(value.ignoreCase) buffer[index++] = 0x69; // i
        if(value.multiline) buffer[index++] = 0x6d; // m
        // Add ending zero
        buffer[index++] = 0x00;
        return index;
      } else {
        if(serializeFunctions && value.scope != null && Object.keys(value.scope).length > 0) {
          // Write the type
          buffer[index++] = BSON.BSON_DATA_CODE_W_SCOPE;
          // Number of written bytes
          var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
          // Encode the name
          index = index + numberOfWrittenBytes + 1;
          buffer[index - 1] = 0;
          // Calculate the scope size
          var scopeSize = BSON.calculateObjectSize(value.scope, serializeFunctions);
          // Function string
          var functionString = value.toString();
          // Function Size
          var codeSize = supportsBuffer ? Buffer.byteLength(functionString) + 1 : numberOfBytes(functionString) + 1;

          // Calculate full size of the object
          var totalSize = 4 + codeSize + scopeSize;

          // Write the total size of the object
          buffer[index++] = totalSize & 0xff;
          buffer[index++] = (totalSize >> 8) & 0xff;
          buffer[index++] = (totalSize >> 16) & 0xff;
          buffer[index++] = (totalSize >> 24) & 0xff;

          // Write the size of the string to buffer
          buffer[index++] = codeSize & 0xff;
          buffer[index++] = (codeSize >> 8) & 0xff;
          buffer[index++] = (codeSize >> 16) & 0xff;
          buffer[index++] = (codeSize >> 24) & 0xff;

          // Write the string
          supportsBuffer ? buffer.write(functionString, index, 'utf8') : writeToTypedArray(buffer, functionString, index);
          // Update index
          index = index + codeSize - 1;
          // Write zero
          buffer[index++] = 0;
          // Serialize the scope object
          var scopeObjectBuffer = new Buffer(scopeSize);
          // Execute the serialization into a seperate buffer
          serializeObject(value.scope, checkKeys, scopeObjectBuffer, 0, serializeFunctions);

          // Adjusted scope Size (removing the header)
          var scopeDocSize = scopeSize - 4;
          // Write scope object size
          buffer[index++] = scopeDocSize & 0xff;
          buffer[index++] = (scopeDocSize >> 8) & 0xff;
          buffer[index++] = (scopeDocSize >> 16) & 0xff;
          buffer[index++] = (scopeDocSize >> 24) & 0xff;

          // Write the scopeObject into the buffer
          scopeObjectBuffer.copy(buffer, index, 0, scopeSize);

          // Adjust index, removing the empty size of the doc (5 bytes 0000000005)
          index = index + scopeDocSize - 5;
          // Write trailing zero
          buffer[index++] = 0;
          return index
        } else if(serializeFunctions) {
          buffer[index++] = BSON.BSON_DATA_CODE;
          // Number of written bytes
          var numberOfWrittenBytes = supportsBuffer ? buffer.write(name, index, 'utf8') : writeToTypedArray(buffer, name, index);
          // Encode the name
          index = index + numberOfWrittenBytes + 1;
          buffer[index - 1] = 0;
          // Function string
          var functionString = value.toString();
          // Function Size
          var size = supportsBuffer ? Buffer.byteLength(functionString) + 1 : numberOfBytes(functionString) + 1;
          // Write the size of the string to buffer
          buffer[index++] = size & 0xff;
          buffer[index++] = (size >> 8) & 0xff;
          buffer[index++] = (size >> 16) & 0xff;
          buffer[index++] = (size >> 24) & 0xff;
          // Write the string
          supportsBuffer ? buffer.write(functionString, index, 'utf8') : writeToTypedArray(buffer, functionString, index);
          // Update index
          index = index + size - 1;
          // Write zero
          buffer[index++] = 0;
          return index;
        }
      }
  }

  // If no value to serialize
  return index;
}

/**
 * Serialize a Javascript object.
 *
 * @param {Object} object the Javascript object to serialize.
 * @param {Boolean} checkKeys the serializer will check if keys are valid.
 * @param {Boolean} asBuffer return the serialized object as a Buffer object **(ignore)**.
 * @param {Boolean} serializeFunctions serialize the javascript functions **(default:false)**.
 * @return {Buffer} returns the Buffer object containing the serialized object.
 * @api public
 */
BSON.serialize = function(object, checkKeys, asBuffer, serializeFunctions) {
  // Throw error if we are trying serialize an illegal type
  if(object == null || typeof object != 'object' || Array.isArray(object)) 
    throw new Error("Only javascript objects supported");
  
  // Emoty target buffer
  var buffer = null;
  // Calculate the size of the object
  var size = BSON.calculateObjectSize(object, serializeFunctions);
  // Fetch the best available type for storing the binary data
  if(buffer = typeof Buffer != 'undefined') {
    buffer = new Buffer(size);
    asBuffer = true;
  } else if(typeof Uint8Array != 'undefined') {
    buffer = new Uint8Array(new ArrayBuffer(size));
  } else {
    buffer = new Array(size);
  }

  // If asBuffer is false use typed arrays
  BSON.serializeWithBufferAndIndex(object, checkKeys, buffer, 0, serializeFunctions);
  return buffer;
}

/**
 * Contains the function cache if we have that enable to allow for avoiding the eval step on each deserialization, comparison is by md5
 *
 * @ignore
 * @api private
 */
var functionCache = BSON.functionCache = {};

/**
 * Crc state variables shared by function
 *
 * @ignore
 * @api private
 */
var table = [0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3, 0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91, 0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7, 0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B, 0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59, 0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F, 0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D, 0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433, 0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01, 0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E, 0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65, 0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9, 0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F, 0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD, 0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A, 0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683, 0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1, 0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7, 0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5, 0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B, 0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79, 0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236, 0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F, 0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D, 0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713, 0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38, 0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777, 0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45, 0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB, 0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9, 0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF, 0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94, 0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D];

/**
 * CRC32 hash method, Fast and enough versitility for our usage
 *
 * @ignore
 * @api private
 */
var crc32 =  function(string, start, end) {
  var crc = 0
  var x = 0;
  var y = 0;
  crc = crc ^ (-1);

  for(var i = start, iTop = end; i < iTop;i++) {
  	y = (crc ^ string[i]) & 0xFF;
    x = table[y];
  	crc = (crc >>> 8) ^ x;
  }

  return crc ^ (-1);
}

/**
 * Deserialize stream data as BSON documents.
 *
 * Options
 *  - **evalFunctions** {Boolean, default:false}, evaluate functions in the BSON document scoped to the object deserialized.
 *  - **cacheFunctions** {Boolean, default:false}, cache evaluated functions for reuse.
 *  - **cacheFunctionsCrc32** {Boolean, default:false}, use a crc32 code for caching, otherwise use the string of the function.
 *  - **promoteLongs** {Boolean, default:true}, when deserializing a Long will fit it into a Number if it's smaller than 53 bits
 *
 * @param {Buffer} data the buffer containing the serialized set of BSON documents.
 * @param {Number} startIndex the start index in the data Buffer where the deserialization is to start.
 * @param {Number} numberOfDocuments number of documents to deserialize.
 * @param {Array} documents an array where to store the deserialized documents.
 * @param {Number} docStartIndex the index in the documents array from where to start inserting documents.
 * @param {Object} [options] additional options used for the deserialization.
 * @return {Number} returns the next index in the buffer after deserialization **x** numbers of documents.
 * @api public
 */
BSON.deserializeStream = function(data, startIndex, numberOfDocuments, documents, docStartIndex, options) {
  // if(numberOfDocuments !== documents.length) throw new Error("Number of expected results back is less than the number of documents");
  options = options != null ? options : {};
  var index = startIndex;
  // Loop over all documents
  for(var i = 0; i < numberOfDocuments; i++) {
    // Find size of the document
    var size = data[index] | data[index + 1] << 8 | data[index + 2] << 16 | data[index + 3] << 24;
    // Update options with index
    options['index'] = index;
    // Parse the document at this point
    documents[docStartIndex + i] = BSON.deserialize(data, options);
    // Adjust index by the document size
    index = index + size;
  }

  // Return object containing end index of parsing and list of documents
  return index;
}

/**
 * Ensure eval is isolated.
 *
 * @ignore
 * @api private
 */
var isolateEvalWithHash = function(functionCache, hash, functionString, object) {
  // Contains the value we are going to set
  var value = null;

  // Check for cache hit, eval if missing and return cached function
  if(functionCache[hash] == null) {
    eval("value = " + functionString);
    functionCache[hash] = value;
  }
  // Set the object
  return functionCache[hash].bind(object);
}

/**
 * Ensure eval is isolated.
 *
 * @ignore
 * @api private
 */
var isolateEval = function(functionString) {
  // Contains the value we are going to set
  var value = null;
  // Eval the function
  eval("value = " + functionString);
  return value;
}

/**
 * Convert Uint8Array to String
 *
 * @ignore
 * @api private
 */
var convertUint8ArrayToUtf8String = function(byteArray, startIndex, endIndex) {
  return BinaryParser.decode_utf8(convertArraytoUtf8BinaryString(byteArray, startIndex, endIndex));
}

var convertArraytoUtf8BinaryString = function(byteArray, startIndex, endIndex) {
  var result = "";
  for(var i = startIndex; i < endIndex; i++) {
    result = result + String.fromCharCode(byteArray[i]);
  }

  return result;
};

/**
 * Deserialize data as BSON.
 *
 * Options
 *  - **evalFunctions** {Boolean, default:false}, evaluate functions in the BSON document scoped to the object deserialized.
 *  - **cacheFunctions** {Boolean, default:false}, cache evaluated functions for reuse.
 *  - **cacheFunctionsCrc32** {Boolean, default:false}, use a crc32 code for caching, otherwise use the string of the function.
 *  - **promoteLongs** {Boolean, default:true}, when deserializing a Long will fit it into a Number if it's smaller than 53 bits
 *
 * @param {Buffer} buffer the buffer containing the serialized set of BSON documents.
 * @param {Object} [options] additional options used for the deserialization.
 * @param {Boolean} [isArray] ignore used for recursive parsing.
 * @return {Object} returns the deserialized Javascript Object.
 * @api public
 */
BSON.deserialize = function(buffer, options, isArray) {
  // Options
  options = options == null ? {} : options;
  var evalFunctions = options['evalFunctions'] == null ? false : options['evalFunctions'];
  var cacheFunctions = options['cacheFunctions'] == null ? false : options['cacheFunctions'];
  var cacheFunctionsCrc32 = options['cacheFunctionsCrc32'] == null ? false : options['cacheFunctionsCrc32'];
  var promoteLongs = options['promoteLongs'] || true;

  // Validate that we have at least 4 bytes of buffer
  if(buffer.length < 5) throw new Error("corrupt bson message < 5 bytes long");

  // Set up index
  var index = typeof options['index'] == 'number' ? options['index'] : 0;
  // Reads in a C style string
  var readCStyleString = function() {
    // Get the start search index
    var i = index;
    // Locate the end of the c string
    while(buffer[i] !== 0x00) { i++ }
    // Grab utf8 encoded string
    var string = supportsBuffer && Buffer.isBuffer(buffer) ? buffer.toString('utf8', index, i) : convertUint8ArrayToUtf8String(buffer, index, i);
    // Update index position
    index = i + 1;
    // Return string
    return string;
  }

  // Create holding object
  var object = isArray ? [] : {};

  // Read the document size
  var size = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;

  // Ensure buffer is valid size
  if(size < 5 || size > buffer.length) throw new Error("corrupt bson message");

  // While we have more left data left keep parsing
  while(true) {
    // Read the type
    var elementType = buffer[index++];
    // If we get a zero it's the last byte, exit
    if(elementType == 0) break;
    // Read the name of the field
    var name = readCStyleString();
    // Switch on the type
    switch(elementType) {
      case BSON.BSON_DATA_OID:
        var string = supportsBuffer && Buffer.isBuffer(buffer) ? buffer.toString('binary', index, index + 12) : convertArraytoUtf8BinaryString(buffer, index, index + 12);
        // Decode the oid
        object[name] = new ObjectID(string);
        // Update index
        index = index + 12;
        break;
      case BSON.BSON_DATA_STRING:
        // Read the content of the field
        var stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        // Add string to object
        object[name] = supportsBuffer && Buffer.isBuffer(buffer) ? buffer.toString('utf8', index, index + stringSize - 1) : convertUint8ArrayToUtf8String(buffer, index, index + stringSize - 1);
        // Update parse index position
        index = index + stringSize;
        break;
      case BSON.BSON_DATA_INT:
        // Decode the 32bit value
        object[name] = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        break;
      case BSON.BSON_DATA_NUMBER:
        // Decode the double value
        object[name] = readIEEE754(buffer, index, 'little', 52, 8);
        // Update the index
        index = index + 8;
        break;
      case BSON.BSON_DATA_DATE:
        // Unpack the low and high bits
        var lowBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        var highBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        // Set date object
        object[name] = new Date(new Long(lowBits, highBits).toNumber());
        break;
      case BSON.BSON_DATA_BOOLEAN:
        // Parse the boolean value
        object[name] = buffer[index++] == 1;
        break;
      case BSON.BSON_DATA_NULL:
        // Parse the boolean value
        object[name] = null;
        break;
      case BSON.BSON_DATA_BINARY:
        // Decode the size of the binary blob
        var binarySize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        // Decode the subtype
        var subType = buffer[index++];
        // Decode as raw Buffer object if options specifies it
        if(buffer['slice'] != null) {
          // If we have subtype 2 skip the 4 bytes for the size
          if(subType == Binary.SUBTYPE_BYTE_ARRAY) {
            binarySize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
          }
          // Slice the data
          object[name] = new Binary(buffer.slice(index, index + binarySize), subType);
        } else {
          var _buffer = typeof Uint8Array != 'undefined' ? new Uint8Array(new ArrayBuffer(binarySize)) : new Array(binarySize);
          // If we have subtype 2 skip the 4 bytes for the size
          if(subType == Binary.SUBTYPE_BYTE_ARRAY) {
            binarySize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
          }
          // Copy the data
          for(var i = 0; i < binarySize; i++) {
            _buffer[i] = buffer[index + i];
          }
          // Create the binary object
          object[name] = new Binary(_buffer, subType);
        }
        // Update the index
        index = index + binarySize;
        break;
      case BSON.BSON_DATA_ARRAY:
        options['index'] = index;
        // Decode the size of the array document
        var objectSize = buffer[index] | buffer[index + 1] << 8 | buffer[index + 2] << 16 | buffer[index + 3] << 24;
        // Set the array to the object
        object[name] = BSON.deserialize(buffer, options, true);
        // Adjust the index
        index = index + objectSize;
        break;
      case BSON.BSON_DATA_OBJECT:
        options['index'] = index;
        // Decode the size of the object document
        var objectSize = buffer[index] | buffer[index + 1] << 8 | buffer[index + 2] << 16 | buffer[index + 3] << 24;
        // Set the array to the object
        object[name] = BSON.deserialize(buffer, options, false);
        // Adjust the index
        index = index + objectSize;
        break;
      case BSON.BSON_DATA_REGEXP:
        // Create the regexp
        var source = readCStyleString();
        var regExpOptions = readCStyleString();
        // For each option add the corresponding one for javascript
        var optionsArray = new Array(regExpOptions.length);

        // Parse options
        for(var i = 0; i < regExpOptions.length; i++) {
          switch(regExpOptions[i]) {
            case 'm':
              optionsArray[i] = 'm';
              break;
            case 's':
              optionsArray[i] = 'g';
              break;
            case 'i':
              optionsArray[i] = 'i';
              break;
          }
        }

        object[name] = new RegExp(source, optionsArray.join(''));
        break;
      case BSON.BSON_DATA_LONG:
        // Unpack the low and high bits
        var lowBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        var highBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        // Create long object
        var long = new Long(lowBits, highBits); 
        // Promote the long if possible
        if(promoteLongs) {
          object[name] = long.lessThanOrEqual(JS_INT_MAX_LONG) && long.greaterThanOrEqual(JS_INT_MIN_LONG) ? long.toNumber() : long;
        } else {
          object[name] = long;
        }
        break;
      case BSON.BSON_DATA_SYMBOL:
        // Read the content of the field
        var stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        // Add string to object
        object[name] = new Symbol(buffer.toString('utf8', index, index + stringSize - 1));
        // Update parse index position
        index = index + stringSize;
        break;
      case BSON.BSON_DATA_TIMESTAMP:
        // Unpack the low and high bits
        var lowBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        var highBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        // Set the object
        object[name] = new Timestamp(lowBits, highBits);
        break;
      case BSON.BSON_DATA_MIN_KEY:
        // Parse the object
        object[name] = new MinKey();
        break;
      case BSON.BSON_DATA_MAX_KEY:
        // Parse the object
        object[name] = new MaxKey();
        break;
      case BSON.BSON_DATA_CODE:
        // Read the content of the field
        var stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        // Function string
        var functionString = supportsBuffer && Buffer.isBuffer(buffer) ? buffer.toString('utf8', index, index + stringSize - 1) : convertUint8ArrayToUtf8String(buffer, index, index + stringSize - 1);

        // If we are evaluating the functions
        if(evalFunctions) {
          // Contains the value we are going to set
          var value = null;
          // If we have cache enabled let's look for the md5 of the function in the cache
          if(cacheFunctions) {
            var hash = cacheFunctionsCrc32 ? crc32(functionString) : functionString;
            // Got to do this to avoid V8 deoptimizing the call due to finding eval
            object[name] = isolateEvalWithHash(functionCache, hash, functionString, object);
          } else {
            // Set directly
            object[name] = isolateEval(functionString);
          }
        } else {
          object[name]  = new Code(functionString, {});
        }

        // Update parse index position
        index = index + stringSize;
        break;
      case BSON.BSON_DATA_CODE_W_SCOPE:
        // Read the content of the field
        var totalSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        var stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
        // Javascript function
        var functionString = supportsBuffer && Buffer.isBuffer(buffer) ? buffer.toString('utf8', index, index + stringSize - 1) : convertUint8ArrayToUtf8String(buffer, index, index + stringSize - 1);
        // Update parse index position
        index = index + stringSize;
        // Parse the element
        options['index'] = index;
        // Decode the size of the object document
        var objectSize = buffer[index] | buffer[index + 1] << 8 | buffer[index + 2] << 16 | buffer[index + 3] << 24;
        // Decode the scope object
        var scopeObject = BSON.deserialize(buffer, options, false);
        // Adjust the index
        index = index + objectSize;

        // If we are evaluating the functions
        if(evalFunctions) {
          // Contains the value we are going to set
          var value = null;
          // If we have cache enabled let's look for the md5 of the function in the cache
          if(cacheFunctions) {
            var hash = cacheFunctionsCrc32 ? crc32(functionString) : functionString;
            // Got to do this to avoid V8 deoptimizing the call due to finding eval
            object[name] = isolateEvalWithHash(functionCache, hash, functionString, object);
          } else {
            // Set directly
            object[name] = isolateEval(functionString);
          }

          // Set the scope on the object
          object[name].scope = scopeObject;
        } else {
          object[name]  = new Code(functionString, scopeObject);
        }

        // Add string to object
        break;
    }
  }

  // Check if we have a db ref object
  if(object['$id'] != null) object = new DBRef(object['$ref'], object['$id'], object['$db']);

  // Return the final objects
  return object;
}

/**
 * Check if key name is valid.
 *
 * @ignore
 * @api private
 */
BSON.checkKey = function checkKey (key, dollarsAndDotsOk) {
  if (!key.length) return;
  // Check if we have a legal key for the object
  if (!!~key.indexOf("\x00")) {
    // The BSON spec doesn't allow keys with null bytes because keys are
    // null-terminated.
    throw Error("key " + key + " must not contain null bytes");
  }
  if (!dollarsAndDotsOk) {
    if('$' == key[0]) {
      throw Error("key " + key + " must not start with '$'");
    } else if (!!~key.indexOf('.')) {
      throw Error("key " + key + " must not contain '.'");
    }
  }
};

/**
 * Deserialize data as BSON.
 *
 * Options
 *  - **evalFunctions** {Boolean, default:false}, evaluate functions in the BSON document scoped to the object deserialized.
 *  - **cacheFunctions** {Boolean, default:false}, cache evaluated functions for reuse.
 *  - **cacheFunctionsCrc32** {Boolean, default:false}, use a crc32 code for caching, otherwise use the string of the function.
 *
 * @param {Buffer} buffer the buffer containing the serialized set of BSON documents.
 * @param {Object} [options] additional options used for the deserialization.
 * @param {Boolean} [isArray] ignore used for recursive parsing.
 * @return {Object} returns the deserialized Javascript Object.
 * @api public
 */
BSON.prototype.deserialize = function(data, options) {
  return BSON.deserialize(data, options);
}

/**
 * Deserialize stream data as BSON documents.
 *
 * Options
 *  - **evalFunctions** {Boolean, default:false}, evaluate functions in the BSON document scoped to the object deserialized.
 *  - **cacheFunctions** {Boolean, default:false}, cache evaluated functions for reuse.
 *  - **cacheFunctionsCrc32** {Boolean, default:false}, use a crc32 code for caching, otherwise use the string of the function.
 *
 * @param {Buffer} data the buffer containing the serialized set of BSON documents.
 * @param {Number} startIndex the start index in the data Buffer where the deserialization is to start.
 * @param {Number} numberOfDocuments number of documents to deserialize.
 * @param {Array} documents an array where to store the deserialized documents.
 * @param {Number} docStartIndex the index in the documents array from where to start inserting documents.
 * @param {Object} [options] additional options used for the deserialization.
 * @return {Number} returns the next index in the buffer after deserialization **x** numbers of documents.
 * @api public
 */
BSON.prototype.deserializeStream = function(data, startIndex, numberOfDocuments, documents, docStartIndex, options) {
  return BSON.deserializeStream(data, startIndex, numberOfDocuments, documents, docStartIndex, options);
}

/**
 * Serialize a Javascript object.
 *
 * @param {Object} object the Javascript object to serialize.
 * @param {Boolean} checkKeys the serializer will check if keys are valid.
 * @param {Boolean} asBuffer return the serialized object as a Buffer object **(ignore)**.
 * @param {Boolean} serializeFunctions serialize the javascript functions **(default:false)**.
 * @return {Buffer} returns the Buffer object containing the serialized object.
 * @api public
 */
BSON.prototype.serialize = function(object, checkKeys, asBuffer, serializeFunctions) {
  return BSON.serialize(object, checkKeys, asBuffer, serializeFunctions);
}

/**
 * Calculate the bson size for a passed in Javascript object.
 *
 * @param {Object} object the Javascript object to calculate the BSON byte size for.
 * @param {Boolean} [serializeFunctions] serialize all functions in the object **(default:false)**.
 * @return {Number} returns the number of bytes the BSON object will take up.
 * @api public
 */
BSON.prototype.calculateObjectSize = function(object, serializeFunctions) {
  return BSON.calculateObjectSize(object, serializeFunctions);
}

/**
 * Serialize a Javascript object using a predefined Buffer and index into the buffer, useful when pre-allocating the space for serialization.
 *
 * @param {Object} object the Javascript object to serialize.
 * @param {Boolean} checkKeys the serializer will check if keys are valid.
 * @param {Buffer} buffer the Buffer you pre-allocated to store the serialized BSON object.
 * @param {Number} index the index in the buffer where we wish to start serializing into.
 * @param {Boolean} serializeFunctions serialize the javascript functions **(default:false)**.
 * @return {Number} returns the new write index in the Buffer.
 * @api public
 */
BSON.prototype.serializeWithBufferAndIndex = function(object, checkKeys, buffer, startIndex, serializeFunctions) {
  return BSON.serializeWithBufferAndIndex(object, checkKeys, buffer, startIndex, serializeFunctions);
}

/**
 * @ignore
 * @api private
 */
exports.Code = Code;
exports.Symbol = Symbol;
exports.BSON = BSON;
exports.DBRef = DBRef;
exports.Binary = Binary;
exports.ObjectID = ObjectID;
exports.Long = Long;
exports.Timestamp = Timestamp;
exports.Double = Double;
exports.MinKey = MinKey;
exports.MaxKey = MaxKey;

}, 



'code': function(module, exports, global, require, undefined){
  /**
 * A class representation of the BSON Code type.
 *
 * @class Represents the BSON Code type.
 * @param {String|Function} code a string or function.
 * @param {Object} [scope] an optional scope for the function.
 * @return {Code}
 */
function Code(code, scope) {
  if(!(this instanceof Code)) return new Code(code, scope);
  
  this._bsontype = 'Code';
  this.code = code;
  this.scope = scope == null ? {} : scope;
};

/**
 * @ignore
 * @api private
 */
Code.prototype.toJSON = function() {
  return {scope:this.scope, code:this.code};
}

exports.Code = Code;
}, 



'db_ref': function(module, exports, global, require, undefined){
  /**
 * A class representation of the BSON DBRef type.
 *
 * @class Represents the BSON DBRef type.
 * @param {String} namespace the collection name.
 * @param {ObjectID} oid the reference ObjectID.
 * @param {String} [db] optional db name, if omitted the reference is local to the current db.
 * @return {DBRef}
 */
function DBRef(namespace, oid, db) {
  if(!(this instanceof DBRef)) return new DBRef(namespace, oid, db);
  
  this._bsontype = 'DBRef';
  this.namespace = namespace;
  this.oid = oid;
  this.db = db;
};

/**
 * @ignore
 * @api private
 */
DBRef.prototype.toJSON = function() {
  return {
    '$ref':this.namespace,
    '$id':this.oid,
    '$db':this.db == null ? '' : this.db
  };
}

exports.DBRef = DBRef;
}, 



'double': function(module, exports, global, require, undefined){
  /**
 * A class representation of the BSON Double type.
 *
 * @class Represents the BSON Double type.
 * @param {Number} value the number we want to represent as a double.
 * @return {Double}
 */
function Double(value) {
  if(!(this instanceof Double)) return new Double(value);
  
  this._bsontype = 'Double';
  this.value = value;
}

/**
 * Access the number value.
 *
 * @return {Number} returns the wrapped double number.
 * @api public
 */
Double.prototype.valueOf = function() {
  return this.value;
};

/**
 * @ignore
 * @api private
 */
Double.prototype.toJSON = function() {
  return this.value;
}

exports.Double = Double;
}, 



'float_parser': function(module, exports, global, require, undefined){
  // Copyright (c) 2008, Fair Oaks Labs, Inc.
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 
//  * Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 
//  * Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
// 
//  * Neither the name of Fair Oaks Labs, Inc. nor the names of its contributors
//    may be used to endorse or promote products derived from this software
//    without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
//
// Modifications to writeIEEE754 to support negative zeroes made by Brian White

var readIEEE754 = function(buffer, offset, endian, mLen, nBytes) {
  var e, m,
      bBE = (endian === 'big'),
      eLen = nBytes * 8 - mLen - 1,
      eMax = (1 << eLen) - 1,
      eBias = eMax >> 1,
      nBits = -7,
      i = bBE ? 0 : (nBytes - 1),
      d = bBE ? 1 : -1,
      s = buffer[offset + i];

  i += d;

  e = s & ((1 << (-nBits)) - 1);
  s >>= (-nBits);
  nBits += eLen;
  for (; nBits > 0; e = e * 256 + buffer[offset + i], i += d, nBits -= 8);

  m = e & ((1 << (-nBits)) - 1);
  e >>= (-nBits);
  nBits += mLen;
  for (; nBits > 0; m = m * 256 + buffer[offset + i], i += d, nBits -= 8);

  if (e === 0) {
    e = 1 - eBias;
  } else if (e === eMax) {
    return m ? NaN : ((s ? -1 : 1) * Infinity);
  } else {
    m = m + Math.pow(2, mLen);
    e = e - eBias;
  }
  return (s ? -1 : 1) * m * Math.pow(2, e - mLen);
};

var writeIEEE754 = function(buffer, value, offset, endian, mLen, nBytes) {
  var e, m, c,
      bBE = (endian === 'big'),
      eLen = nBytes * 8 - mLen - 1,
      eMax = (1 << eLen) - 1,
      eBias = eMax >> 1,
      rt = (mLen === 23 ? Math.pow(2, -24) - Math.pow(2, -77) : 0),
      i = bBE ? (nBytes-1) : 0,
      d = bBE ? -1 : 1,
      s = value < 0 || (value === 0 && 1 / value < 0) ? 1 : 0;

  value = Math.abs(value);

  if (isNaN(value) || value === Infinity) {
    m = isNaN(value) ? 1 : 0;
    e = eMax;
  } else {
    e = Math.floor(Math.log(value) / Math.LN2);
    if (value * (c = Math.pow(2, -e)) < 1) {
      e--;
      c *= 2;
    }
    if (e+eBias >= 1) {
      value += rt / c;
    } else {
      value += rt * Math.pow(2, 1 - eBias);
    }
    if (value * c >= 2) {
      e++;
      c /= 2;
    }

    if (e + eBias >= eMax) {
      m = 0;
      e = eMax;
    } else if (e + eBias >= 1) {
      m = (value * c - 1) * Math.pow(2, mLen);
      e = e + eBias;
    } else {
      m = value * Math.pow(2, eBias - 1) * Math.pow(2, mLen);
      e = 0;
    }
  }

  for (; mLen >= 8; buffer[offset + i] = m & 0xff, i += d, m /= 256, mLen -= 8);

  e = (e << mLen) | m;
  eLen += mLen;
  for (; eLen > 0; buffer[offset + i] = e & 0xff, i += d, e /= 256, eLen -= 8);

  buffer[offset + i - d] |= s * 128;
};

exports.readIEEE754 = readIEEE754;
exports.writeIEEE754 = writeIEEE754;
}, 



'index': function(module, exports, global, require, undefined){
  try {
  exports.BSONPure = require('./bson');
  exports.BSONNative = require('../../ext');
} catch(err) {
  // do nothing
}

[ './binary_parser'
  , './binary'
  , './code'
  , './db_ref'
  , './double'
  , './max_key'
  , './min_key'
  , './objectid'
  , './symbol'
  , './timestamp'
  , './long'].forEach(function (path) {
  	var module = require('./' + path);
  	for (var i in module) {
  		exports[i] = module[i];
    }
});

// Exports all the classes for the NATIVE JS BSON Parser
exports.native = function() {
  var classes = {};
  // Map all the classes
  [ './binary_parser'
    , './binary'
    , './code'
    , './db_ref'
    , './double'
    , './max_key'
    , './min_key'
    , './objectid'
    , './symbol'
    , './timestamp'
    , './long'
    , '../../ext'
].forEach(function (path) {
    	var module = require('./' + path);
    	for (var i in module) {
    		classes[i] = module[i];
      }
  });
  // Return classes list
  return classes;
}

// Exports all the classes for the PURE JS BSON Parser
exports.pure = function() {
  var classes = {};
  // Map all the classes
  [ './binary_parser'
    , './binary'
    , './code'
    , './db_ref'
    , './double'
    , './max_key'
    , './min_key'
    , './objectid'
    , './symbol'
    , './timestamp'
    , './long'
    , '././bson'].forEach(function (path) {
    	var module = require('./' + path);
    	for (var i in module) {
    		classes[i] = module[i];
      }
  });
  // Return classes list
  return classes;
}

}, 



'long': function(module, exports, global, require, undefined){
  // Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright 2009 Google Inc. All Rights Reserved

/**
 * Defines a Long class for representing a 64-bit two's-complement
 * integer value, which faithfully simulates the behavior of a Java "Long". This
 * implementation is derived from LongLib in GWT.
 *
 * Constructs a 64-bit two's-complement integer, given its low and high 32-bit
 * values as *signed* integers.  See the from* functions below for more
 * convenient ways of constructing Longs.
 *
 * The internal representation of a Long is the two given signed, 32-bit values.
 * We use 32-bit pieces because these are the size of integers on which
 * Javascript performs bit-operations.  For operations like addition and
 * multiplication, we split each number into 16-bit pieces, which can easily be
 * multiplied within Javascript's floating-point representation without overflow
 * or change in sign.
 *
 * In the algorithms below, we frequently reduce the negative case to the
 * positive case by negating the input(s) and then post-processing the result.
 * Note that we must ALWAYS check specially whether those values are MIN_VALUE
 * (-2^63) because -MIN_VALUE == MIN_VALUE (since 2^63 cannot be represented as
 * a positive number, it overflows back into a negative).  Not handling this
 * case would often result in infinite recursion.
 *
 * @class Represents the BSON Long type.
 * @param {Number} low  the low (signed) 32 bits of the Long.
 * @param {Number} high the high (signed) 32 bits of the Long.
 */
function Long(low, high) {
  if(!(this instanceof Long)) return new Long(low, high);
  
  this._bsontype = 'Long';
  /**
   * @type {number}
   * @api private
   */
  this.low_ = low | 0;  // force into 32 signed bits.

  /**
   * @type {number}
   * @api private
   */
  this.high_ = high | 0;  // force into 32 signed bits.
};

/**
 * Return the int value.
 *
 * @return {Number} the value, assuming it is a 32-bit integer.
 * @api public
 */
Long.prototype.toInt = function() {
  return this.low_;
};

/**
 * Return the Number value.
 *
 * @return {Number} the closest floating-point representation to this value.
 * @api public
 */
Long.prototype.toNumber = function() {
  return this.high_ * Long.TWO_PWR_32_DBL_ +
         this.getLowBitsUnsigned();
};

/**
 * Return the JSON value.
 *
 * @return {String} the JSON representation.
 * @api public
 */
Long.prototype.toJSON = function() {
  return this.toString();
}

/**
 * Return the String value.
 *
 * @param {Number} [opt_radix] the radix in which the text should be written.
 * @return {String} the textual representation of this value.
 * @api public
 */
Long.prototype.toString = function(opt_radix) {
  var radix = opt_radix || 10;
  if (radix < 2 || 36 < radix) {
    throw Error('radix out of range: ' + radix);
  }

  if (this.isZero()) {
    return '0';
  }

  if (this.isNegative()) {
    if (this.equals(Long.MIN_VALUE)) {
      // We need to change the Long value before it can be negated, so we remove
      // the bottom-most digit in this base and then recurse to do the rest.
      var radixLong = Long.fromNumber(radix);
      var div = this.div(radixLong);
      var rem = div.multiply(radixLong).subtract(this);
      return div.toString(radix) + rem.toInt().toString(radix);
    } else {
      return '-' + this.negate().toString(radix);
    }
  }

  // Do several (6) digits each time through the loop, so as to
  // minimize the calls to the very expensive emulated div.
  var radixToPower = Long.fromNumber(Math.pow(radix, 6));

  var rem = this;
  var result = '';
  while (true) {
    var remDiv = rem.div(radixToPower);
    var intval = rem.subtract(remDiv.multiply(radixToPower)).toInt();
    var digits = intval.toString(radix);

    rem = remDiv;
    if (rem.isZero()) {
      return digits + result;
    } else {
      while (digits.length < 6) {
        digits = '0' + digits;
      }
      result = '' + digits + result;
    }
  }
};

/**
 * Return the high 32-bits value.
 *
 * @return {Number} the high 32-bits as a signed value.
 * @api public
 */
Long.prototype.getHighBits = function() {
  return this.high_;
};

/**
 * Return the low 32-bits value.
 *
 * @return {Number} the low 32-bits as a signed value.
 * @api public
 */
Long.prototype.getLowBits = function() {
  return this.low_;
};

/**
 * Return the low unsigned 32-bits value.
 *
 * @return {Number} the low 32-bits as an unsigned value.
 * @api public
 */
Long.prototype.getLowBitsUnsigned = function() {
  return (this.low_ >= 0) ?
      this.low_ : Long.TWO_PWR_32_DBL_ + this.low_;
};

/**
 * Returns the number of bits needed to represent the absolute value of this Long.
 *
 * @return {Number} Returns the number of bits needed to represent the absolute value of this Long.
 * @api public
 */
Long.prototype.getNumBitsAbs = function() {
  if (this.isNegative()) {
    if (this.equals(Long.MIN_VALUE)) {
      return 64;
    } else {
      return this.negate().getNumBitsAbs();
    }
  } else {
    var val = this.high_ != 0 ? this.high_ : this.low_;
    for (var bit = 31; bit > 0; bit--) {
      if ((val & (1 << bit)) != 0) {
        break;
      }
    }
    return this.high_ != 0 ? bit + 33 : bit + 1;
  }
};

/**
 * Return whether this value is zero.
 *
 * @return {Boolean} whether this value is zero.
 * @api public
 */
Long.prototype.isZero = function() {
  return this.high_ == 0 && this.low_ == 0;
};

/**
 * Return whether this value is negative.
 *
 * @return {Boolean} whether this value is negative.
 * @api public
 */
Long.prototype.isNegative = function() {
  return this.high_ < 0;
};

/**
 * Return whether this value is odd.
 *
 * @return {Boolean} whether this value is odd.
 * @api public
 */
Long.prototype.isOdd = function() {
  return (this.low_ & 1) == 1;
};

/**
 * Return whether this Long equals the other
 *
 * @param {Long} other Long to compare against.
 * @return {Boolean} whether this Long equals the other
 * @api public
 */
Long.prototype.equals = function(other) {
  return (this.high_ == other.high_) && (this.low_ == other.low_);
};

/**
 * Return whether this Long does not equal the other.
 *
 * @param {Long} other Long to compare against.
 * @return {Boolean} whether this Long does not equal the other.
 * @api public
 */
Long.prototype.notEquals = function(other) {
  return (this.high_ != other.high_) || (this.low_ != other.low_);
};

/**
 * Return whether this Long is less than the other.
 *
 * @param {Long} other Long to compare against.
 * @return {Boolean} whether this Long is less than the other.
 * @api public
 */
Long.prototype.lessThan = function(other) {
  return this.compare(other) < 0;
};

/**
 * Return whether this Long is less than or equal to the other.
 *
 * @param {Long} other Long to compare against.
 * @return {Boolean} whether this Long is less than or equal to the other.
 * @api public
 */
Long.prototype.lessThanOrEqual = function(other) {
  return this.compare(other) <= 0;
};

/**
 * Return whether this Long is greater than the other.
 *
 * @param {Long} other Long to compare against.
 * @return {Boolean} whether this Long is greater than the other.
 * @api public
 */
Long.prototype.greaterThan = function(other) {
  return this.compare(other) > 0;
};

/**
 * Return whether this Long is greater than or equal to the other.
 *
 * @param {Long} other Long to compare against.
 * @return {Boolean} whether this Long is greater than or equal to the other.
 * @api public
 */
Long.prototype.greaterThanOrEqual = function(other) {
  return this.compare(other) >= 0;
};

/**
 * Compares this Long with the given one.
 *
 * @param {Long} other Long to compare against.
 * @return {Boolean} 0 if they are the same, 1 if the this is greater, and -1 if the given one is greater.
 * @api public
 */
Long.prototype.compare = function(other) {
  if (this.equals(other)) {
    return 0;
  }

  var thisNeg = this.isNegative();
  var otherNeg = other.isNegative();
  if (thisNeg && !otherNeg) {
    return -1;
  }
  if (!thisNeg && otherNeg) {
    return 1;
  }

  // at this point, the signs are the same, so subtraction will not overflow
  if (this.subtract(other).isNegative()) {
    return -1;
  } else {
    return 1;
  }
};

/**
 * The negation of this value.
 *
 * @return {Long} the negation of this value.
 * @api public
 */
Long.prototype.negate = function() {
  if (this.equals(Long.MIN_VALUE)) {
    return Long.MIN_VALUE;
  } else {
    return this.not().add(Long.ONE);
  }
};

/**
 * Returns the sum of this and the given Long.
 *
 * @param {Long} other Long to add to this one.
 * @return {Long} the sum of this and the given Long.
 * @api public
 */
Long.prototype.add = function(other) {
  // Divide each number into 4 chunks of 16 bits, and then sum the chunks.

  var a48 = this.high_ >>> 16;
  var a32 = this.high_ & 0xFFFF;
  var a16 = this.low_ >>> 16;
  var a00 = this.low_ & 0xFFFF;

  var b48 = other.high_ >>> 16;
  var b32 = other.high_ & 0xFFFF;
  var b16 = other.low_ >>> 16;
  var b00 = other.low_ & 0xFFFF;

  var c48 = 0, c32 = 0, c16 = 0, c00 = 0;
  c00 += a00 + b00;
  c16 += c00 >>> 16;
  c00 &= 0xFFFF;
  c16 += a16 + b16;
  c32 += c16 >>> 16;
  c16 &= 0xFFFF;
  c32 += a32 + b32;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c48 += a48 + b48;
  c48 &= 0xFFFF;
  return Long.fromBits((c16 << 16) | c00, (c48 << 16) | c32);
};

/**
 * Returns the difference of this and the given Long.
 *
 * @param {Long} other Long to subtract from this.
 * @return {Long} the difference of this and the given Long.
 * @api public
 */
Long.prototype.subtract = function(other) {
  return this.add(other.negate());
};

/**
 * Returns the product of this and the given Long.
 *
 * @param {Long} other Long to multiply with this.
 * @return {Long} the product of this and the other.
 * @api public
 */
Long.prototype.multiply = function(other) {
  if (this.isZero()) {
    return Long.ZERO;
  } else if (other.isZero()) {
    return Long.ZERO;
  }

  if (this.equals(Long.MIN_VALUE)) {
    return other.isOdd() ? Long.MIN_VALUE : Long.ZERO;
  } else if (other.equals(Long.MIN_VALUE)) {
    return this.isOdd() ? Long.MIN_VALUE : Long.ZERO;
  }

  if (this.isNegative()) {
    if (other.isNegative()) {
      return this.negate().multiply(other.negate());
    } else {
      return this.negate().multiply(other).negate();
    }
  } else if (other.isNegative()) {
    return this.multiply(other.negate()).negate();
  }

  // If both Longs are small, use float multiplication
  if (this.lessThan(Long.TWO_PWR_24_) &&
      other.lessThan(Long.TWO_PWR_24_)) {
    return Long.fromNumber(this.toNumber() * other.toNumber());
  }

  // Divide each Long into 4 chunks of 16 bits, and then add up 4x4 products.
  // We can skip products that would overflow.

  var a48 = this.high_ >>> 16;
  var a32 = this.high_ & 0xFFFF;
  var a16 = this.low_ >>> 16;
  var a00 = this.low_ & 0xFFFF;

  var b48 = other.high_ >>> 16;
  var b32 = other.high_ & 0xFFFF;
  var b16 = other.low_ >>> 16;
  var b00 = other.low_ & 0xFFFF;

  var c48 = 0, c32 = 0, c16 = 0, c00 = 0;
  c00 += a00 * b00;
  c16 += c00 >>> 16;
  c00 &= 0xFFFF;
  c16 += a16 * b00;
  c32 += c16 >>> 16;
  c16 &= 0xFFFF;
  c16 += a00 * b16;
  c32 += c16 >>> 16;
  c16 &= 0xFFFF;
  c32 += a32 * b00;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c32 += a16 * b16;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c32 += a00 * b32;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c48 += a48 * b00 + a32 * b16 + a16 * b32 + a00 * b48;
  c48 &= 0xFFFF;
  return Long.fromBits((c16 << 16) | c00, (c48 << 16) | c32);
};

/**
 * Returns this Long divided by the given one.
 *
 * @param {Long} other Long by which to divide.
 * @return {Long} this Long divided by the given one.
 * @api public
 */
Long.prototype.div = function(other) {
  if (other.isZero()) {
    throw Error('division by zero');
  } else if (this.isZero()) {
    return Long.ZERO;
  }

  if (this.equals(Long.MIN_VALUE)) {
    if (other.equals(Long.ONE) ||
        other.equals(Long.NEG_ONE)) {
      return Long.MIN_VALUE;  // recall that -MIN_VALUE == MIN_VALUE
    } else if (other.equals(Long.MIN_VALUE)) {
      return Long.ONE;
    } else {
      // At this point, we have |other| >= 2, so |this/other| < |MIN_VALUE|.
      var halfThis = this.shiftRight(1);
      var approx = halfThis.div(other).shiftLeft(1);
      if (approx.equals(Long.ZERO)) {
        return other.isNegative() ? Long.ONE : Long.NEG_ONE;
      } else {
        var rem = this.subtract(other.multiply(approx));
        var result = approx.add(rem.div(other));
        return result;
      }
    }
  } else if (other.equals(Long.MIN_VALUE)) {
    return Long.ZERO;
  }

  if (this.isNegative()) {
    if (other.isNegative()) {
      return this.negate().div(other.negate());
    } else {
      return this.negate().div(other).negate();
    }
  } else if (other.isNegative()) {
    return this.div(other.negate()).negate();
  }

  // Repeat the following until the remainder is less than other:  find a
  // floating-point that approximates remainder / other *from below*, add this
  // into the result, and subtract it from the remainder.  It is critical that
  // the approximate value is less than or equal to the real value so that the
  // remainder never becomes negative.
  var res = Long.ZERO;
  var rem = this;
  while (rem.greaterThanOrEqual(other)) {
    // Approximate the result of division. This may be a little greater or
    // smaller than the actual value.
    var approx = Math.max(1, Math.floor(rem.toNumber() / other.toNumber()));

    // We will tweak the approximate result by changing it in the 48-th digit or
    // the smallest non-fractional digit, whichever is larger.
    var log2 = Math.ceil(Math.log(approx) / Math.LN2);
    var delta = (log2 <= 48) ? 1 : Math.pow(2, log2 - 48);

    // Decrease the approximation until it is smaller than the remainder.  Note
    // that if it is too large, the product overflows and is negative.
    var approxRes = Long.fromNumber(approx);
    var approxRem = approxRes.multiply(other);
    while (approxRem.isNegative() || approxRem.greaterThan(rem)) {
      approx -= delta;
      approxRes = Long.fromNumber(approx);
      approxRem = approxRes.multiply(other);
    }

    // We know the answer can't be zero... and actually, zero would cause
    // infinite recursion since we would make no progress.
    if (approxRes.isZero()) {
      approxRes = Long.ONE;
    }

    res = res.add(approxRes);
    rem = rem.subtract(approxRem);
  }
  return res;
};

/**
 * Returns this Long modulo the given one.
 *
 * @param {Long} other Long by which to mod.
 * @return {Long} this Long modulo the given one.
 * @api public
 */
Long.prototype.modulo = function(other) {
  return this.subtract(this.div(other).multiply(other));
};

/**
 * The bitwise-NOT of this value.
 *
 * @return {Long} the bitwise-NOT of this value.
 * @api public
 */
Long.prototype.not = function() {
  return Long.fromBits(~this.low_, ~this.high_);
};

/**
 * Returns the bitwise-AND of this Long and the given one.
 *
 * @param {Long} other the Long with which to AND.
 * @return {Long} the bitwise-AND of this and the other.
 * @api public
 */
Long.prototype.and = function(other) {
  return Long.fromBits(this.low_ & other.low_, this.high_ & other.high_);
};

/**
 * Returns the bitwise-OR of this Long and the given one.
 *
 * @param {Long} other the Long with which to OR.
 * @return {Long} the bitwise-OR of this and the other.
 * @api public
 */
Long.prototype.or = function(other) {
  return Long.fromBits(this.low_ | other.low_, this.high_ | other.high_);
};

/**
 * Returns the bitwise-XOR of this Long and the given one.
 *
 * @param {Long} other the Long with which to XOR.
 * @return {Long} the bitwise-XOR of this and the other.
 * @api public
 */
Long.prototype.xor = function(other) {
  return Long.fromBits(this.low_ ^ other.low_, this.high_ ^ other.high_);
};

/**
 * Returns this Long with bits shifted to the left by the given amount.
 *
 * @param {Number} numBits the number of bits by which to shift.
 * @return {Long} this shifted to the left by the given amount.
 * @api public
 */
Long.prototype.shiftLeft = function(numBits) {
  numBits &= 63;
  if (numBits == 0) {
    return this;
  } else {
    var low = this.low_;
    if (numBits < 32) {
      var high = this.high_;
      return Long.fromBits(
                 low << numBits,
                 (high << numBits) | (low >>> (32 - numBits)));
    } else {
      return Long.fromBits(0, low << (numBits - 32));
    }
  }
};

/**
 * Returns this Long with bits shifted to the right by the given amount.
 *
 * @param {Number} numBits the number of bits by which to shift.
 * @return {Long} this shifted to the right by the given amount.
 * @api public
 */
Long.prototype.shiftRight = function(numBits) {
  numBits &= 63;
  if (numBits == 0) {
    return this;
  } else {
    var high = this.high_;
    if (numBits < 32) {
      var low = this.low_;
      return Long.fromBits(
                 (low >>> numBits) | (high << (32 - numBits)),
                 high >> numBits);
    } else {
      return Long.fromBits(
                 high >> (numBits - 32),
                 high >= 0 ? 0 : -1);
    }
  }
};

/**
 * Returns this Long with bits shifted to the right by the given amount, with the new top bits matching the current sign bit.
 *
 * @param {Number} numBits the number of bits by which to shift.
 * @return {Long} this shifted to the right by the given amount, with zeros placed into the new leading bits.
 * @api public
 */
Long.prototype.shiftRightUnsigned = function(numBits) {
  numBits &= 63;
  if (numBits == 0) {
    return this;
  } else {
    var high = this.high_;
    if (numBits < 32) {
      var low = this.low_;
      return Long.fromBits(
                 (low >>> numBits) | (high << (32 - numBits)),
                 high >>> numBits);
    } else if (numBits == 32) {
      return Long.fromBits(high, 0);
    } else {
      return Long.fromBits(high >>> (numBits - 32), 0);
    }
  }
};

/**
 * Returns a Long representing the given (32-bit) integer value.
 *
 * @param {Number} value the 32-bit integer in question.
 * @return {Long} the corresponding Long value.
 * @api public
 */
Long.fromInt = function(value) {
  if (-128 <= value && value < 128) {
    var cachedObj = Long.INT_CACHE_[value];
    if (cachedObj) {
      return cachedObj;
    }
  }

  var obj = new Long(value | 0, value < 0 ? -1 : 0);
  if (-128 <= value && value < 128) {
    Long.INT_CACHE_[value] = obj;
  }
  return obj;
};

/**
 * Returns a Long representing the given value, provided that it is a finite number. Otherwise, zero is returned.
 *
 * @param {Number} value the number in question.
 * @return {Long} the corresponding Long value.
 * @api public
 */
Long.fromNumber = function(value) {
  if (isNaN(value) || !isFinite(value)) {
    return Long.ZERO;
  } else if (value <= -Long.TWO_PWR_63_DBL_) {
    return Long.MIN_VALUE;
  } else if (value + 1 >= Long.TWO_PWR_63_DBL_) {
    return Long.MAX_VALUE;
  } else if (value < 0) {
    return Long.fromNumber(-value).negate();
  } else {
    return new Long(
               (value % Long.TWO_PWR_32_DBL_) | 0,
               (value / Long.TWO_PWR_32_DBL_) | 0);
  }
};

/**
 * Returns a Long representing the 64-bit integer that comes by concatenating the given high and low bits. Each is assumed to use 32 bits.
 *
 * @param {Number} lowBits the low 32-bits.
 * @param {Number} highBits the high 32-bits.
 * @return {Long} the corresponding Long value.
 * @api public
 */
Long.fromBits = function(lowBits, highBits) {
  return new Long(lowBits, highBits);
};

/**
 * Returns a Long representation of the given string, written using the given radix.
 *
 * @param {String} str the textual representation of the Long.
 * @param {Number} opt_radix the radix in which the text is written.
 * @return {Long} the corresponding Long value.
 * @api public
 */
Long.fromString = function(str, opt_radix) {
  if (str.length == 0) {
    throw Error('number format error: empty string');
  }

  var radix = opt_radix || 10;
  if (radix < 2 || 36 < radix) {
    throw Error('radix out of range: ' + radix);
  }

  if (str.charAt(0) == '-') {
    return Long.fromString(str.substring(1), radix).negate();
  } else if (str.indexOf('-') >= 0) {
    throw Error('number format error: interior "-" character: ' + str);
  }

  // Do several (8) digits each time through the loop, so as to
  // minimize the calls to the very expensive emulated div.
  var radixToPower = Long.fromNumber(Math.pow(radix, 8));

  var result = Long.ZERO;
  for (var i = 0; i < str.length; i += 8) {
    var size = Math.min(8, str.length - i);
    var value = parseInt(str.substring(i, i + size), radix);
    if (size < 8) {
      var power = Long.fromNumber(Math.pow(radix, size));
      result = result.multiply(power).add(Long.fromNumber(value));
    } else {
      result = result.multiply(radixToPower);
      result = result.add(Long.fromNumber(value));
    }
  }
  return result;
};

// NOTE: Common constant values ZERO, ONE, NEG_ONE, etc. are defined below the
// from* methods on which they depend.


/**
 * A cache of the Long representations of small integer values.
 * @type {Object}
 * @api private
 */
Long.INT_CACHE_ = {};

// NOTE: the compiler should inline these constant values below and then remove
// these variables, so there should be no runtime penalty for these.

/**
 * Number used repeated below in calculations.  This must appear before the
 * first call to any from* function below.
 * @type {number}
 * @api private
 */
Long.TWO_PWR_16_DBL_ = 1 << 16;

/**
 * @type {number}
 * @api private
 */
Long.TWO_PWR_24_DBL_ = 1 << 24;

/**
 * @type {number}
 * @api private
 */
Long.TWO_PWR_32_DBL_ = Long.TWO_PWR_16_DBL_ * Long.TWO_PWR_16_DBL_;

/**
 * @type {number}
 * @api private
 */
Long.TWO_PWR_31_DBL_ = Long.TWO_PWR_32_DBL_ / 2;

/**
 * @type {number}
 * @api private
 */
Long.TWO_PWR_48_DBL_ = Long.TWO_PWR_32_DBL_ * Long.TWO_PWR_16_DBL_;

/**
 * @type {number}
 * @api private
 */
Long.TWO_PWR_64_DBL_ = Long.TWO_PWR_32_DBL_ * Long.TWO_PWR_32_DBL_;

/**
 * @type {number}
 * @api private
 */
Long.TWO_PWR_63_DBL_ = Long.TWO_PWR_64_DBL_ / 2;

/** @type {Long} */
Long.ZERO = Long.fromInt(0);

/** @type {Long} */
Long.ONE = Long.fromInt(1);

/** @type {Long} */
Long.NEG_ONE = Long.fromInt(-1);

/** @type {Long} */
Long.MAX_VALUE =
    Long.fromBits(0xFFFFFFFF | 0, 0x7FFFFFFF | 0);

/** @type {Long} */
Long.MIN_VALUE = Long.fromBits(0, 0x80000000 | 0);

/**
 * @type {Long}
 * @api private
 */
Long.TWO_PWR_24_ = Long.fromInt(1 << 24);

/**
 * Expose.
 */
exports.Long = Long;
}, 



'max_key': function(module, exports, global, require, undefined){
  /**
 * A class representation of the BSON MaxKey type.
 *
 * @class Represents the BSON MaxKey type.
 * @return {MaxKey}
 */
function MaxKey() {
  if(!(this instanceof MaxKey)) return new MaxKey();
  
  this._bsontype = 'MaxKey';  
}

exports.MaxKey = MaxKey;
}, 



'min_key': function(module, exports, global, require, undefined){
  /**
 * A class representation of the BSON MinKey type.
 *
 * @class Represents the BSON MinKey type.
 * @return {MinKey}
 */
function MinKey() {
  if(!(this instanceof MinKey)) return new MinKey();
  
  this._bsontype = 'MinKey';
}

exports.MinKey = MinKey;
}, 



'objectid': function(module, exports, global, require, undefined){
  /**
 * Module dependencies.
 */
var BinaryParser = require('./binary_parser').BinaryParser;

/**
 * Machine id.
 *
 * Create a random 3-byte value (i.e. unique for this
 * process). Other drivers use a md5 of the machine id here, but
 * that would mean an asyc call to gethostname, so we don't bother.
 */
var MACHINE_ID = parseInt(Math.random() * 0xFFFFFF, 10);

// Regular expression that checks for hex value
var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");

/**
* Create a new ObjectID instance
*
* @class Represents the BSON ObjectID type
* @param {String|Number} id Can be a 24 byte hex string, 12 byte binary string or a Number.
* @return {Object} instance of ObjectID.
*/
var ObjectID = function ObjectID(id, _hex) {
  if(!(this instanceof ObjectID)) return new ObjectID(id, _hex);

  this._bsontype = 'ObjectID';
  var __id = null;

  // Throw an error if it's not a valid setup
  if(id != null && 'number' != typeof id && (id.length != 12 && id.length != 24))
    throw new Error("Argument passed in must be a single String of 12 bytes or a string of 24 hex characters");

  // Generate id based on the input
  if(id == null || typeof id == 'number') {
    // convert to 12 byte binary string
    this.id = this.generate(id);
  } else if(id != null && id.length === 12) {
    // assume 12 byte string
    this.id = id;
  } else if(checkForHexRegExp.test(id)) {
    return ObjectID.createFromHexString(id);
  } else {
    throw new Error("Value passed in is not a valid 24 character hex string");
  }

  if(ObjectID.cacheHexString) this.__id = this.toHexString();
};

// Allow usage of ObjectId as well as ObjectID
var ObjectId = ObjectID;

// Precomputed hex table enables speedy hex string conversion
var hexTable = [];
for (var i = 0; i < 256; i++) {
  hexTable[i] = (i <= 15 ? '0' : '') + i.toString(16);
}

/**
* Return the ObjectID id as a 24 byte hex string representation
*
* @return {String} return the 24 byte hex string representation.
* @api public
*/
ObjectID.prototype.toHexString = function() {
  if(ObjectID.cacheHexString && this.__id) return this.__id;

  var hexString = '';

  for (var i = 0; i < this.id.length; i++) {
    hexString += hexTable[this.id.charCodeAt(i)];
  }

  if(ObjectID.cacheHexString) this.__id = hexString;
  return hexString;
};

/**
* Update the ObjectID index used in generating new ObjectID's on the driver
*
* @return {Number} returns next index value.
* @api private
*/
ObjectID.prototype.get_inc = function() {
  return ObjectID.index = (ObjectID.index + 1) % 0xFFFFFF;
};

/**
* Update the ObjectID index used in generating new ObjectID's on the driver
*
* @return {Number} returns next index value.
* @api private
*/
ObjectID.prototype.getInc = function() {
  return this.get_inc();
};

/**
* Generate a 12 byte id string used in ObjectID's
*
* @param {Number} [time] optional parameter allowing to pass in a second based timestamp.
* @return {String} return the 12 byte id binary string.
* @api private
*/
ObjectID.prototype.generate = function(time) {
  if ('number' == typeof time) {
    var time4Bytes = BinaryParser.encodeInt(time, 32, true, true);
    /* for time-based ObjectID the bytes following the time will be zeroed */
    var machine3Bytes = BinaryParser.encodeInt(MACHINE_ID, 24, false);
    var pid2Bytes = BinaryParser.fromShort(typeof process === 'undefined' ? Math.floor(Math.random() * 100000) : process.pid);
    var index3Bytes = BinaryParser.encodeInt(this.get_inc(), 24, false, true);
  } else {
  	var unixTime = parseInt(Date.now()/1000,10);
    var time4Bytes = BinaryParser.encodeInt(unixTime, 32, true, true);
    var machine3Bytes = BinaryParser.encodeInt(MACHINE_ID, 24, false);
    var pid2Bytes = BinaryParser.fromShort(typeof process === 'undefined' ? Math.floor(Math.random() * 100000) : process.pid);
    var index3Bytes = BinaryParser.encodeInt(this.get_inc(), 24, false, true);
  }

  return time4Bytes + machine3Bytes + pid2Bytes + index3Bytes;
};

/**
* Converts the id into a 24 byte hex string for printing
*
* @return {String} return the 24 byte hex string representation.
* @api private
*/
ObjectID.prototype.toString = function() {
  return this.toHexString();
};

/**
* Converts to a string representation of this Id.
*
* @return {String} return the 24 byte hex string representation.
* @api private
*/
ObjectID.prototype.inspect = ObjectID.prototype.toString;

/**
* Converts to its JSON representation.
*
* @return {String} return the 24 byte hex string representation.
* @api private
*/
ObjectID.prototype.toJSON = function() {
  return this.toHexString();
};

/**
* Compares the equality of this ObjectID with `otherID`.
*
* @param {Object} otherID ObjectID instance to compare against.
* @return {Bool} the result of comparing two ObjectID's
* @api public
*/
ObjectID.prototype.equals = function equals (otherID) {
  var id = (otherID instanceof ObjectID || otherID.toHexString)
    ? otherID.id
    : ObjectID.createFromHexString(otherID).id;

  return this.id === id;
}

/**
* Returns the generation date (accurate up to the second) that this ID was generated.
*
* @return {Date} the generation date
* @api public
*/
ObjectID.prototype.getTimestamp = function() {
  var timestamp = new Date();
  timestamp.setTime(Math.floor(BinaryParser.decodeInt(this.id.substring(0,4), 32, true, true)) * 1000);
  return timestamp;
}

/**
* @ignore
* @api private
*/
ObjectID.index = parseInt(Math.random() * 0xFFFFFF, 10);

ObjectID.createPk = function createPk () {
  return new ObjectID();
};

/**
* Creates an ObjectID from a second based number, with the rest of the ObjectID zeroed out. Used for comparisons or sorting the ObjectID.
*
* @param {Number} time an integer number representing a number of seconds.
* @return {ObjectID} return the created ObjectID
* @api public
*/
ObjectID.createFromTime = function createFromTime (time) {
  var id = BinaryParser.encodeInt(time, 32, true, true) +
           BinaryParser.encodeInt(0, 64, true, true);
  return new ObjectID(id);
};

/**
* Creates an ObjectID from a hex string representation of an ObjectID.
*
* @param {String} hexString create a ObjectID from a passed in 24 byte hexstring.
* @return {ObjectID} return the created ObjectID
* @api public
*/
ObjectID.createFromHexString = function createFromHexString (hexString) {
  // Throw an error if it's not a valid setup
  if(typeof hexString === 'undefined' || hexString != null && hexString.length != 24)
    throw new Error("Argument passed in must be a single String of 12 bytes or a string of 24 hex characters");

  var len = hexString.length;

  if(len > 12*2) {
    throw new Error('Id cannot be longer than 12 bytes');
  }

  var result = ''
    , string
    , number;

  for (var index = 0; index < len; index += 2) {
    string = hexString.substr(index, 2);
    number = parseInt(string, 16);
    result += BinaryParser.fromByte(number);
  }

  return new ObjectID(result, hexString);
};

/**
* @ignore
*/
Object.defineProperty(ObjectID.prototype, "generationTime", {
   enumerable: true
 , get: function () {
     return Math.floor(BinaryParser.decodeInt(this.id.substring(0,4), 32, true, true));
   }
 , set: function (value) {
     var value = BinaryParser.encodeInt(value, 32, true, true);
     this.id = value + this.id.substr(4);
     // delete this.__id;
     this.toHexString();
   }
});

/**
 * Expose.
 */
exports.ObjectID = ObjectID;
exports.ObjectId = ObjectID;

}, 



'symbol': function(module, exports, global, require, undefined){
  /**
 * A class representation of the BSON Symbol type.
 *
 * @class Represents the BSON Symbol type.
 * @param {String} value the string representing the symbol.
 * @return {Symbol}
 */
function Symbol(value) {
  if(!(this instanceof Symbol)) return new Symbol(value);
  this._bsontype = 'Symbol';
  this.value = value;
}

/**
 * Access the wrapped string value.
 *
 * @return {String} returns the wrapped string.
 * @api public
 */
Symbol.prototype.valueOf = function() {
  return this.value;
};

/**
 * @ignore
 * @api private
 */
Symbol.prototype.toString = function() {
  return this.value;
}

/**
 * @ignore
 * @api private
 */
Symbol.prototype.inspect = function() {
  return this.value;
}

/**
 * @ignore
 * @api private
 */
Symbol.prototype.toJSON = function() {
  return this.value;
}

exports.Symbol = Symbol;
}, 



'timestamp': function(module, exports, global, require, undefined){
  // Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright 2009 Google Inc. All Rights Reserved

/**
 * Defines a Timestamp class for representing a 64-bit two's-complement
 * integer value, which faithfully simulates the behavior of a Java "Timestamp". This
 * implementation is derived from TimestampLib in GWT.
 *
 * Constructs a 64-bit two's-complement integer, given its low and high 32-bit
 * values as *signed* integers.  See the from* functions below for more
 * convenient ways of constructing Timestamps.
 *
 * The internal representation of a Timestamp is the two given signed, 32-bit values.
 * We use 32-bit pieces because these are the size of integers on which
 * Javascript performs bit-operations.  For operations like addition and
 * multiplication, we split each number into 16-bit pieces, which can easily be
 * multiplied within Javascript's floating-point representation without overflow
 * or change in sign.
 *
 * In the algorithms below, we frequently reduce the negative case to the
 * positive case by negating the input(s) and then post-processing the result.
 * Note that we must ALWAYS check specially whether those values are MIN_VALUE
 * (-2^63) because -MIN_VALUE == MIN_VALUE (since 2^63 cannot be represented as
 * a positive number, it overflows back into a negative).  Not handling this
 * case would often result in infinite recursion.
 *
 * @class Represents the BSON Timestamp type.
 * @param {Number} low  the low (signed) 32 bits of the Timestamp.
 * @param {Number} high the high (signed) 32 bits of the Timestamp.
 */
function Timestamp(low, high) {
  if(!(this instanceof Timestamp)) return new Timestamp(low, high);
  this._bsontype = 'Timestamp';
  /**
   * @type {number}
   * @api private
   */
  this.low_ = low | 0;  // force into 32 signed bits.

  /**
   * @type {number}
   * @api private
   */
  this.high_ = high | 0;  // force into 32 signed bits.
};

/**
 * Return the int value.
 *
 * @return {Number} the value, assuming it is a 32-bit integer.
 * @api public
 */
Timestamp.prototype.toInt = function() {
  return this.low_;
};

/**
 * Return the Number value.
 *
 * @return {Number} the closest floating-point representation to this value.
 * @api public
 */
Timestamp.prototype.toNumber = function() {
  return this.high_ * Timestamp.TWO_PWR_32_DBL_ +
         this.getLowBitsUnsigned();
};

/**
 * Return the JSON value.
 *
 * @return {String} the JSON representation.
 * @api public
 */
Timestamp.prototype.toJSON = function() {
  return this.toString();
}

/**
 * Return the String value.
 *
 * @param {Number} [opt_radix] the radix in which the text should be written.
 * @return {String} the textual representation of this value.
 * @api public
 */
Timestamp.prototype.toString = function(opt_radix) {
  var radix = opt_radix || 10;
  if (radix < 2 || 36 < radix) {
    throw Error('radix out of range: ' + radix);
  }

  if (this.isZero()) {
    return '0';
  }

  if (this.isNegative()) {
    if (this.equals(Timestamp.MIN_VALUE)) {
      // We need to change the Timestamp value before it can be negated, so we remove
      // the bottom-most digit in this base and then recurse to do the rest.
      var radixTimestamp = Timestamp.fromNumber(radix);
      var div = this.div(radixTimestamp);
      var rem = div.multiply(radixTimestamp).subtract(this);
      return div.toString(radix) + rem.toInt().toString(radix);
    } else {
      return '-' + this.negate().toString(radix);
    }
  }

  // Do several (6) digits each time through the loop, so as to
  // minimize the calls to the very expensive emulated div.
  var radixToPower = Timestamp.fromNumber(Math.pow(radix, 6));

  var rem = this;
  var result = '';
  while (true) {
    var remDiv = rem.div(radixToPower);
    var intval = rem.subtract(remDiv.multiply(radixToPower)).toInt();
    var digits = intval.toString(radix);

    rem = remDiv;
    if (rem.isZero()) {
      return digits + result;
    } else {
      while (digits.length < 6) {
        digits = '0' + digits;
      }
      result = '' + digits + result;
    }
  }
};

/**
 * Return the high 32-bits value.
 *
 * @return {Number} the high 32-bits as a signed value.
 * @api public
 */
Timestamp.prototype.getHighBits = function() {
  return this.high_;
};

/**
 * Return the low 32-bits value.
 *
 * @return {Number} the low 32-bits as a signed value.
 * @api public
 */
Timestamp.prototype.getLowBits = function() {
  return this.low_;
};

/**
 * Return the low unsigned 32-bits value.
 *
 * @return {Number} the low 32-bits as an unsigned value.
 * @api public
 */
Timestamp.prototype.getLowBitsUnsigned = function() {
  return (this.low_ >= 0) ?
      this.low_ : Timestamp.TWO_PWR_32_DBL_ + this.low_;
};

/**
 * Returns the number of bits needed to represent the absolute value of this Timestamp.
 *
 * @return {Number} Returns the number of bits needed to represent the absolute value of this Timestamp.
 * @api public
 */
Timestamp.prototype.getNumBitsAbs = function() {
  if (this.isNegative()) {
    if (this.equals(Timestamp.MIN_VALUE)) {
      return 64;
    } else {
      return this.negate().getNumBitsAbs();
    }
  } else {
    var val = this.high_ != 0 ? this.high_ : this.low_;
    for (var bit = 31; bit > 0; bit--) {
      if ((val & (1 << bit)) != 0) {
        break;
      }
    }
    return this.high_ != 0 ? bit + 33 : bit + 1;
  }
};

/**
 * Return whether this value is zero.
 *
 * @return {Boolean} whether this value is zero.
 * @api public
 */
Timestamp.prototype.isZero = function() {
  return this.high_ == 0 && this.low_ == 0;
};

/**
 * Return whether this value is negative.
 *
 * @return {Boolean} whether this value is negative.
 * @api public
 */
Timestamp.prototype.isNegative = function() {
  return this.high_ < 0;
};

/**
 * Return whether this value is odd.
 *
 * @return {Boolean} whether this value is odd.
 * @api public
 */
Timestamp.prototype.isOdd = function() {
  return (this.low_ & 1) == 1;
};

/**
 * Return whether this Timestamp equals the other
 *
 * @param {Timestamp} other Timestamp to compare against.
 * @return {Boolean} whether this Timestamp equals the other
 * @api public
 */
Timestamp.prototype.equals = function(other) {
  return (this.high_ == other.high_) && (this.low_ == other.low_);
};

/**
 * Return whether this Timestamp does not equal the other.
 *
 * @param {Timestamp} other Timestamp to compare against.
 * @return {Boolean} whether this Timestamp does not equal the other.
 * @api public
 */
Timestamp.prototype.notEquals = function(other) {
  return (this.high_ != other.high_) || (this.low_ != other.low_);
};

/**
 * Return whether this Timestamp is less than the other.
 *
 * @param {Timestamp} other Timestamp to compare against.
 * @return {Boolean} whether this Timestamp is less than the other.
 * @api public
 */
Timestamp.prototype.lessThan = function(other) {
  return this.compare(other) < 0;
};

/**
 * Return whether this Timestamp is less than or equal to the other.
 *
 * @param {Timestamp} other Timestamp to compare against.
 * @return {Boolean} whether this Timestamp is less than or equal to the other.
 * @api public
 */
Timestamp.prototype.lessThanOrEqual = function(other) {
  return this.compare(other) <= 0;
};

/**
 * Return whether this Timestamp is greater than the other.
 *
 * @param {Timestamp} other Timestamp to compare against.
 * @return {Boolean} whether this Timestamp is greater than the other.
 * @api public
 */
Timestamp.prototype.greaterThan = function(other) {
  return this.compare(other) > 0;
};

/**
 * Return whether this Timestamp is greater than or equal to the other.
 *
 * @param {Timestamp} other Timestamp to compare against.
 * @return {Boolean} whether this Timestamp is greater than or equal to the other.
 * @api public
 */
Timestamp.prototype.greaterThanOrEqual = function(other) {
  return this.compare(other) >= 0;
};

/**
 * Compares this Timestamp with the given one.
 *
 * @param {Timestamp} other Timestamp to compare against.
 * @return {Boolean} 0 if they are the same, 1 if the this is greater, and -1 if the given one is greater.
 * @api public
 */
Timestamp.prototype.compare = function(other) {
  if (this.equals(other)) {
    return 0;
  }

  var thisNeg = this.isNegative();
  var otherNeg = other.isNegative();
  if (thisNeg && !otherNeg) {
    return -1;
  }
  if (!thisNeg && otherNeg) {
    return 1;
  }

  // at this point, the signs are the same, so subtraction will not overflow
  if (this.subtract(other).isNegative()) {
    return -1;
  } else {
    return 1;
  }
};

/**
 * The negation of this value.
 *
 * @return {Timestamp} the negation of this value.
 * @api public
 */
Timestamp.prototype.negate = function() {
  if (this.equals(Timestamp.MIN_VALUE)) {
    return Timestamp.MIN_VALUE;
  } else {
    return this.not().add(Timestamp.ONE);
  }
};

/**
 * Returns the sum of this and the given Timestamp.
 *
 * @param {Timestamp} other Timestamp to add to this one.
 * @return {Timestamp} the sum of this and the given Timestamp.
 * @api public
 */
Timestamp.prototype.add = function(other) {
  // Divide each number into 4 chunks of 16 bits, and then sum the chunks.

  var a48 = this.high_ >>> 16;
  var a32 = this.high_ & 0xFFFF;
  var a16 = this.low_ >>> 16;
  var a00 = this.low_ & 0xFFFF;

  var b48 = other.high_ >>> 16;
  var b32 = other.high_ & 0xFFFF;
  var b16 = other.low_ >>> 16;
  var b00 = other.low_ & 0xFFFF;

  var c48 = 0, c32 = 0, c16 = 0, c00 = 0;
  c00 += a00 + b00;
  c16 += c00 >>> 16;
  c00 &= 0xFFFF;
  c16 += a16 + b16;
  c32 += c16 >>> 16;
  c16 &= 0xFFFF;
  c32 += a32 + b32;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c48 += a48 + b48;
  c48 &= 0xFFFF;
  return Timestamp.fromBits((c16 << 16) | c00, (c48 << 16) | c32);
};

/**
 * Returns the difference of this and the given Timestamp.
 *
 * @param {Timestamp} other Timestamp to subtract from this.
 * @return {Timestamp} the difference of this and the given Timestamp.
 * @api public
 */
Timestamp.prototype.subtract = function(other) {
  return this.add(other.negate());
};

/**
 * Returns the product of this and the given Timestamp.
 *
 * @param {Timestamp} other Timestamp to multiply with this.
 * @return {Timestamp} the product of this and the other.
 * @api public
 */
Timestamp.prototype.multiply = function(other) {
  if (this.isZero()) {
    return Timestamp.ZERO;
  } else if (other.isZero()) {
    return Timestamp.ZERO;
  }

  if (this.equals(Timestamp.MIN_VALUE)) {
    return other.isOdd() ? Timestamp.MIN_VALUE : Timestamp.ZERO;
  } else if (other.equals(Timestamp.MIN_VALUE)) {
    return this.isOdd() ? Timestamp.MIN_VALUE : Timestamp.ZERO;
  }

  if (this.isNegative()) {
    if (other.isNegative()) {
      return this.negate().multiply(other.negate());
    } else {
      return this.negate().multiply(other).negate();
    }
  } else if (other.isNegative()) {
    return this.multiply(other.negate()).negate();
  }

  // If both Timestamps are small, use float multiplication
  if (this.lessThan(Timestamp.TWO_PWR_24_) &&
      other.lessThan(Timestamp.TWO_PWR_24_)) {
    return Timestamp.fromNumber(this.toNumber() * other.toNumber());
  }

  // Divide each Timestamp into 4 chunks of 16 bits, and then add up 4x4 products.
  // We can skip products that would overflow.

  var a48 = this.high_ >>> 16;
  var a32 = this.high_ & 0xFFFF;
  var a16 = this.low_ >>> 16;
  var a00 = this.low_ & 0xFFFF;

  var b48 = other.high_ >>> 16;
  var b32 = other.high_ & 0xFFFF;
  var b16 = other.low_ >>> 16;
  var b00 = other.low_ & 0xFFFF;

  var c48 = 0, c32 = 0, c16 = 0, c00 = 0;
  c00 += a00 * b00;
  c16 += c00 >>> 16;
  c00 &= 0xFFFF;
  c16 += a16 * b00;
  c32 += c16 >>> 16;
  c16 &= 0xFFFF;
  c16 += a00 * b16;
  c32 += c16 >>> 16;
  c16 &= 0xFFFF;
  c32 += a32 * b00;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c32 += a16 * b16;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c32 += a00 * b32;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c48 += a48 * b00 + a32 * b16 + a16 * b32 + a00 * b48;
  c48 &= 0xFFFF;
  return Timestamp.fromBits((c16 << 16) | c00, (c48 << 16) | c32);
};

/**
 * Returns this Timestamp divided by the given one.
 *
 * @param {Timestamp} other Timestamp by which to divide.
 * @return {Timestamp} this Timestamp divided by the given one.
 * @api public
 */
Timestamp.prototype.div = function(other) {
  if (other.isZero()) {
    throw Error('division by zero');
  } else if (this.isZero()) {
    return Timestamp.ZERO;
  }

  if (this.equals(Timestamp.MIN_VALUE)) {
    if (other.equals(Timestamp.ONE) ||
        other.equals(Timestamp.NEG_ONE)) {
      return Timestamp.MIN_VALUE;  // recall that -MIN_VALUE == MIN_VALUE
    } else if (other.equals(Timestamp.MIN_VALUE)) {
      return Timestamp.ONE;
    } else {
      // At this point, we have |other| >= 2, so |this/other| < |MIN_VALUE|.
      var halfThis = this.shiftRight(1);
      var approx = halfThis.div(other).shiftLeft(1);
      if (approx.equals(Timestamp.ZERO)) {
        return other.isNegative() ? Timestamp.ONE : Timestamp.NEG_ONE;
      } else {
        var rem = this.subtract(other.multiply(approx));
        var result = approx.add(rem.div(other));
        return result;
      }
    }
  } else if (other.equals(Timestamp.MIN_VALUE)) {
    return Timestamp.ZERO;
  }

  if (this.isNegative()) {
    if (other.isNegative()) {
      return this.negate().div(other.negate());
    } else {
      return this.negate().div(other).negate();
    }
  } else if (other.isNegative()) {
    return this.div(other.negate()).negate();
  }

  // Repeat the following until the remainder is less than other:  find a
  // floating-point that approximates remainder / other *from below*, add this
  // into the result, and subtract it from the remainder.  It is critical that
  // the approximate value is less than or equal to the real value so that the
  // remainder never becomes negative.
  var res = Timestamp.ZERO;
  var rem = this;
  while (rem.greaterThanOrEqual(other)) {
    // Approximate the result of division. This may be a little greater or
    // smaller than the actual value.
    var approx = Math.max(1, Math.floor(rem.toNumber() / other.toNumber()));

    // We will tweak the approximate result by changing it in the 48-th digit or
    // the smallest non-fractional digit, whichever is larger.
    var log2 = Math.ceil(Math.log(approx) / Math.LN2);
    var delta = (log2 <= 48) ? 1 : Math.pow(2, log2 - 48);

    // Decrease the approximation until it is smaller than the remainder.  Note
    // that if it is too large, the product overflows and is negative.
    var approxRes = Timestamp.fromNumber(approx);
    var approxRem = approxRes.multiply(other);
    while (approxRem.isNegative() || approxRem.greaterThan(rem)) {
      approx -= delta;
      approxRes = Timestamp.fromNumber(approx);
      approxRem = approxRes.multiply(other);
    }

    // We know the answer can't be zero... and actually, zero would cause
    // infinite recursion since we would make no progress.
    if (approxRes.isZero()) {
      approxRes = Timestamp.ONE;
    }

    res = res.add(approxRes);
    rem = rem.subtract(approxRem);
  }
  return res;
};

/**
 * Returns this Timestamp modulo the given one.
 *
 * @param {Timestamp} other Timestamp by which to mod.
 * @return {Timestamp} this Timestamp modulo the given one.
 * @api public
 */
Timestamp.prototype.modulo = function(other) {
  return this.subtract(this.div(other).multiply(other));
};

/**
 * The bitwise-NOT of this value.
 *
 * @return {Timestamp} the bitwise-NOT of this value.
 * @api public
 */
Timestamp.prototype.not = function() {
  return Timestamp.fromBits(~this.low_, ~this.high_);
};

/**
 * Returns the bitwise-AND of this Timestamp and the given one.
 *
 * @param {Timestamp} other the Timestamp with which to AND.
 * @return {Timestamp} the bitwise-AND of this and the other.
 * @api public
 */
Timestamp.prototype.and = function(other) {
  return Timestamp.fromBits(this.low_ & other.low_, this.high_ & other.high_);
};

/**
 * Returns the bitwise-OR of this Timestamp and the given one.
 *
 * @param {Timestamp} other the Timestamp with which to OR.
 * @return {Timestamp} the bitwise-OR of this and the other.
 * @api public
 */
Timestamp.prototype.or = function(other) {
  return Timestamp.fromBits(this.low_ | other.low_, this.high_ | other.high_);
};

/**
 * Returns the bitwise-XOR of this Timestamp and the given one.
 *
 * @param {Timestamp} other the Timestamp with which to XOR.
 * @return {Timestamp} the bitwise-XOR of this and the other.
 * @api public
 */
Timestamp.prototype.xor = function(other) {
  return Timestamp.fromBits(this.low_ ^ other.low_, this.high_ ^ other.high_);
};

/**
 * Returns this Timestamp with bits shifted to the left by the given amount.
 *
 * @param {Number} numBits the number of bits by which to shift.
 * @return {Timestamp} this shifted to the left by the given amount.
 * @api public
 */
Timestamp.prototype.shiftLeft = function(numBits) {
  numBits &= 63;
  if (numBits == 0) {
    return this;
  } else {
    var low = this.low_;
    if (numBits < 32) {
      var high = this.high_;
      return Timestamp.fromBits(
                 low << numBits,
                 (high << numBits) | (low >>> (32 - numBits)));
    } else {
      return Timestamp.fromBits(0, low << (numBits - 32));
    }
  }
};

/**
 * Returns this Timestamp with bits shifted to the right by the given amount.
 *
 * @param {Number} numBits the number of bits by which to shift.
 * @return {Timestamp} this shifted to the right by the given amount.
 * @api public
 */
Timestamp.prototype.shiftRight = function(numBits) {
  numBits &= 63;
  if (numBits == 0) {
    return this;
  } else {
    var high = this.high_;
    if (numBits < 32) {
      var low = this.low_;
      return Timestamp.fromBits(
                 (low >>> numBits) | (high << (32 - numBits)),
                 high >> numBits);
    } else {
      return Timestamp.fromBits(
                 high >> (numBits - 32),
                 high >= 0 ? 0 : -1);
    }
  }
};

/**
 * Returns this Timestamp with bits shifted to the right by the given amount, with the new top bits matching the current sign bit.
 *
 * @param {Number} numBits the number of bits by which to shift.
 * @return {Timestamp} this shifted to the right by the given amount, with zeros placed into the new leading bits.
 * @api public
 */
Timestamp.prototype.shiftRightUnsigned = function(numBits) {
  numBits &= 63;
  if (numBits == 0) {
    return this;
  } else {
    var high = this.high_;
    if (numBits < 32) {
      var low = this.low_;
      return Timestamp.fromBits(
                 (low >>> numBits) | (high << (32 - numBits)),
                 high >>> numBits);
    } else if (numBits == 32) {
      return Timestamp.fromBits(high, 0);
    } else {
      return Timestamp.fromBits(high >>> (numBits - 32), 0);
    }
  }
};

/**
 * Returns a Timestamp representing the given (32-bit) integer value.
 *
 * @param {Number} value the 32-bit integer in question.
 * @return {Timestamp} the corresponding Timestamp value.
 * @api public
 */
Timestamp.fromInt = function(value) {
  if (-128 <= value && value < 128) {
    var cachedObj = Timestamp.INT_CACHE_[value];
    if (cachedObj) {
      return cachedObj;
    }
  }

  var obj = new Timestamp(value | 0, value < 0 ? -1 : 0);
  if (-128 <= value && value < 128) {
    Timestamp.INT_CACHE_[value] = obj;
  }
  return obj;
};

/**
 * Returns a Timestamp representing the given value, provided that it is a finite number. Otherwise, zero is returned.
 *
 * @param {Number} value the number in question.
 * @return {Timestamp} the corresponding Timestamp value.
 * @api public
 */
Timestamp.fromNumber = function(value) {
  if (isNaN(value) || !isFinite(value)) {
    return Timestamp.ZERO;
  } else if (value <= -Timestamp.TWO_PWR_63_DBL_) {
    return Timestamp.MIN_VALUE;
  } else if (value + 1 >= Timestamp.TWO_PWR_63_DBL_) {
    return Timestamp.MAX_VALUE;
  } else if (value < 0) {
    return Timestamp.fromNumber(-value).negate();
  } else {
    return new Timestamp(
               (value % Timestamp.TWO_PWR_32_DBL_) | 0,
               (value / Timestamp.TWO_PWR_32_DBL_) | 0);
  }
};

/**
 * Returns a Timestamp representing the 64-bit integer that comes by concatenating the given high and low bits. Each is assumed to use 32 bits.
 *
 * @param {Number} lowBits the low 32-bits.
 * @param {Number} highBits the high 32-bits.
 * @return {Timestamp} the corresponding Timestamp value.
 * @api public
 */
Timestamp.fromBits = function(lowBits, highBits) {
  return new Timestamp(lowBits, highBits);
};

/**
 * Returns a Timestamp representation of the given string, written using the given radix.
 *
 * @param {String} str the textual representation of the Timestamp.
 * @param {Number} opt_radix the radix in which the text is written.
 * @return {Timestamp} the corresponding Timestamp value.
 * @api public
 */
Timestamp.fromString = function(str, opt_radix) {
  if (str.length == 0) {
    throw Error('number format error: empty string');
  }

  var radix = opt_radix || 10;
  if (radix < 2 || 36 < radix) {
    throw Error('radix out of range: ' + radix);
  }

  if (str.charAt(0) == '-') {
    return Timestamp.fromString(str.substring(1), radix).negate();
  } else if (str.indexOf('-') >= 0) {
    throw Error('number format error: interior "-" character: ' + str);
  }

  // Do several (8) digits each time through the loop, so as to
  // minimize the calls to the very expensive emulated div.
  var radixToPower = Timestamp.fromNumber(Math.pow(radix, 8));

  var result = Timestamp.ZERO;
  for (var i = 0; i < str.length; i += 8) {
    var size = Math.min(8, str.length - i);
    var value = parseInt(str.substring(i, i + size), radix);
    if (size < 8) {
      var power = Timestamp.fromNumber(Math.pow(radix, size));
      result = result.multiply(power).add(Timestamp.fromNumber(value));
    } else {
      result = result.multiply(radixToPower);
      result = result.add(Timestamp.fromNumber(value));
    }
  }
  return result;
};

// NOTE: Common constant values ZERO, ONE, NEG_ONE, etc. are defined below the
// from* methods on which they depend.


/**
 * A cache of the Timestamp representations of small integer values.
 * @type {Object}
 * @api private
 */
Timestamp.INT_CACHE_ = {};

// NOTE: the compiler should inline these constant values below and then remove
// these variables, so there should be no runtime penalty for these.

/**
 * Number used repeated below in calculations.  This must appear before the
 * first call to any from* function below.
 * @type {number}
 * @api private
 */
Timestamp.TWO_PWR_16_DBL_ = 1 << 16;

/**
 * @type {number}
 * @api private
 */
Timestamp.TWO_PWR_24_DBL_ = 1 << 24;

/**
 * @type {number}
 * @api private
 */
Timestamp.TWO_PWR_32_DBL_ = Timestamp.TWO_PWR_16_DBL_ * Timestamp.TWO_PWR_16_DBL_;

/**
 * @type {number}
 * @api private
 */
Timestamp.TWO_PWR_31_DBL_ = Timestamp.TWO_PWR_32_DBL_ / 2;

/**
 * @type {number}
 * @api private
 */
Timestamp.TWO_PWR_48_DBL_ = Timestamp.TWO_PWR_32_DBL_ * Timestamp.TWO_PWR_16_DBL_;

/**
 * @type {number}
 * @api private
 */
Timestamp.TWO_PWR_64_DBL_ = Timestamp.TWO_PWR_32_DBL_ * Timestamp.TWO_PWR_32_DBL_;

/**
 * @type {number}
 * @api private
 */
Timestamp.TWO_PWR_63_DBL_ = Timestamp.TWO_PWR_64_DBL_ / 2;

/** @type {Timestamp} */
Timestamp.ZERO = Timestamp.fromInt(0);

/** @type {Timestamp} */
Timestamp.ONE = Timestamp.fromInt(1);

/** @type {Timestamp} */
Timestamp.NEG_ONE = Timestamp.fromInt(-1);

/** @type {Timestamp} */
Timestamp.MAX_VALUE =
    Timestamp.fromBits(0xFFFFFFFF | 0, 0x7FFFFFFF | 0);

/** @type {Timestamp} */
Timestamp.MIN_VALUE = Timestamp.fromBits(0, 0x80000000 | 0);

/**
 * @type {Timestamp}
 * @api private
 */
Timestamp.TWO_PWR_24_ = Timestamp.fromInt(1 << 24);

/**
 * Expose.
 */
exports.Timestamp = Timestamp;
}, 

 });


if(typeof module != 'undefined' && module.exports ){
  module.exports = bson;

  if( !module.parent ){
    bson();
  }
}

if(typeof window != 'undefined' && typeof require == 'undefined'){
  window.require = bson.require;
}



// ---- MODULE: error_codes ---- 
// AUTO-GENERATED FILE DO NOT EDIT
// See src/mongo/base/generate_error_codes.py
/*    Copyright 2015 MongoDB, Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects
*    for all of the code used other than as permitted herein. If you modify
*    file(s) with this exception, you may extend this exception to your
*    version of the file(s), but you are not obligated to do so. If you do not
*    wish to do so, delete this exception statement from your version. If you
*    delete this exception statement from all source files in the program,
*    then also delete it in the license file.
*/

var ErrorCodes = {
    OK: 0,
    InternalError: 1,
    BadValue: 2,
    OBSOLETE_DuplicateKey: 3,
    NoSuchKey: 4,
    GraphContainsCycle: 5,
    HostUnreachable: 6,
    HostNotFound: 7,
    UnknownError: 8,
    FailedToParse: 9,
    CannotMutateObject: 10,
    UserNotFound: 11,
    UnsupportedFormat: 12,
    Unauthorized: 13,
    TypeMismatch: 14,
    Overflow: 15,
    InvalidLength: 16,
    ProtocolError: 17,
    AuthenticationFailed: 18,
    CannotReuseObject: 19,
    IllegalOperation: 20,
    EmptyArrayOperation: 21,
    InvalidBSON: 22,
    AlreadyInitialized: 23,
    LockTimeout: 24,
    RemoteValidationError: 25,
    NamespaceNotFound: 26,
    IndexNotFound: 27,
    PathNotViable: 28,
    NonExistentPath: 29,
    InvalidPath: 30,
    RoleNotFound: 31,
    RolesNotRelated: 32,
    PrivilegeNotFound: 33,
    CannotBackfillArray: 34,
    UserModificationFailed: 35,
    RemoteChangeDetected: 36,
    FileRenameFailed: 37,
    FileNotOpen: 38,
    FileStreamFailed: 39,
    ConflictingUpdateOperators: 40,
    FileAlreadyOpen: 41,
    LogWriteFailed: 42,
    CursorNotFound: 43,
    UserDataInconsistent: 45,
    LockBusy: 46,
    NoMatchingDocument: 47,
    NamespaceExists: 48,
    InvalidRoleModification: 49,
    ExceededTimeLimit: 50,
    ManualInterventionRequired: 51,
    DollarPrefixedFieldName: 52,
    InvalidIdField: 53,
    NotSingleValueField: 54,
    InvalidDBRef: 55,
    EmptyFieldName: 56,
    DottedFieldName: 57,
    RoleModificationFailed: 58,
    CommandNotFound: 59,
    OBSOLETE_DatabaseNotFound: 60,
    ShardKeyNotFound: 61,
    OplogOperationUnsupported: 62,
    StaleShardVersion: 63,
    WriteConcernFailed: 64,
    MultipleErrorsOccurred: 65,
    ImmutableField: 66,
    CannotCreateIndex: 67,
    IndexAlreadyExists: 68,
    AuthSchemaIncompatible: 69,
    ShardNotFound: 70,
    ReplicaSetNotFound: 71,
    InvalidOptions: 72,
    InvalidNamespace: 73,
    NodeNotFound: 74,
    WriteConcernLegacyOK: 75,
    NoReplicationEnabled: 76,
    OperationIncomplete: 77,
    CommandResultSchemaViolation: 78,
    UnknownReplWriteConcern: 79,
    RoleDataInconsistent: 80,
    NoMatchParseContext: 81,
    NoProgressMade: 82,
    RemoteResultsUnavailable: 83,
    DuplicateKeyValue: 84,
    IndexOptionsConflict: 85,
    IndexKeySpecsConflict: 86,
    CannotSplit: 87,
    SplitFailed_OBSOLETE: 88,
    NetworkTimeout: 89,
    CallbackCanceled: 90,
    ShutdownInProgress: 91,
    SecondaryAheadOfPrimary: 92,
    InvalidReplicaSetConfig: 93,
    NotYetInitialized: 94,
    NotSecondary: 95,
    OperationFailed: 96,
    NoProjectionFound: 97,
    DBPathInUse: 98,
    CannotSatisfyWriteConcern: 100,
    OutdatedClient: 101,
    IncompatibleAuditMetadata: 102,
    NewReplicaSetConfigurationIncompatible: 103,
    NodeNotElectable: 104,
    IncompatibleShardingMetadata: 105,
    DistributedClockSkewed: 106,
    LockFailed: 107,
    InconsistentReplicaSetNames: 108,
    ConfigurationInProgress: 109,
    CannotInitializeNodeWithData: 110,
    NotExactValueField: 111,
    WriteConflict: 112,
    InitialSyncFailure: 113,
    InitialSyncOplogSourceMissing: 114,
    CommandNotSupported: 115,
    DocTooLargeForCapped: 116,
    ConflictingOperationInProgress: 117,
    NamespaceNotSharded: 118,
    InvalidSyncSource: 119,
    OplogStartMissing: 120,
    DocumentValidationFailure: 121,
    OBSOLETE_ReadAfterOptimeTimeout: 122,
    NotAReplicaSet: 123,
    IncompatibleElectionProtocol: 124,
    CommandFailed: 125,
    RPCProtocolNegotiationFailed: 126,
    UnrecoverableRollbackError: 127,
    LockNotFound: 128,
    LockStateChangeFailed: 129,
    SymbolNotFound: 130,
    RLPInitializationFailed: 131,
    ConfigServersInconsistent: 132,
    FailedToSatisfyReadPreference: 133,
    ReadConcernMajorityNotAvailableYet: 134,
    StaleTerm: 135,
    CappedPositionLost: 136,
    IncompatibleShardingConfigVersion: 137,
    RemoteOplogStale: 138,
    JSInterpreterFailure: 139,
    InvalidSSLConfiguration: 140,
    SSLHandshakeFailed: 141,
    JSUncatchableError: 142,
    CursorInUse: 143,
    IncompatibleCatalogManager: 144,
    PooledConnectionsDropped: 145,
    ExceededMemoryLimit: 146,
    ZLibError: 147,
    ReadConcernMajorityNotEnabled: 148,
    NoConfigMaster: 149,
    StaleEpoch: 150,
    OperationCannotBeBatched: 151,
    OplogOutOfOrder: 152,
    ChunkTooBig: 153,
    InconsistentShardIdentity: 154,
    CannotApplyOplogWhilePrimary: 155,
    NeedsDocumentMove: 156,
    CanRepairToDowngrade: 157,
    MustUpgrade: 158,
    DurationOverflow: 159,
    SocketException: 9001,
    RecvStaleConfig: 9996,
    CannotGrowDocumentInCappedNamespace: 10003,
    NotMaster: 10107,
    DuplicateKey: 11000,
    InterruptedAtShutdown: 11600,
    Interrupted: 11601,
    InterruptedDueToReplStateChange: 11602,
    BackgroundOperationInProgressForDatabase: 12586,
    BackgroundOperationInProgressForNamespace: 12587,
    OBSOLETE_PrepareConfigsFailed: 13104,
    DatabaseDifferCase: 13297,
    ShardKeyTooBig: 13334,
    SendStaleConfig: 13388,
    NotMasterNoSlaveOk: 13435,
    NotMasterOrSecondary: 13436,
    OutOfDiskSpace: 14031,
    KeyTooLong: 17280
};

var ErrorCodeStrings = {
    0: 'OK',
    1: 'InternalError',
    2: 'BadValue',
    3: 'OBSOLETE_DuplicateKey',
    4: 'NoSuchKey',
    5: 'GraphContainsCycle',
    6: 'HostUnreachable',
    7: 'HostNotFound',
    8: 'UnknownError',
    9: 'FailedToParse',
    10: 'CannotMutateObject',
    11: 'UserNotFound',
    12: 'UnsupportedFormat',
    13: 'Unauthorized',
    14: 'TypeMismatch',
    15: 'Overflow',
    16: 'InvalidLength',
    17: 'ProtocolError',
    18: 'AuthenticationFailed',
    19: 'CannotReuseObject',
    20: 'IllegalOperation',
    21: 'EmptyArrayOperation',
    22: 'InvalidBSON',
    23: 'AlreadyInitialized',
    24: 'LockTimeout',
    25: 'RemoteValidationError',
    26: 'NamespaceNotFound',
    27: 'IndexNotFound',
    28: 'PathNotViable',
    29: 'NonExistentPath',
    30: 'InvalidPath',
    31: 'RoleNotFound',
    32: 'RolesNotRelated',
    33: 'PrivilegeNotFound',
    34: 'CannotBackfillArray',
    35: 'UserModificationFailed',
    36: 'RemoteChangeDetected',
    37: 'FileRenameFailed',
    38: 'FileNotOpen',
    39: 'FileStreamFailed',
    40: 'ConflictingUpdateOperators',
    41: 'FileAlreadyOpen',
    42: 'LogWriteFailed',
    43: 'CursorNotFound',
    45: 'UserDataInconsistent',
    46: 'LockBusy',
    47: 'NoMatchingDocument',
    48: 'NamespaceExists',
    49: 'InvalidRoleModification',
    50: 'ExceededTimeLimit',
    51: 'ManualInterventionRequired',
    52: 'DollarPrefixedFieldName',
    53: 'InvalidIdField',
    54: 'NotSingleValueField',
    55: 'InvalidDBRef',
    56: 'EmptyFieldName',
    57: 'DottedFieldName',
    58: 'RoleModificationFailed',
    59: 'CommandNotFound',
    60: 'OBSOLETE_DatabaseNotFound',
    61: 'ShardKeyNotFound',
    62: 'OplogOperationUnsupported',
    63: 'StaleShardVersion',
    64: 'WriteConcernFailed',
    65: 'MultipleErrorsOccurred',
    66: 'ImmutableField',
    67: 'CannotCreateIndex',
    68: 'IndexAlreadyExists',
    69: 'AuthSchemaIncompatible',
    70: 'ShardNotFound',
    71: 'ReplicaSetNotFound',
    72: 'InvalidOptions',
    73: 'InvalidNamespace',
    74: 'NodeNotFound',
    75: 'WriteConcernLegacyOK',
    76: 'NoReplicationEnabled',
    77: 'OperationIncomplete',
    78: 'CommandResultSchemaViolation',
    79: 'UnknownReplWriteConcern',
    80: 'RoleDataInconsistent',
    81: 'NoMatchParseContext',
    82: 'NoProgressMade',
    83: 'RemoteResultsUnavailable',
    84: 'DuplicateKeyValue',
    85: 'IndexOptionsConflict',
    86: 'IndexKeySpecsConflict',
    87: 'CannotSplit',
    88: 'SplitFailed_OBSOLETE',
    89: 'NetworkTimeout',
    90: 'CallbackCanceled',
    91: 'ShutdownInProgress',
    92: 'SecondaryAheadOfPrimary',
    93: 'InvalidReplicaSetConfig',
    94: 'NotYetInitialized',
    95: 'NotSecondary',
    96: 'OperationFailed',
    97: 'NoProjectionFound',
    98: 'DBPathInUse',
    100: 'CannotSatisfyWriteConcern',
    101: 'OutdatedClient',
    102: 'IncompatibleAuditMetadata',
    103: 'NewReplicaSetConfigurationIncompatible',
    104: 'NodeNotElectable',
    105: 'IncompatibleShardingMetadata',
    106: 'DistributedClockSkewed',
    107: 'LockFailed',
    108: 'InconsistentReplicaSetNames',
    109: 'ConfigurationInProgress',
    110: 'CannotInitializeNodeWithData',
    111: 'NotExactValueField',
    112: 'WriteConflict',
    113: 'InitialSyncFailure',
    114: 'InitialSyncOplogSourceMissing',
    115: 'CommandNotSupported',
    116: 'DocTooLargeForCapped',
    117: 'ConflictingOperationInProgress',
    118: 'NamespaceNotSharded',
    119: 'InvalidSyncSource',
    120: 'OplogStartMissing',
    121: 'DocumentValidationFailure',
    122: 'OBSOLETE_ReadAfterOptimeTimeout',
    123: 'NotAReplicaSet',
    124: 'IncompatibleElectionProtocol',
    125: 'CommandFailed',
    126: 'RPCProtocolNegotiationFailed',
    127: 'UnrecoverableRollbackError',
    128: 'LockNotFound',
    129: 'LockStateChangeFailed',
    130: 'SymbolNotFound',
    131: 'RLPInitializationFailed',
    132: 'ConfigServersInconsistent',
    133: 'FailedToSatisfyReadPreference',
    134: 'ReadConcernMajorityNotAvailableYet',
    135: 'StaleTerm',
    136: 'CappedPositionLost',
    137: 'IncompatibleShardingConfigVersion',
    138: 'RemoteOplogStale',
    139: 'JSInterpreterFailure',
    140: 'InvalidSSLConfiguration',
    141: 'SSLHandshakeFailed',
    142: 'JSUncatchableError',
    143: 'CursorInUse',
    144: 'IncompatibleCatalogManager',
    145: 'PooledConnectionsDropped',
    146: 'ExceededMemoryLimit',
    147: 'ZLibError',
    148: 'ReadConcernMajorityNotEnabled',
    149: 'NoConfigMaster',
    150: 'StaleEpoch',
    151: 'OperationCannotBeBatched',
    152: 'OplogOutOfOrder',
    153: 'ChunkTooBig',
    154: 'InconsistentShardIdentity',
    155: 'CannotApplyOplogWhilePrimary',
    156: 'NeedsDocumentMove',
    157: 'CanRepairToDowngrade',
    158: 'MustUpgrade',
    159: 'DurationOverflow',
    9001: 'SocketException',
    9996: 'RecvStaleConfig',
    10003: 'CannotGrowDocumentInCappedNamespace',
    10107: 'NotMaster',
    11000: 'DuplicateKey',
    11600: 'InterruptedAtShutdown',
    11601: 'Interrupted',
    11602: 'InterruptedDueToReplStateChange',
    12586: 'BackgroundOperationInProgressForDatabase',
    12587: 'BackgroundOperationInProgressForNamespace',
    13104: 'OBSOLETE_PrepareConfigsFailed',
    13297: 'DatabaseDifferCase',
    13334: 'ShardKeyTooBig',
    13388: 'SendStaleConfig',
    13435: 'NotMasterNoSlaveOk',
    13436: 'NotMasterOrSecondary',
    14031: 'OutOfDiskSpace',
    17280: 'KeyTooLong'
};



// ---- MODULE: port_bson ---- 

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
	else if(arguments.length == 1 && typeof arguments[0] === "number")
		return bson().Long.fromNumber(arguments[0]);
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
			case "$timestamp": return new Timestamp(obj["$timestamp"]["t"], obj["$timestamp"]["i"])
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
			case "$regex":
			case "$timestamp": return true;
			case "$binary":
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
// x x Timestamp
//   x Regular_expression
// x x OID
//     DB Reference
//     Undefined
// x x MinKey
// x x MaxKey
// x x NumberLong
// TODO: Copy RegExp into Namespace, because we change its prototype, which should not be visible outside of namespace
// caution: we have to handle user inputted regexps then!
// same for Date


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

Date.prototype.toJSON = function() {
	return { "$date": this.toISOString() };
}

Timestamp.prototype.toJSON = function() {
	return { "$timestamp": { "t": this.low_, "i": this.high_ } };
}


// ---- MODULE: port_misc ---- 

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


	/** Replaces occurrences of '([0-9]+)L' outside of strings to NumberLong($1); **/
	function replaceLwithNumberLong(code){
		var result = "";
		code = code.split("");

		/* We have to check for variable names at some places. The regex for that is HUGE, though.
		 * so we check for everything not allowed in a variable and invert, which is close enough
		 * because for invalid code we don't need defined behaviour
		 */
		var nonVariableNameRegex = /[ \t\n\r(){}\[\]:;,<>+-/*%!&|=\"'?^~]/;

		var i;
		var inString = false;
		var currentStringDelim = null;
		var inNumber = false;
		var currentNumber = "";
		for(i = 0; i < code.length; i++){
			if(!inString){
				//we are not in a string and found a numeral digit
				if(!isNaN(parseInt(code[i])) &&
					// which is preceded by a non-alpha character (variables like asdf1L!) or was preceded by a number
					(inNumber || i === 0 || code[i-1].match(nonVariableNameRegex))){
					inNumber = true;
					currentNumber += code[i];
					continue;
				}

				//we are not in a string and have found something else than a numeric digit and were in a number
				if(inNumber){
					if(code[i] === "L"){
						//we were in a number and it ends in L => append as NumberLong
						if(result.endsWith("-")){
							result = result.substr(0, result.length - 1);
							currentNumber = "-" + currentNumber;

							//if this is some arithmetic (wtf, really?) we have to replace the - with a +
							//=> check if the last non-whitespace is a possible identifier character
							var j = i - currentNumber.length;
							var lastNonWhitespace = " ";
							while(--j > 0){
								if(code[j] !== " " && code[j] !== "\n" && code[j] !== "\t"  && code[j] !== "\r"){
									lastNonWhitespace = code[j];
									break;
								}
							}
							//possibly a variable name, result of a function call or array-lookup
							if(!lastNonWhitespace.match(nonVariableNameRegex) || lastNonWhitespace === ")" || lastNonWhitespace === "]")
								result += " + ";
						}

						result += "NumberLong(" + currentNumber + ")";
						currentNumber = "";
						inNumber = false;
						continue;
					}
					//we were in a number and it does not end in L => append the Number normally
					result += currentNumber;
					currentNumber = "";
					inNumber = false;
				}

				//we are not in a string and possibly found the beginning of a string
				if(code[i] === "\"" || code[i] === "'"){
					//count the number of escape characters before the delimiter
					var numberOfEscapeChars = 0;
					while(result.endsWith("\\".repeat(numberOfEscapeChars+1)))
						numberOfEscapeChars++;

					//no unescaped escape character => we are entering the string
					if(numberOfEscapeChars % 2 === 0){
						inString = true;
						currentStringDelim = code[i];
					}
				}

				result += code[i];
				continue;
			}else{
				//we are in a string and found the character with which the string started
				if(code[i] === currentStringDelim){
					//count the number of escape characters before the delimiter
					var numberOfEscapeChars = 0;
					while(result.endsWith("\\".repeat(numberOfEscapeChars+1)))
						numberOfEscapeChars++;

					//no unescaped escape character => we are leaving the string
					if(numberOfEscapeChars % 2 === 0){
						inString = false;
						currentStringDelim = null;
					}
				}
				result += code[i];
				continue;
			}
		}

		if(currentNumber !== "")
			result += currentNumber;

		return result;
	}


	function evaluate(db, code, window, document){
		return eval(replaceLwithNumberLong(code));
	}

	return evaluate.call(context, db, code);
}


// ---- MODULE: port_cursor ---- 
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
        data: null,
        m: null //message
    };

    this.connection = connection;
}


DBClientCursor.prototype.assembleCommandRequest = function(/*BClientWithCommands* */ cli,
                               /*StringData*/ database,
                               /*int*/ legacyQueryOptions,
                               /*BSONObj*/ legacyQuery) {
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
    if (!this.cursorId || this.cursorId === 0 || this.cursorId instanceof NumberLong && this.cursorId.isZero()) {
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
    if (this._isCommand && (this.cursorId === 0 || (this.cursorId instanceof NumberLong && this.cursorId.isZero()))) {
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
        this.cursorId = NumberLong(0);
    }

    if (this.cursorId == 0 || !(this.opts & QueryOptions.QueryOption_CursorTailable)) {
        // only set initially: we don't want to kill it on end of data
        // if it's a tailable cursor
        if(typeof qr.cursorId === "number")
            this.cursorId = NumberLong(qr.cursorId);
        else if(typeof qr.cursorId === "string" && qr.cursorId.startsWith("NumberLong(\"") && qr.cursorId.endsWith("\")"))
            this.cursorId = NumberLong(qr.cursorId.substring(12, qr.cursorId.length-2))
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

    var commandReply = this.batch.m;

    if(commandReply.ok == ErrorCodes.SendStaleConfig){
        throw new Error("stale config in DBClientCursor::dataReceived()" + JSON.stringify(commandReply))
    }

    if(!commandReply.ok){
        this.wasError = true;
    }

    this.batch.data = [JSON.stringify(this.batch.m)]
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
    function handleData(data){
        self.batch.m = data;
        if(typeof data !== "object" || data === {}){
            throw Error("Received invalid or empty data during initialization");
        }
        self.dataReceived();
    }

    Connection.initCursor(toSend, handleData, this);
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

    var extended_json = this.batch.data[this.batch.pos++];
    var jsObj = JSON.parse(extended_json);

    return jsObjectToJSObjectWithBsonValues(jsObj);

    // BSONObj o(batch.data);
    // batch.data += o.objsize();
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
    function handleData(data){
        self.batch.m = data;
        if(typeof data !== "object" || data === {}){
            throw Error("Received invalid or empty data while requesting more data");
        }
        self.dataReceived();
    }

    Connection.requestMore(toSend, handleData, this);
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
    return DBClientCursor.call(this, ns, query, NumberLong(0) /*cursorId*/, nToReturn, nToSkip, fieldsToReturn, queryOptions, batchSize, connection);
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



// ---- MODULE: collection ---- 
// @file collection.js - DBCollection support in the mongo shell
// db.colName is a DBCollection object
// or db["colName"]

if ((typeof DBCollection) == "undefined") {
    DBCollection = function(mongo, db, shortName, fullName) {
        this._mongo = mongo;
        this._db = db;
        this._shortName = shortName;
        this._fullName = fullName;

        this.verify();
    };
}

DBCollection.prototype.verify = function() {
    assert(this._fullName, "no fullName");
    assert(this._shortName, "no shortName");
    assert(this._db, "no db");

    assert.eq(this._fullName, this._db._name + "." + this._shortName, "name mismatch");

    assert(this._mongo, "no mongo in DBCollection");
    assert(this.getMongo(), "no mongo from getMongo()");
};

DBCollection.prototype.getName = function() {
    return this._shortName;
};

DBCollection.prototype.help = function() {
    var shortName = this.getName();
    print("DBCollection help");
    print("\tdb." + shortName + ".find().help() - show DBCursor help");
    print(
        "\tdb." + shortName +
        ".bulkWrite( operations, <optional params> ) - bulk execute write operations, optional parameters are: w, wtimeout, j");
    print(
        "\tdb." + shortName +
        ".count( query = {}, <optional params> ) - count the number of documents that matches the query, optional parameters are: limit, skip, hint, maxTimeMS");
    print(
        "\tdb." + shortName +
        ".copyTo(newColl) - duplicates collection by copying all documents to newColl; no indexes are copied.");
    print("\tdb." + shortName + ".convertToCapped(maxBytes) - calls {convertToCapped:'" +
          shortName + "', size:maxBytes}} command");
    print("\tdb." + shortName + ".createIndex(keypattern[,options])");
    print("\tdb." + shortName + ".createIndexes([keypatterns], <options>)");
    print("\tdb." + shortName + ".dataSize()");
    print(
        "\tdb." + shortName +
        ".deleteOne( filter, <optional params> ) - delete first matching document, optional parameters are: w, wtimeout, j");
    print(
        "\tdb." + shortName +
        ".deleteMany( filter, <optional params> ) - delete all matching documents, optional parameters are: w, wtimeout, j");
    print("\tdb." + shortName + ".distinct( key, query, <optional params> ) - e.g. db." +
          shortName + ".distinct( 'x' ), optional parameters are: maxTimeMS");
    print("\tdb." + shortName + ".drop() drop the collection");
    print("\tdb." + shortName + ".dropIndex(index) - e.g. db." + shortName +
          ".dropIndex( \"indexName\" ) or db." + shortName + ".dropIndex( { \"indexKey\" : 1 } )");
    print("\tdb." + shortName + ".dropIndexes()");
    print("\tdb." + shortName +
          ".ensureIndex(keypattern[,options]) - DEPRECATED, use createIndex() instead");
    print("\tdb." + shortName + ".explain().help() - show explain help");
    print("\tdb." + shortName + ".reIndex()");
    print(
        "\tdb." + shortName +
        ".find([query],[fields]) - query is an optional query filter. fields is optional set of fields to return.");
    print("\t                                              e.g. db." + shortName +
          ".find( {x:77} , {name:1, x:1} )");
    print("\tdb." + shortName + ".find(...).count()");
    print("\tdb." + shortName + ".find(...).limit(n)");
    print("\tdb." + shortName + ".find(...).skip(n)");
    print("\tdb." + shortName + ".find(...).sort(...)");
    print("\tdb." + shortName + ".findOne([query], [fields], [options], [readConcern])");
    print(
        "\tdb." + shortName +
        ".findOneAndDelete( filter, <optional params> ) - delete first matching document, optional parameters are: projection, sort, maxTimeMS");
    print(
        "\tdb." + shortName +
        ".findOneAndReplace( filter, replacement, <optional params> ) - replace first matching document, optional parameters are: projection, sort, maxTimeMS, upsert, returnNewDocument");
    print(
        "\tdb." + shortName +
        ".findOneAndUpdate( filter, update, <optional params> ) - update first matching document, optional parameters are: projection, sort, maxTimeMS, upsert, returnNewDocument");
    print("\tdb." + shortName + ".getDB() get DB object associated with collection");
    print("\tdb." + shortName + ".getPlanCache() get query plan cache associated with collection");
    print("\tdb." + shortName + ".getIndexes()");
    print("\tdb." + shortName + ".group( { key : ..., initial: ..., reduce : ...[, cond: ...] } )");
    print("\tdb." + shortName + ".insert(obj)");
    print(
        "\tdb." + shortName +
        ".insertOne( obj, <optional params> ) - insert a document, optional parameters are: w, wtimeout, j");
    print(
        "\tdb." + shortName +
        ".insertMany( [objects], <optional params> ) - insert multiple documents, optional parameters are: w, wtimeout, j");
    print("\tdb." + shortName + ".mapReduce( mapFunction , reduceFunction , <optional params> )");
    print(
        "\tdb." + shortName +
        ".aggregate( [pipeline], <optional params> ) - performs an aggregation on a collection; returns a cursor");
    print("\tdb." + shortName + ".remove(query)");
    print(
        "\tdb." + shortName +
        ".replaceOne( filter, replacement, <optional params> ) - replace the first matching document, optional parameters are: upsert, w, wtimeout, j");
    print("\tdb." + shortName +
          ".renameCollection( newName , <dropTarget> ) renames the collection.");
    print(
        "\tdb." + shortName +
        ".runCommand( name , <options> ) runs a db command with the given name where the first param is the collection name");
    print("\tdb." + shortName + ".save(obj)");
    print("\tdb." + shortName + ".stats({scale: N, indexDetails: true/false, " +
          "indexDetailsKey: <index key>, indexDetailsName: <index name>})");
    // print("\tdb." + shortName + ".diskStorageStats({[extent: <num>,] [granularity: <bytes>,]
    // ...}) - analyze record layout on disk");
    // print("\tdb." + shortName + ".pagesInRAM({[extent: <num>,] [granularity: <bytes>,] ...}) -
    // analyze resident memory pages");
    print("\tdb." + shortName +
          ".storageSize() - includes free space allocated to this collection");
    print("\tdb." + shortName + ".totalIndexSize() - size in bytes of all the indexes");
    print("\tdb." + shortName + ".totalSize() - storage allocated for all data and indexes");
    print(
        "\tdb." + shortName +
        ".update( query, object[, upsert_bool, multi_bool] ) - instead of two flags, you can pass an object with fields: upsert, multi");
    print(
        "\tdb." + shortName +
        ".updateOne( filter, update, <optional params> ) - update the first matching document, optional parameters are: upsert, w, wtimeout, j");
    print(
        "\tdb." + shortName +
        ".updateMany( filter, update, <optional params> ) - update all matching documents, optional parameters are: upsert, w, wtimeout, j");
    print("\tdb." + shortName + ".validate( <full> ) - SLOW");
    print("\tdb." + shortName + ".getShardVersion() - only for use with sharding");
    print("\tdb." + shortName +
          ".getShardDistribution() - prints statistics about data distribution in the cluster");
    print(
        "\tdb." + shortName +
        ".getSplitKeysForChunks( <maxChunkSize> ) - calculates split points over all chunks and returns splitter function");
    print(
        "\tdb." + shortName +
        ".getWriteConcern() - returns the write concern used for any operations on this collection, inherited from server/db if set");
    print(
        "\tdb." + shortName +
        ".setWriteConcern( <write concern doc> ) - sets the write concern for writes to the collection");
    print(
        "\tdb." + shortName +
        ".unsetWriteConcern( <write concern doc> ) - unsets the write concern for writes to the collection");
    // print("\tdb." + shortName + ".getDiskStorageStats({...}) - prints a summary of disk usage
    // statistics");
    // print("\tdb." + shortName + ".getPagesInRAM({...}) - prints a summary of storage pages
    // currently in physical memory");
    return __magicNoPrint;
};

DBCollection.prototype.getFullName = function() {
    return this._fullName;
};
DBCollection.prototype.getMongo = function() {
    return this._db.getMongo();
};
DBCollection.prototype.getDB = function() {
    return this._db;
};

DBCollection.prototype._makeCommand = function(cmd, params) {
    var c = {};
    c[cmd] = this.getName();
    if (params)
        Object.extend(c, params);
    return c;
};

DBCollection.prototype._dbCommand = function(cmd, params) {
    if (typeof(cmd) === "object")
        return this._db._dbCommand(cmd, {}, this.getQueryOptions());

    return this._db._dbCommand(this._makeCommand(cmd, params), {}, this.getQueryOptions());
};

// Like _dbCommand, but applies $readPreference
DBCollection.prototype._dbReadCommand = function(cmd, params) {
    if (typeof(cmd) === "object")
        return this._db._dbReadCommand(cmd, {}, this.getQueryOptions());

    return this._db._dbReadCommand(this._makeCommand(cmd, params), {}, this.getQueryOptions());
};

DBCollection.prototype.runCommand = DBCollection.prototype._dbCommand;

DBCollection.prototype.runReadCommand = DBCollection.prototype._dbReadCommand;

DBCollection.prototype._massageObject = function(q) {
    if (!q)
        return {};

    var type = typeof q;

    if (type == "function")
        return {
            $where: q
        };

    if (q.isObjectId)
        return {
            _id: q
        };

    if (type == "object")
        return q;

    if (type == "string") {
        if (q.length == 24)
            return {
                _id: q
            };

        return {
            $where: q
        };
    }

    throw Error("don't know how to massage : " + type);

};

DBCollection.prototype._validateObject = function(o) {
    // Hidden property for testing purposes.
    if (this.getMongo()._skipValidation)
        return;

    if (typeof(o) != "object")
        throw Error("attempted to save a " + typeof(o) + " value.  document expected.");

    if (o._ensureSpecial && o._checkModify)
        throw Error("can't save a DBQuery object");
};

DBCollection._allowedFields = {
    $id: 1,
    $ref: 1,
    $db: 1
};

DBCollection.prototype._validateForStorage = function(o) {
    // Hidden property for testing purposes.
    if (this.getMongo()._skipValidation)
        return;

    this._validateObject(o);
    for (var k in o) {
        if (k.indexOf(".") >= 0) {
            throw Error("can't have . in field names [" + k + "]");
        }

        if (k.indexOf("$") == 0 && !DBCollection._allowedFields[k]) {
            throw Error("field names cannot start with $ [" + k + "]");
        }

        if (o[k] !== null && typeof(o[k]) === "object") {
            this._validateForStorage(o[k]);
        }
    }
};

DBCollection.prototype.find = function(query, fields, limit, skip, batchSize, options) {
    var cursor = new DBQuery(this._mongo,
                             this._db,
                             this,
                             this._fullName,
                             this._massageObject(query),
                             fields,
                             limit,
                             skip,
                             batchSize,
                             options || this.getQueryOptions());

    var connObj = this.getMongo();
    var readPrefMode = connObj.getReadPrefMode();
    if (readPrefMode != null) {
        cursor.readPref(readPrefMode, connObj.getReadPrefTagSet());
    }

    return cursor;
};

DBCollection.prototype.findOne = function(query, fields, options, readConcern, collation) {
    var cursor = this.find(query, fields, -1 /* limit */, 0 /* skip*/, 0 /* batchSize */, options);

    if (readConcern) {
        cursor = cursor.readConcern(readConcern);
    }

    if (collation) {
        cursor = cursor.collation(collation);
    }

    if (!cursor.hasNext())
        return null;
    var ret = cursor.next();
    if (cursor.hasNext())
        throw Error("findOne has more than 1 result!");
    if (ret.$err)
        throw _getErrorWithCode(ret, "error " + tojson(ret));
    return ret;
};

DBCollection.prototype.insert = function(obj, options, _allow_dot) {
    if (!obj)
        throw Error("no object passed to insert!");

    var flags = 0;

    var wc = undefined;
    var allowDottedFields = false;
    if (options === undefined) {
        // do nothing
    } else if (typeof(options) == 'object') {
        if (options.ordered === undefined) {
            // do nothing, like above
        } else {
            flags = options.ordered ? 0 : 1;
        }

        if (options.writeConcern)
            wc = options.writeConcern;
        if (options.allowdotted)
            allowDottedFields = true;
    } else {
        flags = options;
    }

    // 1 = continueOnError, which is synonymous with unordered in the write commands/bulk-api
    var ordered = ((flags & 1) == 0);

    if (!wc)
        wc = this.getWriteConcern();

    var result = undefined;
    var startTime =
        (typeof(_verboseShell) === 'undefined' || !_verboseShell) ? 0 : new Date().getTime();

    if (this.getMongo().writeMode() != "legacy") {
        // Bit 1 of option flag is continueOnError. Bit 0 (stop on error) is the default.
        var bulk = ordered ? this.initializeOrderedBulkOp() : this.initializeUnorderedBulkOp();
        var isMultiInsert = Array.isArray(obj);

        if (isMultiInsert) {
            obj.forEach(function(doc) {
                bulk.insert(doc);
            });
        } else {
            bulk.insert(obj);
        }

        try {
            result = bulk.execute(wc);
            if (!isMultiInsert)
                result = result.toSingleResult();
        } catch (ex) {
            if (ex instanceof BulkWriteError) {
                result = isMultiInsert ? ex.toResult() : ex.toSingleResult();
            } else if (ex instanceof WriteCommandError) {
                result = isMultiInsert ? ex : ex.toSingleResult();
            } else {
                // Other exceptions thrown
                throw Error(ex);
            }
        }
    } else {
        if (!_allow_dot) {
            this._validateForStorage(obj);
        }

        if (typeof(obj._id) == "undefined" && !Array.isArray(obj)) {
            var tmp = obj;  // don't want to modify input
            obj = {
                _id: new ObjectId()
            };
            for (var key in tmp) {
                obj[key] = tmp[key];
            }
        }

        this.getMongo().insert(this._fullName, obj, flags);

        // enforce write concern, if required
        if (wc)
            result = this.runCommand("getLastError", wc instanceof WriteConcern ? wc.toJSON() : wc);
    }

    this._lastID = obj._id;
    this._printExtraInfo("Inserted", startTime);
    return result;
};

DBCollection.prototype._validateRemoveDoc = function(doc) {
    // Hidden property for testing purposes.
    if (this.getMongo()._skipValidation)
        return;

    for (var k in doc) {
        if (k == "_id" && typeof(doc[k]) == "undefined") {
            throw new Error("can't have _id set to undefined in a remove expression");
        }
    }
};

/**
 * Does validation of the remove args. Throws if the parse is not successful, otherwise
 * returns a document {query: <query>, justOne: <limit>, wc: <writeConcern>}.
 */
DBCollection.prototype._parseRemove = function(t, justOne) {
    if (undefined === t)
        throw Error("remove needs a query");

    var query = this._massageObject(t);

    var wc = undefined;
    var collation = undefined;
    if (typeof(justOne) === "object") {
        var opts = justOne;
        wc = opts.writeConcern;
        justOne = opts.justOne;
        collation = opts.collation;
    }

    // Normalize "justOne" to a bool.
    justOne = justOne ? true : false;

    // Handle write concern.
    if (!wc) {
        wc = this.getWriteConcern();
    }

    return {
        "query": query,
        "justOne": justOne,
        "wc": wc,
        "collation": collation
    };
};

DBCollection.prototype.remove = function(t, justOne) {
    var parsed = this._parseRemove(t, justOne);
    var query = parsed.query;
    var justOne = parsed.justOne;
    var wc = parsed.wc;
    var collation = parsed.collation;

    var result = undefined;
    var startTime =
        (typeof(_verboseShell) === 'undefined' || !_verboseShell) ? 0 : new Date().getTime();

    if (this.getMongo().writeMode() != "legacy") {
        var bulk = this.initializeOrderedBulkOp();
        var removeOp = bulk.find(query);

        if (collation) {
            removeOp.collation(collation);
        }

        if (justOne) {
            removeOp.removeOne();
        } else {
            removeOp.remove();
        }

        try {
            result = bulk.execute(wc).toSingleResult();
        } catch (ex) {
            if (ex instanceof BulkWriteError || ex instanceof WriteCommandError) {
                result = ex.toSingleResult();
            } else {
                // Other exceptions thrown
                throw Error(ex);
            }
        }
    } else {
        if (collation) {
            throw new Error("collation requires use of write commands");
        }

        this._validateRemoveDoc(t);
        this.getMongo().remove(this._fullName, query, justOne);

        // enforce write concern, if required
        if (wc)
            result = this.runCommand("getLastError", wc instanceof WriteConcern ? wc.toJSON() : wc);
    }

    this._printExtraInfo("Removed", startTime);
    return result;
};

DBCollection.prototype._validateUpdateDoc = function(doc) {
    // Hidden property for testing purposes.
    if (this.getMongo()._skipValidation)
        return;

    var firstKey = null;
    for (var key in doc) {
        firstKey = key;
        break;
    }

    if (firstKey != null && firstKey[0] == '$') {
        // for mods we only validate partially, for example keys may have dots
        this._validateObject(doc);
    } else {
        // we're basically inserting a brand new object, do full validation
        this._validateForStorage(doc);
    }
};

/**
 * Does validation of the update args. Throws if the parse is not successful, otherwise
 * returns a document containing fields for query, obj, upsert, multi, and wc.
 *
 * Throws if the arguments are invalid.
 */
DBCollection.prototype._parseUpdate = function(query, obj, upsert, multi) {
    if (!query)
        throw Error("need a query");
    if (!obj)
        throw Error("need an object");

    var wc = undefined;
    var collation = undefined;
    // can pass options via object for improved readability
    if (typeof(upsert) === "object") {
        if (multi) {
            throw Error("Fourth argument must be empty when specifying " +
                        "upsert and multi with an object.");
        }

        var opts = upsert;
        multi = opts.multi;
        wc = opts.writeConcern;
        upsert = opts.upsert;
        collation = opts.collation;
    }

    // Normalize 'upsert' and 'multi' to booleans.
    upsert = upsert ? true : false;
    multi = multi ? true : false;

    if (!wc) {
        wc = this.getWriteConcern();
    }

    return {
        "query": query,
        "obj": obj,
        "upsert": upsert,
        "multi": multi,
        "wc": wc,
        "collation": collation
    };
};

DBCollection.prototype.update = function(query, obj, upsert, multi) {
    var parsed = this._parseUpdate(query, obj, upsert, multi);
    var query = parsed.query;
    var obj = parsed.obj;
    var upsert = parsed.upsert;
    var multi = parsed.multi;
    var wc = parsed.wc;
    var collation = parsed.collation;

    var result = undefined;
    var startTime =
        (typeof(_verboseShell) === 'undefined' || !_verboseShell) ? 0 : new Date().getTime();

    if (this.getMongo().writeMode() != "legacy") {
        var bulk = this.initializeOrderedBulkOp();
        var updateOp = bulk.find(query);

        if (upsert) {
            updateOp = updateOp.upsert();
        }

        if (collation) {
            updateOp.collation(collation);
        }

        if (multi) {
            updateOp.update(obj);
        } else {
            updateOp.updateOne(obj);
        }

        try {
            result = bulk.execute(wc).toSingleResult();
        } catch (ex) {
            if (ex instanceof BulkWriteError || ex instanceof WriteCommandError) {
                result = ex.toSingleResult();
            } else {
                // Other exceptions thrown
                throw Error(ex);
            }
        }
    } else {
        if (collation) {
            throw new Error("collation requires use of write commands");
        }

        this._validateUpdateDoc(obj);
        this.getMongo().update(this._fullName, query, obj, upsert, multi);

        // Enforce write concern, if required
        if (wc) {
            result = this.runCommand("getLastError", wc instanceof WriteConcern ? wc.toJSON() : wc);
        }
    }

    this._printExtraInfo("Updated", startTime);
    return result;
};

DBCollection.prototype.save = function(obj, opts) {
    if (obj == null)
        throw Error("can't save a null");

    if (typeof(obj) == "number" || typeof(obj) == "string")
        throw Error("can't save a number or string");

    if (typeof(obj._id) == "undefined") {
        obj._id = new ObjectId();
        return this.insert(obj, opts);
    } else {
        return this.update({_id: obj._id}, obj, Object.merge({upsert: true}, opts));
    }
};

DBCollection.prototype._genIndexName = function(keys) {
    var name = "";
    for (var k in keys) {
        var v = keys[k];
        if (typeof v == "function")
            continue;

        if (name.length > 0)
            name += "_";
        name += k + "_";

        name += v;
    }
    return name;
};

DBCollection.prototype._indexSpec = function(keys, options) {
    var ret = {
        ns: this._fullName,
        key: keys,
        name: this._genIndexName(keys)
    };

    if (!options) {
    } else if (typeof(options) == "string")
        ret.name = options;
    else if (typeof(options) == "boolean")
        ret.unique = true;
    else if (typeof(options) == "object") {
        if (Array.isArray(options)) {
            if (options.length > 3) {
                throw new Error("Index options that are supplied in array form may only specify" +
                                " three values: name, unique, dropDups");
            }
            var nb = 0;
            for (var i = 0; i < options.length; i++) {
                if (typeof(options[i]) == "string")
                    ret.name = options[i];
                else if (typeof(options[i]) == "boolean") {
                    if (options[i]) {
                        if (nb == 0)
                            ret.unique = true;
                        if (nb == 1)
                            ret.dropDups = true;
                    }
                    nb++;
                }
            }
        } else {
            Object.extend(ret, options);
        }
    } else {
        throw Error("can't handle: " + typeof(options));
    }

    return ret;
};

DBCollection.prototype.createIndex = function(keys, options) {
    return this.createIndexes([keys], options);
};

DBCollection.prototype.createIndexes = function(keys, options) {
    var indexSpecs = Array(keys.length);
    for (var i = 0; i < indexSpecs.length; i++) {
        indexSpecs[i] = this._indexSpec(keys[i], options);
    }

    if (this.getMongo().writeMode() == "commands") {
        for (i = 0; i++; i < indexSpecs.length) {
            delete (indexSpecs[i].ns);  // ns is passed to the first element in the command.
        }
        return this._db.runCommand({createIndexes: this.getName(), indexes: indexSpecs});
    } else if (this.getMongo().writeMode() == "compatibility") {
        // Use the downconversion machinery of the bulk api to do a safe write, report response as a
        // command response
        var result = this._db.getCollection("system.indexes").insert(indexSpecs, 0, true);

        if (result.hasWriteErrors() || result.hasWriteConcernError()) {
            // Return the first error
            var error = result.hasWriteErrors() ? result.getWriteErrors()[0]
                                                : result.getWriteConcernError();
            return {
                ok: 0.0,
                code: error.code,
                errmsg: error.errmsg
            };
        } else {
            return {
                ok: 1.0
            };
        }
    } else {
        this._db.getCollection("system.indexes").insert(indexSpecs, 0, true);
    }
};

DBCollection.prototype.ensureIndex = function(keys, options) {
    var result = this.createIndex(keys, options);

    if (this.getMongo().writeMode() != "legacy") {
        return result;
    }

    err = this.getDB().getLastErrorObj();
    if (err.err) {
        return err;
    }
    // nothing returned on success
};

DBCollection.prototype.reIndex = function() {
    return this._db.runCommand({reIndex: this.getName()});
};

DBCollection.prototype.dropIndexes = function() {
    if (arguments.length)
        throw Error("dropIndexes doesn't take arguments");

    var res = this._db.runCommand({deleteIndexes: this.getName(), index: "*"});
    assert(res, "no result from dropIndex result");
    if (res.ok)
        return res;

    if (res.errmsg.match(/not found/))
        return res;

    throw _getErrorWithCode(res, "error dropping indexes : " + tojson(res));
};

DBCollection.prototype.drop = function() {
    if (arguments.length > 0)
        throw Error("drop takes no argument");
    var ret = this._db.runCommand({drop: this.getName()});
    if (!ret.ok) {
        if (ret.errmsg == "ns not found")
            return false;
        throw _getErrorWithCode(ret, "drop failed: " + tojson(ret));
    }
    return true;
};

DBCollection.prototype.findAndModify = function(args) {
    var cmd = {
        findandmodify: this.getName()
    };
    for (var key in args) {
        cmd[key] = args[key];
    }

    var ret = this._db.runCommand(cmd);
    if (!ret.ok) {
        if (ret.errmsg == "No matching object found") {
            return null;
        }
        throw _getErrorWithCode(ret, "findAndModifyFailed failed: " + tojson(ret));
    }
    return ret.value;
};

DBCollection.prototype.renameCollection = function(newName, dropTarget) {
    return this._db._adminCommand({
        renameCollection: this._fullName,
        to: this._db._name + "." + newName,
        dropTarget: dropTarget
    });
};

// Display verbose information about the operation
DBCollection.prototype._printExtraInfo = function(action, startTime) {
    if (typeof _verboseShell === 'undefined' || !_verboseShell) {
        __callLastError = true;
        return;
    }

    // explicit w:1 so that replset getLastErrorDefaults aren't used here which would be bad.
    var res = this._db.getLastErrorCmd(1);
    if (res) {
        if (res.err != undefined && res.err != null) {
            // error occurred, display it
            print(res.err);
            return;
        }

        var info = action + " ";
        // hack for inserted because res.n is 0
        info += action != "Inserted" ? res.n : 1;
        if (res.n > 0 && res.updatedExisting != undefined)
            info += " " + (res.updatedExisting ? "existing" : "new");
        info += " record(s)";
        var time = new Date().getTime() - startTime;
        info += " in " + time + "ms";
        print(info);
    }
};

DBCollection.prototype.validate = function(full) {
    var cmd = {
        validate: this.getName()
    };

    if (typeof(full) == 'object')  // support arbitrary options here
        Object.extend(cmd, full);
    else
        cmd.full = full;

    var res = this._db.runCommand(cmd);

    if (typeof(res.valid) == 'undefined') {
        // old-style format just put everything in a string. Now using proper fields

        res.valid = false;

        var raw = res.result || res.raw;

        if (raw) {
            var str = "-" + tojson(raw);
            res.valid = !(str.match(/exception/) || str.match(/corrupt/));

            var p = /lastExtentSize:(\d+)/;
            var r = p.exec(str);
            if (r) {
                res.lastExtentSize = Number(r[1]);
            }
        }
    }

    return res;
};

/**
 * Invokes the storageDetails command to provide aggregate and (if requested) detailed information
 * regarding the layout of records and deleted records in the collection extents.
 * getDiskStorageStats provides a human-readable summary of the command output
 */
DBCollection.prototype.diskStorageStats = function(opt) {
    var cmd = {
        storageDetails: this.getName(),
        analyze: 'diskStorage'
    };
    if (typeof(opt) == 'object')
        Object.extend(cmd, opt);

    var res = this._db.runCommand(cmd);
    if (!res.ok && res.errmsg.match(/no such cmd/)) {
        print("this command requires starting mongod with --enableExperimentalStorageDetailsCmd");
    }
    return res;
};

// Refer to diskStorageStats
DBCollection.prototype.getDiskStorageStats = function(params) {
    var stats = this.diskStorageStats(params);
    if (!stats.ok) {
        print("error executing storageDetails command: " + stats.errmsg);
        return;
    }

    print("\n    " + "size".pad(9) + " " + "# recs".pad(10) + " " +
          "[===occupied by BSON=== ---occupied by padding---       free           ]" + "  " +
          "bson".pad(8) + " " + "rec".pad(8) + " " + "padding".pad(8));
    print();

    var BAR_WIDTH = 70;

    var formatSliceData = function(data) {
        var bar = _barFormat([
            [data.bsonBytes / data.onDiskBytes, "="],
            [(data.recBytes - data.bsonBytes) / data.onDiskBytes, "-"]
        ],
                             BAR_WIDTH);

        return sh._dataFormat(data.onDiskBytes).pad(9) + " " + data.numEntries.toFixed(0).pad(10) +
            " " + bar + "  " + (data.bsonBytes / data.onDiskBytes).toPercentStr().pad(8) + " " +
            (data.recBytes / data.onDiskBytes).toPercentStr().pad(8) + " " +
            (data.recBytes / data.bsonBytes).toFixed(4).pad(8);
    };

    var printExtent = function(ex, rng) {
        print("--- extent " + rng + " ---");
        print("tot " + formatSliceData(ex));
        print();
        if (ex.slices) {
            for (var c = 0; c < ex.slices.length; c++) {
                var slice = ex.slices[c];
                print(("" + c).pad(3) + " " + formatSliceData(slice));
            }
            print();
        }
    };

    if (stats.extents) {
        print("--- extent overview ---\n");
        for (var i = 0; i < stats.extents.length; i++) {
            var ex = stats.extents[i];
            print(("" + i).pad(3) + " " + formatSliceData(ex));
        }
        print();
        if (params && (params.granularity || params.numberOfSlices)) {
            for (var i = 0; i < stats.extents.length; i++) {
                printExtent(stats.extents[i], i);
            }
        }
    } else {
        printExtent(stats, "range " + stats.range);
    }

};

/**
 * Invokes the storageDetails command to report the percentage of virtual memory pages of the
 * collection storage currently in physical memory (RAM).
 * getPagesInRAM provides a human-readable summary of the command output
 */
DBCollection.prototype.pagesInRAM = function(opt) {
    var cmd = {
        storageDetails: this.getName(),
        analyze: 'pagesInRAM'
    };
    if (typeof(opt) == 'object')
        Object.extend(cmd, opt);

    var res = this._db.runCommand(cmd);
    if (!res.ok && res.errmsg.match(/no such cmd/)) {
        print("this command requires starting mongod with --enableExperimentalStorageDetailsCmd");
    }
    return res;
};

// Refer to pagesInRAM
DBCollection.prototype.getPagesInRAM = function(params) {
    var stats = this.pagesInRAM(params);
    if (!stats.ok) {
        print("error executing storageDetails command: " + stats.errmsg);
        return;
    }

    var BAR_WIDTH = 70;
    var formatExtentData = function(data) {
        return "size".pad(8) + " " + _barFormat([[data.inMem, '=']], BAR_WIDTH) + "  " +
            data.inMem.toPercentStr().pad(7);
    };

    var printExtent = function(ex, rng) {
        print("--- extent " + rng + " ---");
        print("tot " + formatExtentData(ex));
        print();
        if (ex.slices) {
            print("\tslices, percentage of pages in memory (< .1% : ' ', <25% : '.', " +
                  "<50% : '_', <75% : '=', >75% : '#')");
            print();
            print("\t" + "offset".pad(8) + "  [slices...] (each slice is " +
                  sh._dataFormat(ex.sliceBytes) + ")");
            line = "\t" + ("" + 0).pad(8) + "  [";
            for (var c = 0; c < ex.slices.length; c++) {
                if (c % 80 == 0 && c != 0) {
                    print(line + "]");
                    line = "\t" + sh._dataFormat(ex.sliceBytes * c).pad(8) + "  [";
                }
                var inMem = ex.slices[c];
                if (inMem <= .001)
                    line += " ";
                else if (inMem <= .25)
                    line += ".";
                else if (inMem <= .5)
                    line += "_";
                else if (inMem <= .75)
                    line += "=";
                else
                    line += "#";
            }
            print(line + "]");
            print();
        }
    };

    if (stats.extents) {
        print("--- extent overview ---\n");
        for (var i = 0; i < stats.extents.length; i++) {
            var ex = stats.extents[i];
            print(("" + i).pad(3) + " " + formatExtentData(ex));
        }
        print();
        if (params && (params.granularity || params.numberOfSlices)) {
            for (var i = 0; i < stats.extents.length; i++) {
                printExtent(stats.extents[i], i);
            }
        } else {
            print("use getPagesInRAM({granularity: _bytes_}) or " +
                  "getPagesInRAM({numberOfSlices: _num_} for details");
            print("use pagesInRAM(...) for json output, same parameters apply");
        }
    } else {
        printExtent(stats, "range " + stats.range);
    }
};

DBCollection.prototype.getShardVersion = function() {
    return this._db._adminCommand({getShardVersion: this._fullName});
};

DBCollection.prototype._getIndexesSystemIndexes = function(filter) {
    var si = this.getDB().getCollection("system.indexes");
    var query = {
        ns: this.getFullName()
    };
    if (filter)
        query = Object.extend(query, filter);
    return si.find(query).toArray();
};

DBCollection.prototype._getIndexesCommand = function(filter) {
    var res = this.runCommand("listIndexes", filter);

    if (!res.ok) {
        if (res.code == 59) {
            // command doesn't exist, old mongod
            return null;
        }

        if (res.code == 26) {
            // NamespaceNotFound, for compatability, return []
            return [];
        }

        if (res.errmsg && res.errmsg.startsWith("no such cmd")) {
            return null;
        }

        throw _getErrorWithCode(res, "listIndexes failed: " + tojson(res));
    }

    return new DBCommandCursor(this._mongo, res).toArray();
};

DBCollection.prototype.getIndexes = function(filter) {
    var res = this._getIndexesCommand(filter);
    if (res) {
        return res;
    }
    return this._getIndexesSystemIndexes(filter);
};

DBCollection.prototype.getIndices = DBCollection.prototype.getIndexes;
DBCollection.prototype.getIndexSpecs = DBCollection.prototype.getIndexes;

DBCollection.prototype.getIndexKeys = function() {
    return this.getIndexes().map(function(i) {
        return i.key;
    });
};

DBCollection.prototype.hashAllDocs = function() {
    var cmd = {
        dbhash: 1,
        collections: [this._shortName]
    };
    var res = this._dbCommand(cmd);
    var hash = res.collections[this._shortName];
    assert(hash);
    assert(typeof(hash) == "string");
    return hash;
};

/**
 * <p>Drop a specified index.</p>
 *
 * <p>
 * "index" is the name of the index in the system.indexes name field (run db.system.indexes.find()
 *to
 *  see example data), or an object holding the key(s) used to create the index.
 * For example:
 *  db.collectionName.dropIndex( "myIndexName" );
 *  db.collectionName.dropIndex( { "indexKey" : 1 } );
 * </p>
 *
 * @param {String} name or key object of index to delete.
 * @return A result object.  result.ok will be true if successful.
 */
DBCollection.prototype.dropIndex = function(index) {
    assert(index, "need to specify index to dropIndex");
    var res = this._dbCommand("deleteIndexes", {index: index});
    return res;
};

DBCollection.prototype.copyTo = function(newName) {
    return this.getDB().eval(function(collName, newName) {
        var from = db[collName];
        var to = db[newName];
        to.ensureIndex({_id: 1});
        var count = 0;

        var cursor = from.find();
        while (cursor.hasNext()) {
            var o = cursor.next();
            count++;
            to.save(o);
        }

        return count;
    }, this.getName(), newName);
};

DBCollection.prototype.getCollection = function(subName) {
    return this._db.getCollection(this._shortName + "." + subName);
};

/**
  * scale: The scale at which to deliver results. Unless specified, this command returns all data
  *        in bytes.
  * indexDetails: Includes indexDetails field in results. Default: false.
  * indexDetailsKey: If indexDetails is true, filter contents in indexDetails by this index key.
  * indexDetailsname: If indexDetails is true, filter contents in indexDetails by this index name.
  *
  * It is an error to provide both indexDetailsKey and indexDetailsName.
  */
DBCollection.prototype.stats = function(args) {
    'use strict';

    // For backwards compatibility with db.collection.stats(scale).
    var scale = isObject(args) ? args.scale : args;

    var options = isObject(args) ? args : {};
    if (options.indexDetailsKey && options.indexDetailsName) {
        throw new Error('Cannot filter indexDetails on both indexDetailsKey and ' +
                        'indexDetailsName');
    }
    // collStats can run on a secondary, so we need to apply readPreference
    var res = this._db.runReadCommand({collStats: this._shortName, scale: scale});
    if (!res.ok) {
        return res;
    }

    var getIndexName = function(collection, indexKey) {
        if (!isObject(indexKey))
            return undefined;
        var indexName;
        collection.getIndexes().forEach(function(spec) {
            if (friendlyEqual(spec.key, options.indexDetailsKey)) {
                indexName = spec.name;
            }
        });
        return indexName;
    };

    var filterIndexName = options.indexDetailsName || getIndexName(this, options.indexDetailsKey);

    var updateStats = function(stats, keepIndexDetails, indexName) {
        if (!stats.indexDetails)
            return;
        if (!keepIndexDetails) {
            delete stats.indexDetails;
            return;
        }
        if (!indexName)
            return;
        for (var key in stats.indexDetails) {
            if (key == indexName)
                continue;
            delete stats.indexDetails[key];
        }
    };

    updateStats(res, options.indexDetails, filterIndexName);

    if (res.sharded) {
        for (var shardName in res.shards) {
            updateStats(res.shards[shardName], options.indexDetails, filterIndexName);
        }
    }

    return res;
};

DBCollection.prototype.dataSize = function() {
    return this.stats().size;
};

DBCollection.prototype.storageSize = function() {
    return this.stats().storageSize;
};

DBCollection.prototype.totalIndexSize = function(verbose) {
    var stats = this.stats();
    if (verbose) {
        for (var ns in stats.indexSizes) {
            print(ns + "\t" + stats.indexSizes[ns]);
        }
    }
    return stats.totalIndexSize;
};

DBCollection.prototype.totalSize = function() {
    var total = this.storageSize();
    var totalIndexSize = this.totalIndexSize();
    if (totalIndexSize) {
        total += totalIndexSize;
    }
    return total;
};

DBCollection.prototype.convertToCapped = function(bytes) {
    if (!bytes)
        throw Error("have to specify # of bytes");
    return this._dbCommand({convertToCapped: this._shortName, size: bytes});
};

DBCollection.prototype.exists = function() {
    var res = this._db.runCommand("listCollections", {filter: {name: this._shortName}});
    if (res.ok) {
        var cursor = new DBCommandCursor(this._mongo, res);
        if (!cursor.hasNext())
            return null;
        return cursor.next();
    }

    if (res.errmsg && res.errmsg.startsWith("no such cmd")) {
        return this._db.system.namespaces.findOne({name: this._fullName});
    }

    throw _getErrorWithCode(res, "listCollections failed: " + tojson(res));
};

DBCollection.prototype.isCapped = function() {
    var e = this.exists();
    return (e && e.options && e.options.capped) ? true : false;
};

//
// CRUD specification aggregation cursor extension
//
DBCollection.prototype.aggregate = function(pipeline, aggregateOptions) {
    if (!(pipeline instanceof Array)) {
        // support legacy varargs form. (Also handles db.foo.aggregate())
        pipeline = Array.from(arguments);
        aggregateOptions = {};
    } else if (aggregateOptions === undefined) {
        aggregateOptions = {};
    }

    // Copy the aggregateOptions
    var copy = Object.extend({}, aggregateOptions);

    // Ensure handle crud API aggregateOptions
    var keys = Object.keys(copy);

    for (var i = 0; i < keys.length; i++) {
        var name = keys[i];

        if (name == 'batchSize') {
            if (copy.cursor == null) {
                copy.cursor = {};
            }

            copy.cursor.batchSize = copy['batchSize'];
            delete copy['batchSize'];
        } else if (name == 'useCursor') {
            if (copy.cursor == null) {
                copy.cursor = {};
            }

            delete copy['useCursor'];
        }
    }

    // Assign the cleaned up options
    aggregateOptions = copy;
    // Create the initial command document
    var cmd = {
        pipeline: pipeline
    };
    Object.extend(cmd, aggregateOptions);

    if (!('cursor' in cmd)) {
        // implicitly use cursors
        cmd.cursor = {};
    }

    // in a well formed pipeline, $out must be the last stage. If it isn't then the server
    // will reject the pipeline anyway.
    var hasOutStage = pipeline.length >= 1 && pipeline[pipeline.length - 1].hasOwnProperty("$out");

    var doAgg = function(cmd) {
        // if we don't have an out stage, we could run on a secondary
        // so we need to attach readPreference
        return hasOutStage ? this.runCommand("aggregate", cmd)
                           : this.runReadCommand("aggregate", cmd);
    }.bind(this);

    var res = doAgg(cmd);

    if (!res.ok && (res.code == 17020 || res.errmsg == "unrecognized field \"cursor") &&
        !("cursor" in aggregateOptions)) {
        // If the command failed because cursors aren't supported and the user didn't explicitly
        // request a cursor, try again without requesting a cursor.
        delete cmd.cursor;

        res = doAgg(cmd);

        if ('result' in res && !("cursor" in res)) {
            // convert old-style output to cursor-style output
            res.cursor = {
                ns: '',
                id: NumberLong(0)
            };
            res.cursor.firstBatch = res.result;
            delete res.result;
        }
    }

    assert.commandWorked(res, "aggregate failed");

    if ("cursor" in res) {
        return new DBCommandCursor(this._mongo, res);
    }

    return res;
};

DBCollection.prototype.group = function(params) {
    params.ns = this._shortName;
    return this._db.group(params);
};

DBCollection.prototype.groupcmd = function(params) {
    params.ns = this._shortName;
    return this._db.groupcmd(params);
};

MapReduceResult = function(db, o) {
    Object.extend(this, o);
    this._o = o;
    this._keys = Object.keySet(o);
    this._db = db;
    if (this.result != null) {
        this._coll = this._db.getCollection(this.result);
    }
};

MapReduceResult.prototype._simpleKeys = function() {
    return this._o;
};

MapReduceResult.prototype.find = function() {
    if (this.results)
        return this.results;
    return DBCollection.prototype.find.apply(this._coll, arguments);
};

MapReduceResult.prototype.drop = function() {
    if (this._coll) {
        return this._coll.drop();
    }
};

/**
* just for debugging really
*/
MapReduceResult.prototype.convertToSingleObject = function() {
    var z = {};
    var it = this.results != null ? this.results : this._coll.find();
    it.forEach(function(a) {
        z[a._id] = a.value;
    });
    return z;
};

DBCollection.prototype.convertToSingleObject = function(valueField) {
    var z = {};
    this.find().forEach(function(a) {
        z[a._id] = a[valueField];
    });
    return z;
};

/**
* @param optional object of optional fields;
*/
DBCollection.prototype.mapReduce = function(map, reduce, optionsOrOutString) {
    var c = {
        mapreduce: this._shortName,
        map: map,
        reduce: reduce
    };
    assert(optionsOrOutString, "need to supply an optionsOrOutString");

    if (typeof(optionsOrOutString) == "string")
        c["out"] = optionsOrOutString;
    else
        Object.extend(c, optionsOrOutString);

    var raw;

    if (c["out"].hasOwnProperty("inline") && c["out"]["inline"] === 1) {
        // if inline output is specified, we need to apply readPreference on the command
        // as it could be run on a secondary
        raw = this._db.runReadCommand(c);
    } else {
        raw = this._db.runCommand(c);
    }

    if (!raw.ok) {
        __mrerror__ = raw;
        throw _getErrorWithCode(raw, "map reduce failed:" + tojson(raw));
    }
    return new MapReduceResult(this._db, raw);

};

DBCollection.prototype.toString = function() {
    return this.getFullName();
};

DBCollection.prototype.toString = function() {
    return this.getFullName();
};

DBCollection.prototype.tojson = DBCollection.prototype.toString;

DBCollection.prototype.shellPrint = DBCollection.prototype.toString;

DBCollection.autocomplete = function(obj) {
    var colls = DB.autocomplete(obj.getDB());
    var ret = [];
    for (var i = 0; i < colls.length; i++) {
        var c = colls[i];
        if (c.length <= obj.getName().length)
            continue;
        if (c.slice(0, obj.getName().length + 1) != obj.getName() + '.')
            continue;

        ret.push(c.slice(obj.getName().length + 1));
    }
    return ret;
};

// Sharding additions

/*
Usage :

mongo <mongos>
> load('path-to-file/shardingAdditions.js')
Loading custom sharding extensions...
true

> var collection = db.getMongo().getCollection("foo.bar")
> collection.getShardDistribution() // prints statistics related to the collection's data
distribution

> collection.getSplitKeysForChunks() // generates split points for all chunks in the collection,
based on the
                                     // default maxChunkSize or alternately a specified chunk size
> collection.getSplitKeysForChunks( 10 ) // Mb

> var splitter = collection.getSplitKeysForChunks() // by default, the chunks are not split, the
keys are just
                                                    // found.  A splitter function is returned which
will actually
                                                    // do the splits.

> splitter() // ! Actually executes the splits on the cluster !

*/

DBCollection.prototype.getShardDistribution = function() {

    var stats = this.stats();

    if (!stats.sharded) {
        print("Collection " + this + " is not sharded.");
        return;
    }

    var config = this.getMongo().getDB("config");

    var numChunks = 0;

    for (var shard in stats.shards) {
        var shardDoc = config.shards.findOne({_id: shard});

        print("\nShard " + shard + " at " + shardDoc.host);

        var shardStats = stats.shards[shard];

        var chunks = config.chunks.find({_id: sh._collRE(this), shard: shard}).toArray();

        numChunks += chunks.length;

        var estChunkData = shardStats.size / chunks.length;
        var estChunkCount = Math.floor(shardStats.count / chunks.length);

        print(" data : " + sh._dataFormat(shardStats.size) + " docs : " + shardStats.count +
              " chunks : " + chunks.length);
        print(" estimated data per chunk : " + sh._dataFormat(estChunkData));
        print(" estimated docs per chunk : " + estChunkCount);
    }

    print("\nTotals");
    print(" data : " + sh._dataFormat(stats.size) + " docs : " + stats.count + " chunks : " +
          numChunks);
    for (var shard in stats.shards) {
        var shardStats = stats.shards[shard];

        var estDataPercent = Math.floor(shardStats.size / stats.size * 10000) / 100;
        var estDocPercent = Math.floor(shardStats.count / stats.count * 10000) / 100;

        print(" Shard " + shard + " contains " + estDataPercent + "% data, " + estDocPercent +
              "% docs in cluster, " + "avg obj size on shard : " +
              sh._dataFormat(stats.shards[shard].avgObjSize));
    }

    print("\n");

};

DBCollection.prototype.getSplitKeysForChunks = function(chunkSize) {

    var stats = this.stats();

    if (!stats.sharded) {
        print("Collection " + this + " is not sharded.");
        return;
    }

    var config = this.getMongo().getDB("config");

    if (!chunkSize) {
        chunkSize = config.settings.findOne({_id: "chunksize"}).value;
        print("Chunk size not set, using default of " + chunkSize + "MB");
    } else {
        print("Using chunk size of " + chunkSize + "MB");
    }

    var shardDocs = config.shards.find().toArray();

    var allSplitPoints = {};
    var numSplits = 0;

    for (var i = 0; i < shardDocs.length; i++) {
        var shardDoc = shardDocs[i];
        var shard = shardDoc._id;
        var host = shardDoc.host;
        var sconn = new Mongo(host);

        var chunks = config.chunks.find({_id: sh._collRE(this), shard: shard}).toArray();

        print("\nGetting split points for chunks on shard " + shard + " at " + host);

        var splitPoints = [];

        for (var j = 0; j < chunks.length; j++) {
            var chunk = chunks[j];
            var result = sconn.getDB("admin").runCommand(
                {splitVector: this + "", min: chunk.min, max: chunk.max, maxChunkSize: chunkSize});
            if (!result.ok) {
                print(" Had trouble getting split keys for chunk " + sh._pchunk(chunk) + " :\n");
                printjson(result);
            } else {
                splitPoints = splitPoints.concat(result.splitKeys);

                if (result.splitKeys.length > 0)
                    print(" Added " + result.splitKeys.length + " split points for chunk " +
                          sh._pchunk(chunk));
            }
        }

        print("Total splits for shard " + shard + " : " + splitPoints.length);

        numSplits += splitPoints.length;
        allSplitPoints[shard] = splitPoints;
    }

    // Get most recent migration
    var migration = config.changelog.find({what: /^move.*/}).sort({time: -1}).limit(1).toArray();
    if (migration.length == 0)
        print("\nNo migrations found in changelog.");
    else {
        migration = migration[0];
        print("\nMost recent migration activity was on " + migration.ns + " at " + migration.time);
    }

    var admin = this.getMongo().getDB("admin");
    var coll = this;
    var splitFunction = function() {

        // Turn off the balancer, just to be safe
        print("Turning off balancer...");
        config.settings.update({_id: "balancer"}, {$set: {stopped: true}}, true);
        print(
            "Sleeping for 30s to allow balancers to detect change.  To be extra safe, check config.changelog" +
            " for recent migrations.");
        sleep(30000);

        for (var shard in allSplitPoints) {
            for (var i = 0; i < allSplitPoints[shard].length; i++) {
                var splitKey = allSplitPoints[shard][i];
                print("Splitting at " + tojson(splitKey));
                printjson(admin.runCommand({split: coll + "", middle: splitKey}));
            }
        }

        print("Turning the balancer back on.");
        config.settings.update({_id: "balancer"}, {$set: {stopped: false}});
        sleep(1);
    };

    splitFunction.getSplitPoints = function() {
        return allSplitPoints;
    };

    print("\nGenerated " + numSplits + " split keys, run output function to perform splits.\n" +
          " ex : \n" + "  > var splitter = <collection>.getSplitKeysForChunks()\n" +
          "  > splitter() // Execute splits on cluster !\n");

    return splitFunction;

};

DBCollection.prototype.setSlaveOk = function(value) {
    if (value == undefined)
        value = true;
    this._slaveOk = value;
};

DBCollection.prototype.getSlaveOk = function() {
    if (this._slaveOk != undefined)
        return this._slaveOk;
    return this._db.getSlaveOk();
};

DBCollection.prototype.getQueryOptions = function() {
    // inherit this method from DB but use apply so
    // that slaveOk will be set if is overridden on this DBCollection
    return this._db.getQueryOptions.apply(this, arguments);
};

/**
 * Returns a PlanCache for the collection.
 */
DBCollection.prototype.getPlanCache = function() {
    return new PlanCache(this);
};

// Overrides connection-level settings.
//

DBCollection.prototype.setWriteConcern = function(wc) {
    if (wc instanceof WriteConcern) {
        this._writeConcern = wc;
    } else {
        this._writeConcern = new WriteConcern(wc);
    }
};

DBCollection.prototype.getWriteConcern = function() {
    if (this._writeConcern)
        return this._writeConcern;

    if (this._db.getWriteConcern())
        return this._db.getWriteConcern();

    return null;
};

DBCollection.prototype.unsetWriteConcern = function() {
    delete this._writeConcern;
};

//
// CRUD specification read methods
//

/**
* Count number of matching documents in the db to a query.
*
* @method
* @param {object} query The query for the count.
* @param {object} [options=null] Optional settings.
* @param {number} [options.limit=null] The limit of documents to count.
* @param {number} [options.skip=null] The number of documents to skip for the count.
* @param {string|object} [options.hint=null] An index name hint or specification for the query.
* @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
* @param {string} [options.readConcern=null] The level of readConcern passed to the count command
* @param {object} [options.collation=null] The collation that should be used for string comparisons
* for this count op.
* @return {number}
*/
DBCollection.prototype.count = function(query, options) {
    var opts = Object.extend({}, options || {});

    var query = this.find(query);
    if (typeof opts.skip == 'number') {
        query.skip(opts.skip);
    }

    if (typeof opts.limit == 'number') {
        query.limit(opts.limit);
    }

    if (typeof opts.maxTimeMS == 'number') {
        query.maxTimeMS(opts.maxTimeMS);
    }

    if (opts.hint) {
        query.hint(opts.hint);
    }

    if (typeof opts.readConcern == 'string') {
        query.readConcern(opts.readConcern);
    }

    if (typeof opts.collation == 'object') {
        query.collation(opts.collation);
    }

    // Return the result of the find
    return query.count(true);
};

/**
* The distinct command returns returns a list of distinct values for the given key across a
*collection.
*
* @method
* @param {string} key Field of the document to find distinct values for.
* @param {object} query The query for filtering the set of documents to which we apply the distinct
*filter.
* @param {object} [options=null] Optional settings.
* @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
* @return {object}
*/
DBCollection.prototype.distinct = function(keyString, query, options) {
    var opts = Object.extend({}, options || {});
    var keyStringType = typeof keyString;
    var queryType = typeof query;

    if (keyStringType != "string") {
        throw new Error("The first argument to the distinct command must be a string but was a " +
                        keyStringType);
    }

    if (query != null && queryType != "object") {
        throw new Error("The query argument to the distinct command must be a document but was a " +
                        queryType);
    }

    // Distinct command
    var cmd = {
        distinct: this.getName(),
        key: keyString,
        query: query || {}
    };

    // Set maxTimeMS if provided
    if (opts.maxTimeMS) {
        cmd.maxTimeMS = opts.maxTimeMS;
    }

    if (opts.collation) {
        cmd.collation = opts.collation;
    }

    // Execute distinct command
    var res = this.runReadCommand(cmd);
    if (!res.ok) {
        throw new Error("distinct failed: " + tojson(res));
    }

    return res.values;
};

DBCollection.prototype._distinct = function(keyString, query) {
    return this._dbReadCommand({distinct: this._shortName, key: keyString, query: query || {}});
};

/**
 * PlanCache
 * Holds a reference to the collection.
 * Proxy for planCache* commands.
 */
if ((typeof PlanCache) == "undefined") {
    PlanCache = function(collection) {
        this._collection = collection;
    };
}

/**
 * Name of PlanCache.
 * Same as collection.
 */
PlanCache.prototype.getName = function() {
    return this._collection.getName();
};

/**
 * toString prints the name of the collection
 */
PlanCache.prototype.toString = function() {
    return "PlanCache for collection " + this.getName() + '. Type help() for more info.';
};

PlanCache.prototype.shellPrint = PlanCache.prototype.toString;

/**
 * Displays help for a PlanCache object.
 */
PlanCache.prototype.help = function() {
    var shortName = this.getName();
    print("PlanCache help");
    print("\tdb." + shortName + ".getPlanCache().help() - show PlanCache help");
    print("\tdb." + shortName + ".getPlanCache().listQueryShapes() - " +
          "displays all query shapes in a collection");
    print("\tdb." + shortName + ".getPlanCache().clear() - " +
          "drops all cached queries in a collection");
    print("\tdb." + shortName + ".getPlanCache().clearPlansByQuery(query[, projection, sort]) - " +
          "drops query shape from plan cache");
    print("\tdb." + shortName + ".getPlanCache().getPlansByQuery(query[, projection, sort]) - " +
          "displays the cached plans for a query shape");
    return __magicNoPrint;
};

/**
 * Internal function to parse query shape.
 */
PlanCache.prototype._parseQueryShape = function(query, projection, sort) {
    if (query == undefined) {
        throw new Error("required parameter query missing");
    }

    // Accept query shape object as only argument.
    // Query shape contains exactly 3 fields (query, projection and sort)
    // as generated in the listQueryShapes() result.
    if (typeof(query) == 'object' && projection == undefined && sort == undefined) {
        var keysSorted = Object.keys(query).sort();
        // Expected keys must be sorted for the comparison to work.
        if (bsonWoCompare(keysSorted, ['projection', 'query', 'sort']) == 0) {
            return query;
        }
    }

    // Extract query shape, projection and sort from DBQuery if it is the first
    // argument. If a sort or projection is provided in addition to DBQuery, do not
    // overwrite with the DBQuery value.
    if (query instanceof DBQuery) {
        if (projection != undefined) {
            throw new Error("cannot pass DBQuery with projection");
        }
        if (sort != undefined) {
            throw new Error("cannot pass DBQuery with sort");
        }

        var queryObj = query._query["query"] || {};
        projection = query._fields || {};
        sort = query._query["orderby"] || {};
        // Overwrite DBQuery with the BSON query.
        query = queryObj;
    }

    var shape = {
        query: query,
        projection: projection == undefined ? {} : projection,
        sort: sort == undefined ? {} : sort,
    };
    return shape;
};

/**
 * Internal function to run command.
 */
PlanCache.prototype._runCommandThrowOnError = function(cmd, params) {
    var res = this._collection.runCommand(cmd, params);
    if (!res.ok) {
        throw new Error(res.errmsg);
    }
    return res;
};

/**
 * Lists query shapes in a collection.
 */
PlanCache.prototype.listQueryShapes = function() {
    return this._runCommandThrowOnError("planCacheListQueryShapes", {}).shapes;
};

/**
 * Clears plan cache in a collection.
 */
PlanCache.prototype.clear = function() {
    this._runCommandThrowOnError("planCacheClear", {});
    return;
};

/**
 * List plans for a query shape.
 */
PlanCache.prototype.getPlansByQuery = function(query, projection, sort) {
    return this._runCommandThrowOnError("planCacheListPlans",
                                        this._parseQueryShape(query, projection, sort)).plans;
};

/**
 * Drop query shape from the plan cache.
 */
PlanCache.prototype.clearPlansByQuery = function(query, projection, sort) {
    this._runCommandThrowOnError("planCacheClear", this._parseQueryShape(query, projection, sort));
    return;
};



// ---- MODULE: bridge ---- 
/**
 * Wrapper around a mongobridge process. Construction of a MongoBridge instance will start a new
 * mongobridge process that listens on 'options.port' and forwards messages to 'options.dest'.
 *
 * @param {Object} options
 * @param {string} options.dest - The host:port to forward messages to.
 * @param {string} [options.hostName=localhost] - The hostname to specify when connecting to the
 * mongobridge process.
 * @param {number} [options.port=allocatePort()] - The port number the mongobridge should listen on.
 *
 * @returns {Proxy} Acts as a typical connection object to options.hostName:options.port that has
 * additional functions exposed to shape network traffic from other processes.
 */
function MongoBridge(options) {
    'use strict';

    if (!(this instanceof MongoBridge)) {
        return new MongoBridge(options);
    }

    options = options || {};
    if (!options.hasOwnProperty('dest')) {
        throw new Error('Missing required field "dest"');
    }

    var hostName = options.hostName || 'localhost';

    this.dest = options.dest;
    this.port = options.port || allocatePort();

    // The connection used by a test for running commands against the mongod or mongos process.
    var userConn;

    // A separate (hidden) connection for configuring the mongobridge process.
    var controlConn;

    // Start the mongobridge on port 'this.port' routing network traffic to 'this.dest'.
    var args = ['mongobridge', '--port', this.port, '--dest', this.dest];
    var keysToSkip = ['dest', 'hostName', 'port', ];

    // Append any command line arguments that are optional for mongobridge.
    Object.keys(options).forEach(function(key) {
        if (Array.contains(keysToSkip, key)) {
            return;
        }

        var value = options[key];
        if (value === null || value === undefined) {
            throw new Error("Value '" + value + "' for '" + key + "' option is ambiguous; specify" +
                            " {flag: ''} to add --flag command line options'");
        }

        args.push('--' + key);
        if (value !== '') {
            args.push(value.toString());
        }
    });

    var pid = _startMongoProgram.apply(null, args);

    /**
     * Initializes the mongo shell's connections to the mongobridge process. Throws an error if the
     * mongobridge process stopped running or if a connection cannot be made.
     *
     * The mongod or mongos process corresponding to this mongobridge process may need to connect to
     * itself through the mongobridge process, e.g. when running the _isSelf command. This means
     * the mongobridge process needs to be running prior to the other process. However, to avoid
     * spurious failures during situations where the mongod or mongos process is not ready to accept
     * connections, connections to the mongobridge process should only be made after the other
     * process is known to be reachable:
     *
     *     var bridge = new MongoBridge(...);
     *     var conn = MongoRunner.runMongoXX(...);
     *     assert.neq(null, conn);
     *     bridge.connectToBridge();
     */
    this.connectToBridge = function connectToBridge() {
        var failedToStart = false;
        assert.soon(() => {
            if (!checkProgram(pid)) {
                failedToStart = true;
                return true;
            }

            try {
                userConn = new Mongo(hostName + ':' + this.port);
            } catch (e) {
                return false;
            }
            return true;
        }, 'failed to connect to the mongobridge on port ' + this.port);
        assert(!failedToStart, 'mongobridge failed to start on port ' + this.port);

        // The MongoRunner.runMongoXX() functions define a 'name' property on the returned
        // connection object that is equivalent to its 'host' property. Certain functions in
        // ReplSetTest and ShardingTest use the 'name' property instead of the 'host' property, so
        // we define it here for consistency.
        Object.defineProperty(userConn,
                              'name',
                              {
                                enumerable: true,
                                get: function() {
                                    return this.host;
                                },
                              });

        controlConn = new Mongo(hostName + ':' + this.port);
    };

    /**
     * Terminates the mongobridge process.
     */
    this.stop = function stop() {
        _stopMongoProgram(this.port);
    };

    // Throws an error if 'obj' is not a MongoBridge instance.
    function throwErrorIfNotMongoBridgeInstance(obj) {
        if (!(obj instanceof MongoBridge)) {
            throw new Error('Expected MongoBridge instance, but got ' + tojson(obj));
        }
    }

    // Runs a command intended to configure the mongobridge.
    function runBridgeCommand(conn, cmdName, cmdArgs) {
        // The wire version of this mongobridge is detected as the wire version of the corresponding
        // mongod or mongos process because the message is simply forwarded to that process.
        // Commands to configure the mongobridge process must support being sent as an OP_QUERY
        // message in order to handle when the mongobridge is a proxy for a mongos process or when
        // --readMode=legacy is passed to the mongo shell. Create a new Object with 'cmdName' as the
        // first key and $forBridge=true.
        var cmdObj = {};
        cmdObj[cmdName] = 1;
        cmdObj.$forBridge = true;
        Object.extend(cmdObj, cmdArgs);

        var dbName = 'test';
        var noQueryOptions = 0;
        return conn.runCommand(dbName, cmdObj, noQueryOptions);
    }

    /**
     * Allows communication between 'this.dest' and the 'dest' of each of the 'bridges'.
     *
     * Configures 'this' bridge to accept new connections from the 'dest' of each of the 'bridges'.
     * Additionally configures each of the 'bridges' to accept new connections from 'this.dest'.
     *
     * @param {(MongoBridge|MongoBridge[])} bridges
     */
    this.reconnect = function reconnect(bridges) {
        if (!Array.isArray(bridges)) {
            bridges = [bridges];
        }
        bridges.forEach(throwErrorIfNotMongoBridgeInstance);

        this.acceptConnectionsFrom(bridges);
        bridges.forEach(bridge => bridge.acceptConnectionsFrom(this));
    };

    /**
     * Disallows communication between 'this.dest' and the 'dest' of each of the 'bridges'.
     *
     * Configures 'this' bridge to close existing connections and reject new connections from the
     * 'dest' of each of the 'bridges'. Additionally configures each of the 'bridges' to close
     * existing connections and reject new connections from 'this.dest'.
     *
     * @param {(MongoBridge|MongoBridge[])} bridges
     */
    this.disconnect = function disconnect(bridges) {
        if (!Array.isArray(bridges)) {
            bridges = [bridges];
        }
        bridges.forEach(throwErrorIfNotMongoBridgeInstance);

        this.rejectConnectionsFrom(bridges);
        bridges.forEach(bridge => bridge.rejectConnectionsFrom(this));
    };

    /**
     * Configures 'this' bridge to accept new connections from the 'dest' of each of the 'bridges'.
     *
     * @param {(MongoBridge|MongoBridge[])} bridges
     */
    this.acceptConnectionsFrom = function acceptConnectionsFrom(bridges) {
        if (!Array.isArray(bridges)) {
            bridges = [bridges];
        }
        bridges.forEach(throwErrorIfNotMongoBridgeInstance);

        bridges.forEach(bridge => {
            var res = runBridgeCommand(controlConn, 'acceptConnectionsFrom', {host: bridge.dest});
            assert.commandWorked(res,
                                 'failed to configure the mongobridge listening on port ' +
                                     this.port + ' to accept new connections from ' + bridge.dest);
        });
    };

    /**
     * Configures 'this' bridge to close existing connections and reject new connections from the
     * 'dest' of each of the 'bridges'.
     *
     * @param {(MongoBridge|MongoBridge[])} bridges
     */
    this.rejectConnectionsFrom = function rejectConnectionsFrom(bridges) {
        if (!Array.isArray(bridges)) {
            bridges = [bridges];
        }
        bridges.forEach(throwErrorIfNotMongoBridgeInstance);

        bridges.forEach(bridge => {
            var res = runBridgeCommand(controlConn, 'rejectConnectionsFrom', {host: bridge.dest});
            assert.commandWorked(res,
                                 'failed to configure the mongobridge listening on port ' +
                                     this.port + ' to hang up connections from ' + bridge.dest);
        });
    };

    /**
     * Configures 'this' bridge to delay forwarding requests from the 'dest' of each of the
     * 'bridges' to 'this.dest' by the specified amount.
     *
     * @param {(MongoBridge|MongoBridge[])} bridges
     * @param {number} delay - The delay to apply in milliseconds.
     */
    this.delayMessagesFrom = function delayMessagesFrom(bridges, delay) {
        if (!Array.isArray(bridges)) {
            bridges = [bridges];
        }
        bridges.forEach(throwErrorIfNotMongoBridgeInstance);

        bridges.forEach(bridge => {
            var res = runBridgeCommand(controlConn,
                                       'delayMessagesFrom',
                                       {
                                         host: bridge.dest,
                                         delay: delay,
                                       });
            assert.commandWorked(res,
                                 'failed to configure the mongobridge listening on port ' +
                                     this.port + ' to delay messages from ' + bridge.dest + ' by ' +
                                     delay + ' milliseconds');
        });
    };

    /**
     * Configures 'this' bridge to uniformly discard requests from the 'dest' of each of the
     * 'bridges' to 'this.dest' with probability 'lossProbability'.
     *
     * @param {(MongoBridge|MongoBridge[])} bridges
     * @param {number} lossProbability
     */
    this.discardMessagesFrom = function discardMessagesFrom(bridges, lossProbability) {
        if (!Array.isArray(bridges)) {
            bridges = [bridges];
        }
        bridges.forEach(throwErrorIfNotMongoBridgeInstance);

        bridges.forEach(bridge => {
            var res = runBridgeCommand(controlConn,
                                       'discardMessagesFrom',
                                       {
                                         host: bridge.dest,
                                         loss: lossProbability,
                                       });
            assert.commandWorked(res,
                                 'failed to configure the mongobridge listening on port ' +
                                     this.port + ' to discard messages from ' + bridge.dest +
                                     ' with probability ' + lossProbability);
        });
    };

    // Use a Proxy to "extend" the underlying connection object. The C++ functions, e.g.
    // runCommand(), require that they are called on the Mongo instance itself and so typical
    // prototypical inheritance isn't possible.
    return new Proxy(this,
                     {
                       get: function get(target, property, receiver) {
                           // If the property is defined on the MongoBridge instance itself, then
                           // return it.
                           // Otherwise, get the value of the property from the Mongo instance.
                           if (target.hasOwnProperty(property)) {
                               return target[property];
                           }
                           var value = userConn[property];
                           if (typeof value === 'function') {
                               return value.bind(userConn);
                           }
                           return value;
                       },

                       set: function set(target, property, value, receiver) {
                           // Delegate setting the value of any property to the Mongo instance so
                           // that it can be
                           // accessed in functions acting on the Mongo instance directly instead of
                           // this Proxy.
                           // For example, the "slaveOk" property needs to be set on the Mongo
                           // instance in order
                           // for the query options bit to be set correctly.
                           userConn[property] = value;
                           return true;
                       },
                     });
}



// ---- MODULE: bulk_api ---- 
//
// Scope for the function
//
var _bulk_api_module = (function() {

    // Batch types
    var NONE = 0;
    var INSERT = 1;
    var UPDATE = 2;
    var REMOVE = 3;

    // Error codes
    var UNKNOWN_ERROR = 8;
    var WRITE_CONCERN_FAILED = 64;
    var UNKNOWN_REPL_WRITE_CONCERN = 79;
    var NOT_MASTER = 10107;

    // Constants
    var IndexCollPattern = new RegExp('system\.indexes$');

    /**
     * Helper function to define properties
     */
    var defineReadOnlyProperty = function(self, name, value) {
        Object.defineProperty(self,
                              name,
                              {
                                enumerable: true,
                                get: function() {
                                    return value;
                                }
                              });
    };

    /**
     * Shell representation of WriteConcern, possibly includes:
     *  j: write waits for journal
     *  w: write waits until replicated to number of servers (including primary), or mode (string)
     *  wtimeout: how long to wait for "w" replication
     *  fsync: waits for data flush (either journal, nor database files depending on server conf)
     *
     * Accepts { w : x, j : x, wtimeout : x, fsync: x } or w, wtimeout, j
     */
    var WriteConcern = function(wValue, wTimeout, jValue) {

        if (!(this instanceof WriteConcern)) {
            var writeConcern = Object.create(WriteConcern.prototype);
            WriteConcern.apply(writeConcern, arguments);
            return writeConcern;
        }

        var opts = {};
        if (typeof wValue == 'object') {
            if (arguments.length == 1)
                opts = Object.merge(wValue);
            else
                throw Error("If the first arg is an Object then no additional args are allowed!");
        } else {
            if (typeof wValue != 'undefined')
                opts.w = wValue;
            if (typeof wTimeout != 'undefined')
                opts.wtimeout = wTimeout;
            if (typeof jValue != 'undefined')
                opts.j = jValue;
        }

        // Do basic validation.
        if (typeof opts.w != 'undefined' && typeof opts.w != 'number' && typeof opts.w != 'string')
            throw Error("w value must be a number or string but was found to be a " +
                        typeof opts.w);
        if (typeof opts.w == 'number' && NumberInt(opts.w).toNumber() < 0)
            throw Error("Numeric w value must be equal to or larger than 0, not " + opts.w);

        if (typeof opts.wtimeout != 'undefined') {
            if (typeof opts.wtimeout != 'number')
                throw Error("wtimeout must be a number, not " + opts.wtimeout);
            if (NumberInt(opts.wtimeout).toNumber() < 0)
                throw Error("wtimeout must be a number greater than or equal to 0, not " +
                            opts.wtimeout);
        }

        if (typeof opts.j != 'undefined' && typeof opts.j != 'boolean')
            throw Error("j value must be true or false if defined, not " + opts.j);

        this._wc = opts;

        this.toJSON = function() {
            return Object.merge({}, this._wc);
        };

        /**
         * @return {string}
         */
        this.tojson = function(indent, nolint) {
            return tojson(this.toJSON(), indent, nolint);
        };

        this.toString = function() {
            return "WriteConcern(" + this.tojson() + ")";
        };

        this.shellPrint = function() {
            return this.toString();
        };

    };

    /**
     * Wraps the result for write commands and presents a convenient api for accessing
     * single results & errors (returns the last one if there are multiple).
     * singleBatchType is passed in on bulk operations consisting of a single batch and
     * are used to filter the WriteResult to only include relevant result fields.
     */
    var WriteResult = function(bulkResult, singleBatchType, writeConcern) {

        if (!(this instanceof WriteResult))
            return new WriteResult(bulkResult, singleBatchType, writeConcern);

        // Define properties
        defineReadOnlyProperty(this, "ok", bulkResult.ok);
        defineReadOnlyProperty(this, "nInserted", bulkResult.nInserted);
        defineReadOnlyProperty(this, "nUpserted", bulkResult.nUpserted);
        defineReadOnlyProperty(this, "nMatched", bulkResult.nMatched);
        defineReadOnlyProperty(this, "nModified", bulkResult.nModified);
        defineReadOnlyProperty(this, "nRemoved", bulkResult.nRemoved);

        //
        // Define access methods
        this.getUpsertedId = function() {
            if (bulkResult.upserted.length == 0) {
                return null;
            }

            return bulkResult.upserted[bulkResult.upserted.length - 1];
        };

        this.getRawResponse = function() {
            return bulkResult;
        };

        this.getWriteError = function() {
            if (bulkResult.writeErrors.length == 0) {
                return null;
            } else {
                return bulkResult.writeErrors[bulkResult.writeErrors.length - 1];
            }
        };

        this.hasWriteError = function() {
            return this.getWriteError() != null;
        };

        this.getWriteConcernError = function() {
            if (bulkResult.writeConcernErrors.length == 0) {
                return null;
            } else {
                return bulkResult.writeConcernErrors[0];
            }
        };

        this.hasWriteConcernError = function() {
            return this.getWriteConcernError() != null;
        };

        /**
         * @return {string}
         */
        this.tojson = function(indent, nolint) {
            var result = {};

            if (singleBatchType == INSERT) {
                result.nInserted = this.nInserted;
            }

            if (singleBatchType == UPDATE) {
                result.nMatched = this.nMatched;
                result.nUpserted = this.nUpserted;

                if (this.nModified != undefined)
                    result.nModified = this.nModified;

                if (Array.isArray(bulkResult.upserted) && bulkResult.upserted.length == 1) {
                    result._id = bulkResult.upserted[0]._id;
                }
            }

            if (singleBatchType == REMOVE) {
                result.nRemoved = bulkResult.nRemoved;
            }

            if (this.getWriteError() != null) {
                result.writeError = {};
                result.writeError.code = this.getWriteError().code;
                result.writeError.errmsg = this.getWriteError().errmsg;
            }

            if (this.getWriteConcernError() != null) {
                result.writeConcernError = this.getWriteConcernError();
            }

            return tojson(result, indent, nolint);
        };

        this.toString = function() {
            // Suppress all output for the write concern w:0, since the client doesn't care.
            if (writeConcern && writeConcern.w == 0) {
                return "WriteResult(" + tojson({}) + ")";
            }
            return "WriteResult(" + this.tojson() + ")";
        };

        this.shellPrint = function() {
            return this.toString();
        };
    };

    /**
     * Wraps the result for the commands
     */
    var BulkWriteResult = function(bulkResult, singleBatchType, writeConcern) {

        if (!(this instanceof BulkWriteResult) && !(this instanceof BulkWriteError))
            return new BulkWriteResult(bulkResult, singleBatchType, writeConcern);

        // Define properties
        defineReadOnlyProperty(this, "ok", bulkResult.ok);
        defineReadOnlyProperty(this, "nInserted", bulkResult.nInserted);
        defineReadOnlyProperty(this, "nUpserted", bulkResult.nUpserted);
        defineReadOnlyProperty(this, "nMatched", bulkResult.nMatched);
        defineReadOnlyProperty(this, "nModified", bulkResult.nModified);
        defineReadOnlyProperty(this, "nRemoved", bulkResult.nRemoved);

        //
        // Define access methods
        this.getUpsertedIds = function() {
            return bulkResult.upserted;
        };

        this.getUpsertedIdAt = function(index) {
            return bulkResult.upserted[index];
        };

        this.getRawResponse = function() {
            return bulkResult;
        };

        this.hasWriteErrors = function() {
            return bulkResult.writeErrors.length > 0;
        };

        this.getWriteErrorCount = function() {
            return bulkResult.writeErrors.length;
        };

        this.getWriteErrorAt = function(index) {
            if (index < bulkResult.writeErrors.length) {
                return bulkResult.writeErrors[index];
            }
            return null;
        };

        //
        // Get all errors
        this.getWriteErrors = function() {
            return bulkResult.writeErrors;
        };

        this.hasWriteConcernError = function() {
            return bulkResult.writeConcernErrors.length > 0;
        };

        this.getWriteConcernError = function() {
            if (bulkResult.writeConcernErrors.length == 0) {
                return null;
            } else if (bulkResult.writeConcernErrors.length == 1) {
                // Return the error
                return bulkResult.writeConcernErrors[0];
            } else {
                // Combine the errors
                var errmsg = "";
                for (var i = 0; i < bulkResult.writeConcernErrors.length; i++) {
                    var err = bulkResult.writeConcernErrors[i];
                    errmsg = errmsg + err.errmsg;
                    // TODO: Something better
                    if (i != bulkResult.writeConcernErrors.length - 1) {
                        errmsg = errmsg + " and ";
                    }
                }

                return new WriteConcernError({errmsg: errmsg, code: WRITE_CONCERN_FAILED});
            }
        };

        /**
         * @return {string}
         */
        this.tojson = function(indent, nolint) {
            return tojson(bulkResult, indent, nolint);
        };

        this.toString = function() {
            // Suppress all output for the write concern w:0, since the client doesn't care.
            if (writeConcern && writeConcern.w == 0) {
                return "BulkWriteResult(" + tojson({}) + ")";
            }
            return "BulkWriteResult(" + this.tojson() + ")";
        };

        this.shellPrint = function() {
            return this.toString();
        };

        this.hasErrors = function() {
            return this.hasWriteErrors() || this.hasWriteConcernError();
        };

        this.toError = function() {
            if (this.hasErrors()) {
                // Create a combined error message
                var message = "";
                var numWriteErrors = this.getWriteErrorCount();
                if (numWriteErrors == 1) {
                    message += "write error at item " + this.getWriteErrors()[0].index;
                } else if (numWriteErrors > 1) {
                    message += numWriteErrors + " write errors";
                }

                var hasWCError = this.hasWriteConcernError();
                if (numWriteErrors > 0 && hasWCError) {
                    message += " and ";
                }

                if (hasWCError) {
                    message += "problem enforcing write concern";
                }
                message += " in bulk operation";

                return new BulkWriteError(bulkResult, singleBatchType, writeConcern, message);
            } else {
                throw Error("batch was successful, cannot create BulkWriteError");
            }
        };

        /**
         * @return {WriteResult} the simplified results condensed into one.
         */
        this.toSingleResult = function() {
            if (singleBatchType == null)
                throw Error("Cannot output single WriteResult from multiple batch result");
            return new WriteResult(bulkResult, singleBatchType, writeConcern);
        };
    };

    /**
     * Represents a bulk write error, identical to a BulkWriteResult but thrown
     */
    var BulkWriteError = function(bulkResult, singleBatchType, writeConcern, message) {

        if (!(this instanceof BulkWriteError))
            return new BulkWriteError(bulkResult, singleBatchType, writeConcern, message);

        this.name = 'BulkWriteError';
        this.message = message || 'unknown bulk write error';

        // Bulk errors are basically bulk results with additional error information
        BulkWriteResult.apply(this, arguments);

        // Override some particular methods
        delete this.toError;

        this.toString = function() {
            return "BulkWriteError(" + this.tojson() + ")";
        };
        this.stack = this.toString() + "\n" + (new Error().stack);

        this.toResult = function() {
            return new BulkWriteResult(bulkResult, singleBatchType, writeConcern);
        };
    };

    BulkWriteError.prototype = new Error();
    BulkWriteError.prototype.constructor = BulkWriteError;

    var getEmptyBulkResult = function() {
        return {
            writeErrors: [],
            writeConcernErrors: [],
            nInserted: 0,
            nUpserted: 0,
            nMatched: 0,
            nModified: 0,
            nRemoved: 0,
            upserted: []
        };
    };

    /**
     * Wraps a command error
     */
    var WriteCommandError = function(commandError) {

        if (!(this instanceof WriteCommandError))
            return new WriteCommandError(commandError);

        // Define properties
        defineReadOnlyProperty(this, "code", commandError.code);
        defineReadOnlyProperty(this, "errmsg", commandError.errmsg);

        this.name = 'WriteCommandError';
        this.message = this.errmsg;

        /**
         * @return {string}
         */
        this.tojson = function(indent, nolint) {
            return tojson(commandError, indent, nolint);
        };

        this.toString = function() {
            return "WriteCommandError(" + this.tojson() + ")";
        };
        this.stack = this.toString() + "\n" + (new Error().stack);

        this.shellPrint = function() {
            return this.toString();
        };

        this.toSingleResult = function() {
            // This is *only* safe to do with a WriteCommandError from the bulk api when the bulk is
            // known to be of size == 1
            var bulkResult = getEmptyBulkResult();
            bulkResult.writeErrors.push({code: this.code, index: 0, errmsg: this.errmsg});
            return new BulkWriteResult(bulkResult, NONE).toSingleResult();
        };
    };

    WriteCommandError.prototype = new Error();
    WriteCommandError.prototype.constructor = WriteCommandError;

    /**
     * Wraps an error for a single write
     */
    var WriteError = function(err) {
        if (!(this instanceof WriteError))
            return new WriteError(err);

        // Define properties
        defineReadOnlyProperty(this, "code", err.code);
        defineReadOnlyProperty(this, "index", err.index);
        defineReadOnlyProperty(this, "errmsg", err.errmsg);

        //
        // Define access methods
        this.getOperation = function() {
            return err.op;
        };

        /**
         * @return {string}
         */
        this.tojson = function(indent, nolint) {
            return tojson(err, indent, nolint);
        };

        this.toString = function() {
            return "WriteError(" + tojson(err) + ")";
        };

        this.shellPrint = function() {
            return this.toString();
        };
    };

    /**
     * Wraps a write concern error
     */
    var WriteConcernError = function(err) {
        if (!(this instanceof WriteConcernError))
            return new WriteConcernError(err);

        // Define properties
        defineReadOnlyProperty(this, "code", err.code);
        defineReadOnlyProperty(this, "errInfo", err.errInfo);
        defineReadOnlyProperty(this, "errmsg", err.errmsg);

        /**
         * @return {string}
         */
        this.tojson = function(indent, nolint) {
            return tojson(err, indent, nolint);
        };

        this.toString = function() {
            return "WriteConcernError(" + tojson(err) + ")";
        };

        this.shellPrint = function() {
            return this.toString();
        };
    };

    /**
     * Keeps the state of an unordered batch so we can rewrite the results
     * correctly after command execution
     */
    var Batch = function(batchType, originalZeroIndex) {
        this.originalZeroIndex = originalZeroIndex;
        this.batchType = batchType;
        this.operations = [];
    };

    /**
     * Wraps a legacy operation so we can correctly rewrite its error
     */
    var LegacyOp = function(batchType, operation, index) {
        this.batchType = batchType;
        this.index = index;
        this.operation = operation;
    };

    /***********************************************************
     * Wraps the operations done for the batch
     ***********************************************************/
    var Bulk = function(collection, ordered) {
        var self = this;
        var coll = collection;
        var executed = false;

        // Set max byte size
        var maxBatchSizeBytes = 1024 * 1024 * 16;
        var maxNumberOfDocsInBatch = 1000;
        var writeConcern = null;
        var currentOp;

        // Final results
        var bulkResult = getEmptyBulkResult();

        // Current batch
        var currentBatch = null;
        var currentIndex = 0;
        var currentBatchSize = 0;
        var currentBatchSizeBytes = 0;
        var batches = [];

        var defineBatchTypeCounter = function(self, name, type) {
            Object.defineProperty(self,
                                  name,
                                  {
                                    enumerable: true,
                                    get: function() {
                                        var counter = 0;

                                        for (var i = 0; i < batches.length; i++) {
                                            if (batches[i].batchType == type) {
                                                counter += batches[i].operations.length;
                                            }
                                        }

                                        if (currentBatch && currentBatch.batchType == type) {
                                            counter += currentBatch.operations.length;
                                        }

                                        return counter;
                                    }
                                  });
        };

        defineBatchTypeCounter(this, "nInsertOps", INSERT);
        defineBatchTypeCounter(this, "nUpdateOps", UPDATE);
        defineBatchTypeCounter(this, "nRemoveOps", REMOVE);

        // Convert bulk into string
        this.toString = function() {
            return this.tojson();
        };

        this.tojson = function() {
            return tojson({
                nInsertOps: this.nInsertOps,
                nUpdateOps: this.nUpdateOps,
                nRemoveOps: this.nRemoveOps,
                nBatches: batches.length + (currentBatch == null ? 0 : 1)
            });
        };

        this.getOperations = function() {
            return batches;
        };

        var finalizeBatch = function(newDocType) {
            // Save the batch to the execution stack
            batches.push(currentBatch);

            // Create a new batch
            currentBatch = new Batch(newDocType, currentIndex);

            // Reset the current size trackers
            currentBatchSize = 0;
            currentBatchSizeBytes = 0;
        };

        // Add to internal list of documents
        var addToOperationsList = function(docType, document) {

            if (Array.isArray(document))
                throw Error("operation passed in cannot be an Array");

            // Get the bsonSize
            var bsonSize = Object.bsonsize(document);

            // Create a new batch object if we don't have a current one
            if (currentBatch == null)
                currentBatch = new Batch(docType, currentIndex);

            // Finalize and create a new batch if this op would take us over the
            // limits *or* if this op is of a different type
            if (currentBatchSize + 1 > maxNumberOfDocsInBatch ||
                (currentBatchSize > 0 && currentBatchSizeBytes + bsonSize >= maxBatchSizeBytes) ||
                currentBatch.batchType != docType) {
                finalizeBatch(docType);
            }

            currentBatch.operations.push(document);
            currentIndex = currentIndex + 1;
            // Update current batch size
            currentBatchSize = currentBatchSize + 1;
            currentBatchSizeBytes = currentBatchSizeBytes + bsonSize;
        };

        /**
         * @return {Object} a new document with an _id: ObjectId if _id is not present.
         *     Otherwise, returns the same object passed.
         */
        var addIdIfNeeded = function(obj) {
            if (typeof(obj._id) == "undefined" && !Array.isArray(obj)) {
                var tmp = obj;  // don't want to modify input
                obj = {
                    _id: new ObjectId()
                };
                for (var key in tmp) {
                    obj[key] = tmp[key];
                }
            }

            return obj;
        };

        /**
         * Add the insert document.
         *
         * @param document {Object} the document to insert.
         */
        this.insert = function(document) {
            if (!IndexCollPattern.test(coll.getName())) {
                collection._validateForStorage(document);
            }

            return addToOperationsList(INSERT, document);
        };

        //
        // Find based operations
        var findOperations = {
            update: function(updateDocument) {
                collection._validateUpdateDoc(updateDocument);

                // Set the top value for the update 0 = multi true, 1 = multi false
                var upsert = typeof currentOp.upsert == 'boolean' ? currentOp.upsert : false;
                // Establish the update command
                var document = {
                    q: currentOp.selector,
                    u: updateDocument,
                    multi: true,
                    upsert: upsert
                };

                // Copy over the collation, if we have one.
                if (currentOp.hasOwnProperty('collation')) {
                    document.collation = currentOp.collation;
                }

                // Clear out current Op
                currentOp = null;
                // Add the update document to the list
                return addToOperationsList(UPDATE, document);
            },

            updateOne: function(updateDocument) {
                collection._validateUpdateDoc(updateDocument);

                // Set the top value for the update 0 = multi true, 1 = multi false
                var upsert = typeof currentOp.upsert == 'boolean' ? currentOp.upsert : false;
                // Establish the update command
                var document = {
                    q: currentOp.selector,
                    u: updateDocument,
                    multi: false,
                    upsert: upsert
                };

                // Copy over the collation, if we have one.
                if (currentOp.hasOwnProperty('collation')) {
                    document.collation = currentOp.collation;
                }

                // Clear out current Op
                currentOp = null;
                // Add the update document to the list
                return addToOperationsList(UPDATE, document);
            },

            replaceOne: function(updateDocument) {
                findOperations.updateOne(updateDocument);
            },

            upsert: function() {
                currentOp.upsert = true;
                // Return the findOperations
                return findOperations;
            },

            removeOne: function() {
                collection._validateRemoveDoc(currentOp.selector);

                // Establish the removeOne command
                var document = {
                    q: currentOp.selector,
                    limit: 1
                };

                // Copy over the collation, if we have one.
                if (currentOp.hasOwnProperty('collation')) {
                    document.collation = currentOp.collation;
                }

                // Clear out current Op
                currentOp = null;
                // Add the remove document to the list
                return addToOperationsList(REMOVE, document);
            },

            remove: function() {
                collection._validateRemoveDoc(currentOp.selector);

                // Establish the remove command
                var document = {
                    q: currentOp.selector,
                    limit: 0
                };

                // Copy over the collation, if we have one.
                if (currentOp.hasOwnProperty('collation')) {
                    document.collation = currentOp.collation;
                }

                // Clear out current Op
                currentOp = null;
                // Add the remove document to the list
                return addToOperationsList(REMOVE, document);
            },

            collation: function(collationSpec) {
                if (!collection.getMongo().hasWriteCommands()) {
                    throw new Error(
                        "cannot use collation if server does not support write commands");
                }

                if (collection.getMongo().writeMode() !== "commands") {
                    throw new Error("write mode must be 'commands' in order to use collation, " +
                                    "but found write mode: " + collection.getMongo().writeMode());
                }

                currentOp.collation = collationSpec;
                return findOperations;
            },
        };

        //
        // Start of update and remove operations
        this.find = function(selector) {
            if (selector == undefined)
                throw Error("find() requires query criteria");
            // Save a current selector
            currentOp = {
                selector: selector
            };

            // Return the find Operations
            return findOperations;
        };

        //
        // Merge write command result into aggregated results object
        var mergeBatchResults = function(batch, bulkResult, result) {

            // If we have an insert Batch type
            if (batch.batchType == INSERT) {
                bulkResult.nInserted = bulkResult.nInserted + result.n;
            }

            // If we have a remove batch type
            if (batch.batchType == REMOVE) {
                bulkResult.nRemoved = bulkResult.nRemoved + result.n;
            }

            var nUpserted = 0;

            // We have an array of upserted values, we need to rewrite the indexes
            if (Array.isArray(result.upserted)) {
                nUpserted = result.upserted.length;

                for (var i = 0; i < result.upserted.length; i++) {
                    bulkResult.upserted.push({
                        index: result.upserted[i].index + batch.originalZeroIndex,
                        _id: result.upserted[i]._id
                    });
                }
            } else if (result.upserted) {
                nUpserted = 1;

                bulkResult.upserted.push({index: batch.originalZeroIndex, _id: result.upserted});
            }

            // If we have an update Batch type
            if (batch.batchType == UPDATE) {
                bulkResult.nUpserted = bulkResult.nUpserted + nUpserted;
                bulkResult.nMatched = bulkResult.nMatched + (result.n - nUpserted);
                if (result.nModified == undefined) {
                    bulkResult.nModified = undefined;
                } else if (bulkResult.nModified != undefined) {
                    bulkResult.nModified = bulkResult.nModified + result.nModified;
                }
            }

            if (Array.isArray(result.writeErrors)) {
                for (var i = 0; i < result.writeErrors.length; i++) {
                    var writeError = {
                        index: batch.originalZeroIndex + result.writeErrors[i].index,
                        code: result.writeErrors[i].code,
                        errmsg: result.writeErrors[i].errmsg,
                        op: batch.operations[result.writeErrors[i].index]
                    };

                    bulkResult.writeErrors.push(new WriteError(writeError));
                }
            }

            if (result.writeConcernError) {
                bulkResult.writeConcernErrors.push(new WriteConcernError(result.writeConcernError));
            }
        };

        //
        // Constructs the write batch command.
        var buildBatchCmd = function(batch) {
            var cmd = null;

            // Generate the right update
            if (batch.batchType == UPDATE) {
                cmd = {
                    update: coll.getName(),
                    updates: batch.operations,
                    ordered: ordered
                };
            } else if (batch.batchType == INSERT) {
                var transformedInserts = [];
                batch.operations.forEach(function(insertDoc) {
                    transformedInserts.push(addIdIfNeeded(insertDoc));
                });
                batch.operations = transformedInserts;

                cmd = {
                    insert: coll.getName(),
                    documents: batch.operations,
                    ordered: ordered
                };
            } else if (batch.batchType == REMOVE) {
                cmd = {
                    delete: coll.getName(),
                    deletes: batch.operations,
                    ordered: ordered
                };
            }

            // If we have a write concern
            if (writeConcern) {
                cmd.writeConcern = writeConcern;
            }

            return cmd;
        };

        //
        // Execute the batch
        var executeBatch = function(batch) {
            var result = null;
            var cmd = buildBatchCmd(batch);

            // Run the command (may throw)

            // Get command collection
            var cmdColl = collection._db.getCollection('$cmd');
            // Bypass runCommand to ignore slaveOk and read pref settings
            result = new DBQuery(collection.getMongo(),
                                 collection._db,
                                 cmdColl,
                                 cmdColl.getFullName(),
                                 cmd,
                                 {} /* proj */,
                                 -1 /* limit */,
                                 0 /* skip */,
                                 0 /* batchSize */,
                                 0 /* flags */).next();

            if (result.ok == 0) {
                throw new WriteCommandError(result);
            }

            // Merge the results
            mergeBatchResults(batch, bulkResult, result);
        };

        // Execute a single legacy op
        var executeLegacyOp = function(_legacyOp) {
            // Handle the different types of operation types
            if (_legacyOp.batchType == INSERT) {
                if (Array.isArray(_legacyOp.operation)) {
                    var transformedInserts = [];
                    _legacyOp.operation.forEach(function(insertDoc) {
                        transformedInserts.push(addIdIfNeeded(insertDoc));
                    });
                    _legacyOp.operation = transformedInserts;
                } else {
                    _legacyOp.operation = addIdIfNeeded(_legacyOp.operation);
                }

                collection.getMongo().insert(
                    collection.getFullName(), _legacyOp.operation, ordered);
            } else if (_legacyOp.batchType == UPDATE) {
                collection.getMongo().update(collection.getFullName(),
                                             _legacyOp.operation.q,
                                             _legacyOp.operation.u,
                                             _legacyOp.operation.upsert,
                                             _legacyOp.operation.multi);
            } else if (_legacyOp.batchType == REMOVE) {
                var single = Boolean(_legacyOp.operation.limit);

                collection.getMongo().remove(
                    collection.getFullName(), _legacyOp.operation.q, single);
            }
        };

        /**
         * Parses the getLastError response and properly sets the write errors and
         * write concern errors.
         * Should kept be up to date with BatchSafeWriter::extractGLEErrors.
         *
         * @return {object} an object with the format:
         *
         * {
         *   writeError: {object|null} raw write error object without the index.
         *   wcError: {object|null} raw write concern error object.
         * }
         */
        var extractGLEErrors = function(gleResponse) {
            var isOK = gleResponse.ok ? true : false;
            var err = (gleResponse.err) ? gleResponse.err : '';
            var errMsg = (gleResponse.errmsg) ? gleResponse.errmsg : '';
            var wNote = (gleResponse.wnote) ? gleResponse.wnote : '';
            var jNote = (gleResponse.jnote) ? gleResponse.jnote : '';
            var code = gleResponse.code;
            var timeout = gleResponse.wtimeout ? true : false;

            var extractedErr = {
                writeError: null,
                wcError: null,
                unknownError: null
            };

            if (err == 'norepl' || err == 'noreplset') {
                // Know this is legacy gle and the repl not enforced - write concern error in 2.4.
                var errObj = {
                    code: WRITE_CONCERN_FAILED
                };

                if (errMsg != '') {
                    errObj.errmsg = errMsg;
                } else if (wNote != '') {
                    errObj.errmsg = wNote;
                } else {
                    errObj.errmsg = err;
                }

                extractedErr.wcError = errObj;
            } else if (timeout) {
                // Know there was not write error.
                var errObj = {
                    code: WRITE_CONCERN_FAILED
                };

                if (errMsg != '') {
                    errObj.errmsg = errMsg;
                } else {
                    errObj.errmsg = err;
                }

                errObj.errInfo = {
                    wtimeout: true
                };
                extractedErr.wcError = errObj;
            } else if (code == 19900 ||  // No longer primary
                       code == 16805 ||  // replicatedToNum no longer primary
                       code == 14330 ||  // gle wmode changed; invalid
                       code == NOT_MASTER ||
                       code == UNKNOWN_REPL_WRITE_CONCERN || code == WRITE_CONCERN_FAILED) {
                extractedErr.wcError = {
                    code: code,
                    errmsg: errMsg
                };
            } else if (!isOK) {
                // This is a GLE failure we don't understand
                extractedErr.unknownError = {
                    code: code,
                    errmsg: errMsg
                };
            } else if (err != '') {
                extractedErr.writeError = {
                    code: (code == 0) ? UNKNOWN_ERROR : code,
                    errmsg: err
                };
            } else if (jNote != '') {
                extractedErr.writeError = {
                    code: WRITE_CONCERN_FAILED,
                    errmsg: jNote
                };
            }

            // Handling of writeback not needed for mongo shell.
            return extractedErr;
        };

        /**
         * getLastErrorMethod that supports all write concerns
         */
        var executeGetLastError = function(db, options) {
            var cmd = {
                getlasterror: 1
            };
            cmd = Object.extend(cmd, options);
            // Execute the getLastErrorCommand
            return db.runCommand(cmd);
        };

        // Execute the operations, serially
        var executeBatchWithLegacyOps = function(batch) {

            var batchResult = {
                n: 0,
                writeErrors: [],
                upserted: []
            };

            var extractedErr = null;

            var totalToExecute = batch.operations.length;
            // Run over all the operations
            for (var i = 0; i < batch.operations.length; i++) {
                if (batchResult.writeErrors.length > 0 && ordered)
                    break;

                var _legacyOp = new LegacyOp(batch.batchType, batch.operations[i], i);
                executeLegacyOp(_legacyOp);

                var result = executeGetLastError(collection.getDB(), {w: 1});
                extractedErr = extractGLEErrors(result);

                if (extractedErr.unknownError) {
                    throw new WriteCommandError({
                        ok: 0.0,
                        code: extractedErr.unknownError.code,
                        errmsg: extractedErr.unknownError.errmsg
                    });
                }

                if (extractedErr.writeError != null) {
                    // Create the emulated result set
                    var errResult = {
                        index: _legacyOp.index,
                        code: extractedErr.writeError.code,
                        errmsg: extractedErr.writeError.errmsg,
                        op: batch.operations[_legacyOp.index]
                    };

                    batchResult.writeErrors.push(errResult);
                } else if (_legacyOp.batchType == INSERT) {
                    // Inserts don't give us "n" back, so we can only infer
                    batchResult.n = batchResult.n + 1;
                }

                if (_legacyOp.batchType == UPDATE) {
                    // Unfortunately v2.4 GLE does not include the upserted field when
                    // the upserted _id is non-OID type.  We can detect this by the
                    // updatedExisting field + an n of 1
                    var upserted = result.upserted !== undefined ||
                        (result.updatedExisting === false && result.n == 1);

                    if (upserted) {
                        batchResult.n = batchResult.n + 1;

                        // If we don't have an upserted value, see if we can pull it from the update
                        // or the
                        // query
                        if (result.upserted === undefined) {
                            result.upserted = _legacyOp.operation.u._id;
                            if (result.upserted === undefined) {
                                result.upserted = _legacyOp.operation.q._id;
                            }
                        }

                        batchResult.upserted.push({index: _legacyOp.index, _id: result.upserted});
                    } else if (result.n) {
                        batchResult.n = batchResult.n + result.n;
                    }
                }

                if (_legacyOp.batchType == REMOVE && result.n) {
                    batchResult.n = batchResult.n + result.n;
                }
            }

            var needToEnforceWC = writeConcern != null &&
                bsonWoCompare(writeConcern, {w: 1}) != 0 &&
                bsonWoCompare(writeConcern, {w: 0}) != 0;

            extractedErr = null;
            if (needToEnforceWC && (batchResult.writeErrors.length == 0 ||
                                    (!ordered &&
                                     // not all errored.
                                     batchResult.writeErrors.length < batch.operations.length))) {
                // if last write errored
                if (batchResult.writeErrors.length > 0 &&
                    batchResult.writeErrors[batchResult.writeErrors.length - 1].index ==
                        (batch.operations.length - 1)) {
                    // Reset previous errors so we can apply the write concern no matter what
                    // as long as it is valid.
                    collection.getDB().runCommand({resetError: 1});
                }

                result = executeGetLastError(collection.getDB(), writeConcern);
                extractedErr = extractGLEErrors(result);

                if (extractedErr.unknownError) {
                    // Report as a wc failure
                    extractedErr.wcError = extractedErr.unknownError;
                }
            }

            if (extractedErr != null && extractedErr.wcError != null) {
                bulkResult.writeConcernErrors.push(extractedErr.wcError);
            }

            // Merge the results
            mergeBatchResults(batch, bulkResult, batchResult);
        };

        //
        // Execute the batch
        this.execute = function(_writeConcern) {
            if (executed)
                throw Error("A bulk operation cannot be re-executed");

            // If writeConcern set, use it, else get from collection (which will inherit from
            // db/mongo)
            writeConcern = _writeConcern ? _writeConcern : coll.getWriteConcern();
            if (writeConcern instanceof WriteConcern)
                writeConcern = writeConcern.toJSON();

            // If we have current batch
            if (currentBatch)
                batches.push(currentBatch);

            // Total number of batches to execute
            var totalNumberToExecute = batches.length;

            var useWriteCommands = collection.getMongo().useWriteCommands();

            // Execute all the batches
            for (var i = 0; i < batches.length; i++) {
                // Execute the batch
                if (collection.getMongo().hasWriteCommands() &&
                    collection.getMongo().writeMode() == "commands") {
                    executeBatch(batches[i]);
                } else {
                    executeBatchWithLegacyOps(batches[i]);
                }

                // If we are ordered and have errors and they are
                // not all replication errors terminate the operation
                if (bulkResult.writeErrors.length > 0 && ordered) {
                    // Ordered batches can't enforce full-batch write concern if they fail - they
                    // fail-fast
                    bulkResult.writeConcernErrors = [];
                    break;
                }
            }

            // Set as executed
            executed = true;

            // Create final result object
            typedResult = new BulkWriteResult(
                bulkResult, batches.length == 1 ? batches[0].batchType : null, writeConcern);
            // Throw on error
            if (typedResult.hasErrors()) {
                throw typedResult.toError();
            }

            return typedResult;
        };

        // Generate an explain command for the bulk operation. Currently we only support single
        // batches
        // of size 1, which must be either delete or update.
        this.convertToExplainCmd = function(verbosity) {
            // If we have current batch
            if (currentBatch) {
                batches.push(currentBatch);
            }

            // We can only explain singleton batches.
            if (batches.length !== 1) {
                throw Error("Explained bulk operations must consist of exactly 1 batch");
            }

            var explainBatch = batches[0];
            var writeCmd = buildBatchCmd(explainBatch);
            return {
                "explain": writeCmd,
                "verbosity": verbosity
            };
        };
    };

    //
    // Exports
    //

    module = {};
    module.WriteConcern = WriteConcern;
    module.WriteResult = WriteResult;
    module.BulkWriteResult = BulkWriteResult;
    module.BulkWriteError = BulkWriteError;
    module.WriteCommandError = WriteCommandError;
    module.initializeUnorderedBulkOp = function() {
        return new Bulk(this, false);
    };
    module.initializeOrderedBulkOp = function() {
        return new Bulk(this, true);
    };

    return module;

})();

// Globals
WriteConcern = _bulk_api_module.WriteConcern;
WriteResult = _bulk_api_module.WriteResult;
BulkWriteResult = _bulk_api_module.BulkWriteResult;
BulkWriteError = _bulk_api_module.BulkWriteError;
WriteCommandError = _bulk_api_module.WriteCommandError;

/***********************************************************
 * Adds the initializers of bulk operations to the db collection
 ***********************************************************/
DBCollection.prototype.initializeUnorderedBulkOp = _bulk_api_module.initializeUnorderedBulkOp;
DBCollection.prototype.initializeOrderedBulkOp = _bulk_api_module.initializeOrderedBulkOp;



// ---- MODULE: crud_api ---- 
DBCollection.prototype._createWriteConcern = function(options) {
    // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
    var writeConcern = options.writeConcern || this.getWriteConcern();
    var writeConcernOptions = ['w', 'wtimeout', 'j', 'fsync'];

    if (writeConcern instanceof WriteConcern) {
        writeConcern = writeConcern.toJSON();
    }

    // Only merge in write concern options if at least one is specified in options
    if (options.w != null || options.wtimeout != null || options.j != null ||
        options.fsync != null) {
        writeConcern = {};

        writeConcernOptions.forEach(function(wc) {
            if (options[wc] != null) {
                writeConcern[wc] = options[wc];
            }
        });
    }

    return writeConcern;
};

/**
 * @return {Object} a new document with an _id: ObjectId if _id is not present.
 *     Otherwise, returns the same object passed.
 */
DBCollection.prototype.addIdIfNeeded = function(obj) {
    if (typeof(obj._id) == "undefined" && !Array.isArray(obj)) {
        var tmp = obj;  // don't want to modify input
        obj = {
            _id: new ObjectId()
        };

        for (var key in tmp) {
            obj[key] = tmp[key];
        }
    }

    return obj;
};

/**
* Perform a bulkWrite operation without a fluent API
*
* Legal operation types are
*
*  { insertOne: { document: { a: 1 } } }
*
*  { updateOne: { filter: {a:2}, update: {$set: {a:2}}, upsert:true, collation: {locale: "fr"} } }
*
*  { updateMany: { filter: {a:2}, update: {$set: {a:2}}, upsert:true collation: {locale: "fr"} } }
*
*  { deleteOne: { filter: {c:1}, collation: {locale: "fr"} } }
*
*  { deleteMany: { filter: {c:1}, collation: {locale: "fr"} } }
*
*  { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true, collation: {locale: "fr"} } }
*
* @method
* @param {object[]} operations Bulk operations to perform.
* @param {object} [options=null] Optional settings.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.bulkWrite = function(operations, options) {
    var opts = Object.extend({}, options || {});
    opts.ordered = (typeof opts.ordered == 'boolean') ? opts.ordered : true;

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {
        acknowledged: (writeConcern && writeConcern.w == 0) ? false : true
    };

    // Use bulk operation API already in the shell
    var bulkOp = opts.ordered ? this.initializeOrderedBulkOp() : this.initializeUnorderedBulkOp();

    // Contains all inserted _ids
    var insertedIds = {};

    // For each of the operations we need to add the op to the bulk
    operations.forEach(function(op, index) {
        if (op.insertOne) {
            if (!op.insertOne.document) {
                throw new Error('insertOne bulkWrite operation expects the document field');
            }

            // Add _id ObjectId if needed
            op.insertOne.document = this.addIdIfNeeded(op.insertOne.document);
            // InsertedIds is a map of [originalInsertOrderIndex] = document._id
            insertedIds[index] = op.insertOne.document._id;
            // Translate operation to bulk operation
            bulkOp.insert(op.insertOne.document);
        } else if (op.updateOne) {
            if (!op.updateOne.filter) {
                throw new Error('updateOne bulkWrite operation expects the filter field');
            }

            if (!op.updateOne.update) {
                throw new Error('updateOne bulkWrite operation expects the update field');
            }

            // Translate operation to bulk operation
            var operation = bulkOp.find(op.updateOne.filter);
            if (op.updateOne.upsert) {
                operation = operation.upsert();
            }

            if (op.updateOne.collation) {
                operation.collation(op.updateOne.collation);
            }

            operation.updateOne(op.updateOne.update);
        } else if (op.updateMany) {
            if (!op.updateMany.filter) {
                throw new Error('updateMany bulkWrite operation expects the filter field');
            }

            if (!op.updateMany.update) {
                throw new Error('updateMany bulkWrite operation expects the update field');
            }

            // Translate operation to bulk operation
            var operation = bulkOp.find(op.updateMany.filter);
            if (op.updateMany.upsert) {
                operation = operation.upsert();
            }

            if (op.updateMany.collation) {
                operation.collation(op.updateMany.collation);
            }

            operation.update(op.updateMany.update);
        } else if (op.replaceOne) {
            if (!op.replaceOne.filter) {
                throw new Error('replaceOne bulkWrite operation expects the filter field');
            }

            if (!op.replaceOne.replacement) {
                throw new Error('replaceOne bulkWrite operation expects the replacement field');
            }

            // Translate operation to bulkOp operation
            var operation = bulkOp.find(op.replaceOne.filter);
            if (op.replaceOne.upsert) {
                operation = operation.upsert();
            }

            if (op.replaceOne.collation) {
                operation.collation(op.replaceOne.collation);
            }

            operation.replaceOne(op.replaceOne.replacement);
        } else if (op.deleteOne) {
            if (!op.deleteOne.filter) {
                throw new Error('deleteOne bulkWrite operation expects the filter field');
            }

            // Translate operation to bulkOp operation.
            var deleteOp = bulkOp.find(op.deleteOne.filter);

            if (op.deleteOne.collation) {
                deleteOp.collation(op.deleteOne.collation);
            }

            deleteOp.removeOne();
        } else if (op.deleteMany) {
            if (!op.deleteMany.filter) {
                throw new Error('deleteMany bulkWrite operation expects the filter field');
            }

            // Translate operation to bulkOp operation.
            var deleteOp = bulkOp.find(op.deleteMany.filter);

            if (op.deleteMany.collation) {
                deleteOp.collation(op.deleteMany.collation);
            }

            deleteOp.remove();
        }
    }, this);

    // Execute bulkOp operation
    var response = bulkOp.execute(writeConcern);
    if (!result.acknowledged) {
        return result;
    }

    result.deletedCount = response.nRemoved;
    result.insertedCount = response.nInserted;
    result.matchedCount = response.nMatched;
    result.upsertedCount = response.nUpserted;
    result.insertedIds = insertedIds;
    result.upsertedIds = {};

    // Iterate over all the upserts
    var upserts = response.getUpsertedIds();
    upserts.forEach(function(x) {
        result.upsertedIds[x.index] = x._id;
    });

    // Return the result
    return result;
};

/**
* Inserts a single document into MongoDB.
*
* @method
* @param {object} doc Document to insert.
* @param {object} [options=null] Optional settings.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.insertOne = function(document, options) {
    var opts = Object.extend({}, options || {});

    // Add _id ObjectId if needed
    document = this.addIdIfNeeded(document);

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {
        acknowledged: (writeConcern && writeConcern.w == 0) ? false : true
    };

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();
    bulk.insert(document);

    try {
        // Execute insert
        bulk.execute(writeConcern);
    } catch (err) {
        if (err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }

        if (err.hasWriteConcernError()) {
            throw err.getWriteConcernError();
        }

        throw err;
    }

    if (!result.acknowledged) {
        return result;
    }

    // Set the inserted id
    result.insertedId = document._id;

    // Return the result
    return result;
};

/**
* Inserts an array of documents into MongoDB.
*
* @method
* @param {object[]} docs Documents to insert.
* @param {object} [options=null] Optional settings.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @param {boolean} [options.ordered=true] Execute inserts in ordered or unordered fashion.
* @return {object}
*/
DBCollection.prototype.insertMany = function(documents, options) {
    var opts = Object.extend({}, options || {});
    opts.ordered = (typeof opts.ordered == 'boolean') ? opts.ordered : true;

    // Ensure all documents have an _id
    documents = documents.map(function(x) {
        return this.addIdIfNeeded(x);
    }, this);

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {
        acknowledged: (writeConcern && writeConcern.w == 0) ? false : true
    };

    // Use bulk operation API already in the shell
    var bulk = opts.ordered ? this.initializeOrderedBulkOp() : this.initializeUnorderedBulkOp();

    // Add all operations to the bulk operation
    documents.forEach(function(doc) {
        bulk.insert(doc);
    });

    // Execute bulk write operation
    bulk.execute(writeConcern);

    if (!result.acknowledged) {
        return result;
    }

    // Set all the created inserts
    result.insertedIds = documents.map(function(x) {
        return x._id;
    });

    // Return the result
    return result;
};

/**
* Delete a document on MongoDB
*
* @method
* @param {object} filter The filter used to select the document to remove
* @param {object} [options=null] Optional settings.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.deleteOne = function(filter, options) {
    var opts = Object.extend({}, options || {});

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {
        acknowledged: (writeConcern && writeConcern.w == 0) ? false : true
    };

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();
    var removeOp = bulk.find(filter);

    // Add the collation, if there is one.
    if (opts.collation) {
        removeOp.collation(opts.collation);
    }

    // Add the deleteOne operation.
    removeOp.removeOne();

    try {
        // Remove the first document that matches the selector
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if (err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }

        if (err.hasWriteConcernError()) {
            throw err.getWriteConcernError();
        }

        throw err;
    }

    if (!result.acknowledged) {
        return result;
    }

    result.deletedCount = r.nRemoved;
    return result;
};

/**
* Delete multiple documents on MongoDB
*
* @method
* @param {object} filter The Filter used to select the documents to remove
* @param {object} [options=null] Optional settings.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.deleteMany = function(filter, options) {
    var opts = Object.extend({}, options || {});

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {
        acknowledged: (writeConcern && writeConcern.w == 0) ? false : true
    };

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();
    var removeOp = bulk.find(filter);

    // Add the collation, if there is one.
    if (opts.collation) {
        removeOp.collation(opts.collation);
    }

    // Add the deleteOne operation.
    removeOp.remove();

    try {
        // Remove all documents that matche the selector
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if (err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }

        if (err.hasWriteConcernError()) {
            throw err.getWriteConcernError();
        }

        throw err;
    }

    if (!result.acknowledged) {
        return result;
    }

    result.deletedCount = r.nRemoved;
    return result;
};

/**
* Replace a document on MongoDB
*
* @method
* @param {object} filter The Filter used to select the document to update
* @param {object} doc The Document that replaces the matching document
* @param {object} [options=null] Optional settings.
* @param {boolean} [options.upsert=false] Update operation is an upsert.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.replaceOne = function(filter, replacement, options) {
    var opts = Object.extend({}, options || {});

    // Check if first key in update statement contains a $
    var keys = Object.keys(replacement);
    // Check if first key does not have the $
    if (keys.length > 0 && keys[0][0] == "$") {
        throw new Error('the replace operation document must not contain atomic operators');
    }

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {
        acknowledged: (writeConcern && writeConcern.w == 0) ? false : true
    };

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();

    // Add the deleteOne operation
    var op = bulk.find(filter);
    if (opts.upsert) {
        op = op.upsert();
    }

    if (opts.collation) {
        op.collation(opts.collation);
    }

    op.replaceOne(replacement);

    try {
        // Replace the document
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if (err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }

        if (err.hasWriteConcernError()) {
            throw err.getWriteConcernError();
        }

        throw err;
    }

    if (!result.acknowledged) {
        return result;
    }

    result.matchedCount = r.nMatched;
    result.modifiedCount = (r.nModified != null) ? r.nModified : r.n;

    if (r.getUpsertedIds().length > 0) {
        result.upsertedId = r.getUpsertedIdAt(0)._id;
    }

    return result;
};

/**
* Update a single document on MongoDB
*
* @method
* @param {object} filter The Filter used to select the document to update
* @param {object} update The update operations to be applied to the document
* @param {object} [options=null] Optional settings.
* @param {boolean} [options.upsert=false] Update operation is an upsert.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.updateOne = function(filter, update, options) {
    var opts = Object.extend({}, options || {});

    // Check if first key in update statement contains a $
    var keys = Object.keys(update);
    if (keys.length == 0) {
        throw new Error("the update operation document must contain at least one atomic operator");
    }

    // Check if first key does not have the $
    if (keys[0][0] != "$") {
        throw new Error('the update operation document must contain atomic operators');
    }

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {
        acknowledged: (writeConcern && writeConcern.w == 0) ? false : true
    };

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();

    // Add the updateOne operation
    var op = bulk.find(filter);
    if (opts.upsert) {
        op = op.upsert();
    }

    if (opts.collation) {
        op.collation(opts.collation);
    }

    op.updateOne(update);

    try {
        // Update the first document that matches the selector
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if (err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }

        if (err.hasWriteConcernError()) {
            throw err.getWriteConcernError();
        }

        throw err;
    }

    if (!result.acknowledged) {
        return result;
    }

    result.matchedCount = r.nMatched;
    result.modifiedCount = (r.nModified != null) ? r.nModified : r.n;

    if (r.getUpsertedIds().length > 0) {
        result.upsertedId = r.getUpsertedIdAt(0)._id;
    }

    return result;
};

/**
* Update multiple documents on MongoDB
*
* @method
* @param {object} filter The Filter used to select the document to update
* @param {object} update The update operations to be applied to the document
* @param {object} [options=null] Optional settings.
* @param {boolean} [options.upsert=false] Update operation is an upsert.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.updateMany = function(filter, update, options) {
    var opts = Object.extend({}, options || {});

    // Check if first key in update statement contains a $
    var keys = Object.keys(update);
    if (keys.length == 0) {
        throw new Error("the update operation document must contain at least one atomic operator");
    }

    // Check if first key does not have the $
    if (keys[0][0] != "$") {
        throw new Error('the update operation document must contain atomic operators');
    }

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {
        acknowledged: (writeConcern && writeConcern.w == 0) ? false : true
    };

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();

    // Add the updateMany operation
    var op = bulk.find(filter);
    if (opts.upsert) {
        op = op.upsert();
    }

    if (opts.collation) {
        op.collation(opts.collation);
    }

    op.update(update);

    try {
        // Update all documents that match the selector
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if (err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }

        if (err.hasWriteConcernError()) {
            throw err.getWriteConcernError();
        }

        throw err;
    }

    if (!result.acknowledged) {
        return result;
    }

    result.matchedCount = r.nMatched;
    result.modifiedCount = (r.nModified != null) ? r.nModified : r.n;

    if (r.getUpsertedIds().length > 0) {
        result.upsertedId = r.getUpsertedIdAt(0)._id;
    }

    return result;
};

/**
* Find a document and delete it in one atomic operation,
* requires a write lock for the duration of the operation.
*
* @method
* @param {object} filter Document selection filter.
* @param {object} [options=null] Optional settings.
* @param {object} [options.projection=null] Limits the fields to return for all matching documents.
* @param {object} [options.sort=null] Determines which document the operation modifies if the query
*selects multiple documents.
* @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
* @return {object}
*/
DBCollection.prototype.findOneAndDelete = function(filter, options) {
    var opts = Object.extend({}, options || {});
    // Set up the command
    var cmd = {
        query: filter,
        remove: true
    };

    if (opts.sort) {
        cmd.sort = opts.sort;
    }

    if (opts.projection) {
        cmd.fields = opts.projection;
    }

    if (opts.maxTimeMS) {
        cmd.maxTimeMS = opts.maxTimeMS;
    }

    if (opts.collation) {
        cmd.collation = opts.collation;
    }

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Setup the write concern
    if (writeConcern) {
        cmd.writeConcern = writeConcern;
    }

    // Execute findAndModify
    return this.findAndModify(cmd);
};

/**
* Find a document and replace it in one atomic operation, requires a write lock for the duration of
*the operation.
*
* @method
* @param {object} filter Document selection filter.
* @param {object} replacement Document replacing the matching document.
* @param {object} [options=null] Optional settings.
* @param {object} [options.projection=null] Limits the fields to return for all matching documents.
* @param {object} [options.sort=null] Determines which document the operation modifies if the query
*selects multiple documents.
* @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
* @param {boolean} [options.upsert=false] Upsert the document if it does not exist.
* @param {boolean} [options.returnNewDocument=false] When true, returns the updated document rather
*than the original. The default is false.
* @return {object}
*/
DBCollection.prototype.findOneAndReplace = function(filter, replacement, options) {
    var opts = Object.extend({}, options || {});

    // Check if first key in update statement contains a $
    var keys = Object.keys(replacement);
    // Check if first key does not have the $
    if (keys.length > 0 && keys[0][0] == "$") {
        throw new Error("the replace operation document must not contain atomic operators");
    }

    // Set up the command
    var cmd = {
        query: filter,
        update: replacement
    };
    if (opts.sort) {
        cmd.sort = opts.sort;
    }

    if (opts.projection) {
        cmd.fields = opts.projection;
    }

    if (opts.maxTimeMS) {
        cmd.maxTimeMS = opts.maxTimeMS;
    }

    if (opts.collation) {
        cmd.collation = opts.collation;
    }

    // Set flags
    cmd.upsert = (typeof opts.upsert == 'boolean') ? opts.upsert : false;
    cmd.new = (typeof opts.returnNewDocument == 'boolean') ? opts.returnNewDocument : false;

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Setup the write concern
    if (writeConcern) {
        cmd.writeConcern = writeConcern;
    }

    // Execute findAndModify
    return this.findAndModify(cmd);
};

/**
* Find a document and update it in one atomic operation, requires a write lock for the duration of
*the operation.
*
* @method
* @param {object} filter Document selection filter.
* @param {object} update Update operations to be performed on the document
* @param {object} [options=null] Optional settings.
* @param {object} [options.projection=null] Limits the fields to return for all matching documents.
* @param {object} [options.sort=null] Determines which document the operation modifies if the query
*selects multiple documents.
* @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
* @param {boolean} [options.upsert=false] Upsert the document if it does not exist.
* @param {boolean} [options.returnNewDocument=false] When true, returns the updated document rather
*than the original. The default is false.
* @return {object}
*/
DBCollection.prototype.findOneAndUpdate = function(filter, update, options) {
    var opts = Object.extend({}, options || {});

    // Check if first key in update statement contains a $
    var keys = Object.keys(update);
    if (keys.length == 0) {
        throw new Error("the update operation document must contain at least one atomic operator");
    }

    // Check if first key does not have the $
    if (keys[0][0] != "$") {
        throw new Error("the update operation document must contain atomic operators");
    }

    // Set up the command
    var cmd = {
        query: filter,
        update: update
    };
    if (opts.sort) {
        cmd.sort = opts.sort;
    }

    if (opts.projection) {
        cmd.fields = opts.projection;
    }

    if (opts.maxTimeMS) {
        cmd.maxTimeMS = opts.maxTimeMS;
    }

    if (opts.collation) {
        cmd.collation = opts.collation;
    }

    // Set flags
    cmd.upsert = (typeof opts.upsert == 'boolean') ? opts.upsert : false;
    cmd.new = (typeof opts.returnNewDocument == 'boolean') ? opts.returnNewDocument : false;

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Setup the write concern
    if (writeConcern) {
        cmd.writeConcern = writeConcern;
    }

    // Execute findAndModify
    return this.findAndModify(cmd);
};



// ---- MODULE: db ---- 
// db.js

var DB;

(function() {

    if (DB === undefined) {
        DB = function(mongo, name) {
            this._mongo = mongo;
            this._name = name;
        };
    }

    DB.prototype.getMongo = function() {
        assert(this._mongo, "why no mongo!");
        return this._mongo;
    };

    DB.prototype.getSiblingDB = function(name) {
        return this.getMongo().getDB(name);
    };

    DB.prototype.getSisterDB = DB.prototype.getSiblingDB;

    DB.prototype.getName = function() {
        return this._name;
    };

    DB.prototype.stats = function(scale) {
        return this.runCommand({dbstats: 1, scale: scale});
    };

    DB.prototype.getCollection = function(name) {
        return new DBCollection(this._mongo, this, name, this._name + "." + name);
    };

    DB.prototype.commandHelp = function(name) {
        var c = {};
        c[name] = 1;
        c.help = true;
        var res = this.runCommand(c);
        if (!res.ok)
            throw _getErrorWithCode(res, res.errmsg);
        return res.help;
    };

    // utility to attach readPreference if needed.
    DB.prototype._attachReadPreferenceToCommand = function(cmdObj, readPref) {
        "use strict";
        // if the user has not set a readpref, return the original cmdObj
        if ((readPref === null) || typeof(readPref) !== "object") {
            return cmdObj;
        }

        // if user specifies $readPreference manually, then don't change it
        if (cmdObj.hasOwnProperty("$readPreference")) {
            return cmdObj;
        }

        // copy object so we don't mutate the original
        var clonedCmdObj = Object.extend({}, cmdObj);
        // The server selection spec mandates that the key is '$query', but
        // the shell has historically used 'query'. The server accepts both,
        // so we maintain the existing behavior
        var cmdObjWithReadPref = {
            query: clonedCmdObj,
            $readPreference: readPref
        };
        return cmdObjWithReadPref;
    };

    // if someone passes i.e. runCommand("foo", {bar: "baz"}
    // we merge it in to runCommand({foo: 1, bar: "baz"}
    // this helper abstracts that logic.
    DB.prototype._mergeCommandOptions = function(commandName, extraKeys) {
        "use strict";
        var mergedCmdObj = {};
        mergedCmdObj[commandName] = 1;

        if (typeof(extraKeys) === "object") {
            // this will traverse the prototype chain of extra, but keeping
            // to maintain legacy behavior
            for (var key in extraKeys) {
                mergedCmdObj[key] = extraKeys[key];
            }
        }
        return mergedCmdObj;
    };

    // Like runCommand but applies readPreference if one has been set
    // on the connection. Also sets slaveOk if a (non-primary) readPref has been set.
    DB.prototype.runReadCommand = function(obj, extra, queryOptions) {
        "use strict";

        // Support users who call this function with a string commandName, e.g.
        // db.runReadCommand("commandName", {arg1: "value", arg2: "value"}).
        var mergedObj = (typeof(obj) === "string") ? this._mergeCommandOptions(obj, extra) : obj;
        var cmdObjWithReadPref =
            this._attachReadPreferenceToCommand(mergedObj, this.getMongo().getReadPref());

        var options =
            (typeof(queryOptions) !== "undefined") ? queryOptions : this.getQueryOptions();
        var readPrefMode = this.getMongo().getReadPrefMode();

        // Set slaveOk if readPrefMode has been explicitly set with a readPreference other than
        // primary.
        if (!!readPrefMode && readPrefMode !== "primary") {
            options |= 4;
        }

        // The 'extra' parameter is not used as we have already created a merged command object.
        return this.runCommand(cmdObjWithReadPref, null, options);
    };

    // runCommand uses this impl to actually execute the command
    DB.prototype._runCommandImpl = function(name, obj, options) {
        return this.getMongo().runCommand(name, obj, options);
    };

    DB.prototype.runCommand = function(obj, extra, queryOptions) {
        var mergedObj = (typeof(obj) === "string") ? this._mergeCommandOptions(obj, extra) : obj;
        // if options were passed (i.e. because they were overridden on a collection), use them.
        // Otherwise use getQueryOptions.
        var options =
            (typeof(queryOptions) !== "undefined") ? queryOptions : this.getQueryOptions();
        var res;
        try {
            res = this._runCommandImpl(this._name, mergedObj, options);
        } catch (ex) {
            // When runCommand flowed through query, a connection error resulted in the message
            // "error doing query: failed". Even though this message is arguably incorrect
            // for a command failing due to a connection failure, we preserve it for backwards
            // compatibility. See SERVER-18334 for details.
            if (ex.message.indexOf("network error") >= 0) {
                throw new Error("error doing query: failed: " + ex.message);
            }
            throw ex;
        }
        return res;
    };

    DB.prototype.runCommandWithMetadata = function(commandName, commandArgs, metadata) {
        return this.getMongo().runCommandWithMetadata(
            this._name, commandName, metadata, commandArgs);
    };

    DB.prototype._dbCommand = DB.prototype.runCommand;
    DB.prototype._dbReadCommand = DB.prototype.runReadCommand;

    DB.prototype.adminCommand = function(obj, extra) {
        if (this._name == "admin")
            return this.runCommand(obj, extra);
        return this.getSiblingDB("admin").runCommand(obj, extra);
    };

    DB.prototype._adminCommand = DB.prototype.adminCommand;  // alias old name

    /**
      Create a new collection in the database.  Normally, collection creation is automatic.  You
     would
       use this function if you wish to specify special options on creation.

       If the collection already exists, no action occurs.

        <p>Options:</p>
        <ul>
        <li>
            size: desired initial extent size for the collection.  Must be <= 1000000000.
                  for fixed size (capped) collections, this size is the total/max size of the
                  collection.
        </li>
        <li>
            capped: if true, this is a capped collection (where old data rolls out).
        </li>
        <li> max: maximum number of objects if capped (optional).</li>
        <li> usePowerOf2Sizes: if true, set usePowerOf2Sizes allocation for the collection.</li>
        <li>
            storageEngine: BSON document containing storage engine specific options. Format:
                           {
                               storageEngine: {
                                   storageEngine1: {
                                       ...
                                   },
                                   storageEngine2: {
                                       ...
                                   },
                                   ...
                               }
                           }
        </li>
        </ul>

        <p>Example:</p>
        <code>db.createCollection("movies", { size: 10 * 1024 * 1024, capped:true } );</code>

     * @param {String} name Name of new collection to create
     * @param {Object} options Object with options for call.  Options are listed above.
     * @return SOMETHING_FIXME
    */
    DB.prototype.createCollection = function(name, opt) {
        var options = opt || {};

        // We have special handling for the 'flags' field, and provide sugar for specific flags. If
        // the
        // user specifies any flags we send the field in the command. Otherwise, we leave it blank
        // and
        // use the server's defaults.
        var sendFlags = false;
        var flags = 0;
        if (options.usePowerOf2Sizes != undefined) {
            print(
                "WARNING: The 'usePowerOf2Sizes' flag is ignored in 3.0 and higher as all MMAPv1 " +
                "collections use fixed allocation sizes unless the 'noPadding' flag is specified");

            sendFlags = true;
            if (options.usePowerOf2Sizes) {
                flags |= 1;  // Flag_UsePowerOf2Sizes
            }
            delete options.usePowerOf2Sizes;
        }
        if (options.noPadding != undefined) {
            sendFlags = true;
            if (options.noPadding) {
                flags |= 2;  // Flag_NoPadding
            }
            delete options.noPadding;
        }

        // New flags must be added above here.
        if (sendFlags) {
            if (options.flags != undefined)
                throw Error("Can't set 'flags' with either 'usePowerOf2Sizes' or 'noPadding'");
            options.flags = flags;
        }

        var cmd = {
            create: name
        };
        Object.extend(cmd, options);

        return this._dbCommand(cmd);
    };

    /**
     * @deprecated use getProfilingStatus
     *  Returns the current profiling level of this database
     *  @return SOMETHING_FIXME or null on error
     */
    DB.prototype.getProfilingLevel = function() {
        var res = assert.commandWorked(this._dbCommand({profile: -1}));
        return res ? res.was : null;
    };

    /**
     *  @return the current profiling status
     *  example { was : 0, slowms : 100 }
     *  @return SOMETHING_FIXME or null on error
     */
    DB.prototype.getProfilingStatus = function() {
        var res = this._dbCommand({profile: -1});
        if (!res.ok)
            throw _getErrorWithCode(res, "profile command failed: " + tojson(res));
        delete res.ok;
        return res;
    };

    /**
      Erase the entire database.  (!)

     * @return Object returned has member ok set to true if operation succeeds, false otherwise.
     */
    DB.prototype.dropDatabase = function() {
        if (arguments.length)
            throw Error("dropDatabase doesn't take arguments");
        return this._dbCommand({dropDatabase: 1});
    };

    /**
     * Shuts down the database.  Must be run while using the admin database.
     * @param opts Options for shutdown. Possible options are:
     *   - force: (boolean) if the server should shut down, even if there is no
     *     up-to-date slave
     *   - timeoutSecs: (number) the server will continue checking over timeoutSecs
     *     if any other servers have caught up enough for it to shut down.
     */
    DB.prototype.shutdownServer = function(opts) {
        if ("admin" != this._name) {
            return "shutdown command only works with the admin database; try 'use admin'";
        }

        var cmd = {
            'shutdown': 1
        };
        opts = opts || {};
        for (var o in opts) {
            cmd[o] = opts[o];
        }

        try {
            var res = this.runCommand(cmd);
            if (!res.ok) {
                throw _getErrorWithCode(res, 'shutdownServer failed: ' + tojson(res));
            }
            throw Error('shutdownServer failed: server is still up.');
        } catch (e) {
            // we expect the command to not return a response, as the server will shut down
            // immediately.
            if (e.message.indexOf("error doing query: failed") >= 0) {
                print('server should be down...');
                return;
            }
            throw e;
        }
    };

    /**
      Clone database on another server to here.
      <p>
      Generally, you should dropDatabase() first as otherwise the cloned information will MERGE
      into whatever data is already present in this database.  (That is however a valid way to use
      clone if you are trying to do something intentionally, such as union three non-overlapping
      databases into one.)
      <p>
      This is a low level administrative function will is not typically used.

     * @param {String} from Where to clone from (dbhostname[:port]).  May not be this database
                       (self) as you cannot clone to yourself.
     * @return Object returned has member ok set to true if operation succeeds, false otherwise.
     * See also: db.copyDatabase()
     */
    DB.prototype.cloneDatabase = function(from) {
        assert(isString(from) && from.length);
        return this._dbCommand({clone: from});
    };

    /**
     Clone collection on another server to here.
     <p>
     Generally, you should drop() first as otherwise the cloned information will MERGE
     into whatever data is already present in this collection.  (That is however a valid way to use
     clone if you are trying to do something intentionally, such as union three non-overlapping
     collections into one.)
     <p>
     This is a low level administrative function is not typically used.

     * @param {String} from mongod instance from which to clnoe (dbhostname:port).  May
     not be this mongod instance, as clone from self is not allowed.
     * @param {String} collection name of collection to clone.
     * @param {Object} query query specifying which elements of collection are to be cloned.
     * @return Object returned has member ok set to true if operation succeeds, false otherwise.
     * See also: db.cloneDatabase()
     */
    DB.prototype.cloneCollection = function(from, collection, query) {
        assert(isString(from) && from.length);
        assert(isString(collection) && collection.length);
        collection = this._name + "." + collection;
        query = query || {};
        return this._dbCommand({cloneCollection: collection, from: from, query: query});
    };

    /**
      Copy database from one server or name to another server or name.

      Generally, you should dropDatabase() first as otherwise the copied information will MERGE
      into whatever data is already present in this database (and you will get duplicate objects
      in collections potentially.)

      For security reasons this function only works when executed on the "admin" db.  However,
      if you have access to said db, you can copy any database from one place to another.

      This method provides a way to "rename" a database by copying it to a new db name and
      location.  Additionally, it effectively provides a repair facility.

      * @param {String} fromdb database name from which to copy.
      * @param {String} todb database name to copy to.
      * @param {String} fromhost hostname of the database (and optionally, ":port") from which to
                        copy the data.  default if unspecified is to copy from self.
      * @return Object returned has member ok set to true if operation succeeds, false otherwise.
      * See also: db.clone()
    */
    DB.prototype.copyDatabase = function(fromdb, todb, fromhost, username, password, mechanism) {
        assert(isString(fromdb) && fromdb.length);
        assert(isString(todb) && todb.length);
        fromhost = fromhost || "";

        if (!mechanism) {
            mechanism = this._getDefaultAuthenticationMechanism();
        }
        assert(mechanism == "SCRAM-SHA-1" || mechanism == "MONGODB-CR");

        // Check for no auth or copying from localhost
        if (!username || !password || fromhost == "") {
            return this._adminCommand({copydb: 1, fromhost: fromhost, fromdb: fromdb, todb: todb});
        }

        // Use the copyDatabase native helper for SCRAM-SHA-1
        if (mechanism == "SCRAM-SHA-1") {
            return this.getMongo().copyDatabaseWithSCRAM(
                fromdb, todb, fromhost, username, password);
        }

        // Fall back to MONGODB-CR
        var n = this._adminCommand({copydbgetnonce: 1, fromhost: fromhost});
        return this._adminCommand({
            copydb: 1,
            fromhost: fromhost,
            fromdb: fromdb,
            todb: todb,
            username: username,
            nonce: n.nonce,
            key: this.__pwHash(n.nonce, username, password)
        });
    };

    /**
      Repair database.

     * @return Object returned has member ok set to true if operation succeeds, false otherwise.
    */
    DB.prototype.repairDatabase = function() {
        return this._dbCommand({repairDatabase: 1});
    };

    DB.prototype.help = function() {
        print("DB methods:");
        print(
            "\tdb.adminCommand(nameOrDocument) - switches to 'admin' db, and runs command [ just calls db.runCommand(...) ]");
        print("\tdb.auth(username, password)");
        print("\tdb.cloneDatabase(fromhost)");
        print("\tdb.commandHelp(name) returns the help for the command");
        print("\tdb.copyDatabase(fromdb, todb, fromhost)");
        print("\tdb.createCollection(name, { size : ..., capped : ..., max : ... } )");
        print("\tdb.createUser(userDocument)");
        print("\tdb.currentOp() displays currently executing operations in the db");
        print("\tdb.dropDatabase()");
        print("\tdb.eval() - deprecated");
        print("\tdb.fsyncLock() flush data to disk and lock server for backups");
        print("\tdb.fsyncUnlock() unlocks server following a db.fsyncLock()");
        print("\tdb.getCollection(cname) same as db['cname'] or db.cname");
        print(
            "\tdb.getCollectionInfos([filter]) - returns a list that contains the names and options" +
            " of the db's collections");
        print("\tdb.getCollectionNames()");
        print("\tdb.getLastError() - just returns the err msg string");
        print("\tdb.getLastErrorObj() - return full status object");
        print("\tdb.getLogComponents()");
        print("\tdb.getMongo() get the server connection object");
        print("\tdb.getMongo().setSlaveOk() allow queries on a replication slave server");
        print("\tdb.getName()");
        print("\tdb.getPrevError()");
        print("\tdb.getProfilingLevel() - deprecated");
        print("\tdb.getProfilingStatus() - returns if profiling is on and slow threshold");
        print("\tdb.getReplicationInfo()");
        print("\tdb.getSiblingDB(name) get the db at the same server as this one");
        print(
            "\tdb.getWriteConcern() - returns the write concern used for any operations on this db, inherited from server object if set");
        print("\tdb.hostInfo() get details about the server's host");
        print("\tdb.isMaster() check replica primary status");
        print("\tdb.killOp(opid) kills the current operation in the db");
        print("\tdb.listCommands() lists all the db commands");
        print("\tdb.loadServerScripts() loads all the scripts in db.system.js");
        print("\tdb.logout()");
        print("\tdb.printCollectionStats()");
        print("\tdb.printReplicationInfo()");
        print("\tdb.printShardingStatus()");
        print("\tdb.printSlaveReplicationInfo()");
        print("\tdb.dropUser(username)");
        print("\tdb.repairDatabase()");
        print("\tdb.resetError()");
        print(
            "\tdb.runCommand(cmdObj) run a database command.  if cmdObj is a string, turns it into { cmdObj : 1 }");
        print("\tdb.serverStatus()");
        print("\tdb.setLogLevel(level,<component>)");
        print("\tdb.setProfilingLevel(level,<slowms>) 0=off 1=slow 2=all");
        print(
            "\tdb.setWriteConcern( <write concern doc> ) - sets the write concern for writes to the db");
        print(
            "\tdb.unsetWriteConcern( <write concern doc> ) - unsets the write concern for writes to the db");
        print("\tdb.setVerboseShell(flag) display extra information in shell output");
        print("\tdb.shutdownServer()");
        print("\tdb.stats()");
        print("\tdb.version() current version of the server");

        return __magicNoPrint;
    };

    DB.prototype.printCollectionStats = function(scale) {
        if (arguments.length > 1) {
            print("printCollectionStats() has a single optional argument (scale)");
            return;
        }
        if (typeof scale != 'undefined') {
            if (typeof scale != 'number') {
                print("scale has to be a number >= 1");
                return;
            }
            if (scale < 1) {
                print("scale has to be >= 1");
                return;
            }
        }
        var mydb = this;
        this.getCollectionNames().forEach(function(z) {
            print(z);
            printjson(mydb.getCollection(z).stats(scale));
            print("---");
        });
    };

    /**
     * <p> Set profiling level for your db.  Profiling gathers stats on query performance. </p>
     *
     * <p>Default is off, and resets to off on a database restart -- so if you want it on,
     *    turn it on periodically. </p>
     *
     *  <p>Levels :</p>
     *   <ul>
     *    <li>0=off</li>
     *    <li>1=log very slow operations; optional argument slowms specifies slowness threshold</li>
     *    <li>2=log all</li>
     *  @param {String} level Desired level of profiling
     *  @param {String} slowms For slow logging, query duration that counts as slow (default 100ms)
     *  @return SOMETHING_FIXME or null on error
     */
    DB.prototype.setProfilingLevel = function(level, slowms) {

        if (level < 0 || level > 2) {
            var errorText = "input level " + level + " is out of range [0..2]";
            var errorObject = new Error(errorText);
            errorObject['dbSetProfilingException'] = errorText;
            throw errorObject;
        }

        var cmd = {
            profile: level
        };
        if (isNumber(slowms))
            cmd["slowms"] = slowms;
        return assert.commandWorked(this._dbCommand(cmd));
    };

    /**
     * @deprecated
     *  <p> Evaluate a js expression at the database server.</p>
     *
     * <p>Useful if you need to touch a lot of data lightly; in such a scenario
     *  the network transfer of the data could be a bottleneck.  A good example
     *  is "select count(*)" -- can be done server side via this mechanism.
     * </p>
     *
     * <p>
     * If the eval fails, an exception is thrown of the form:
     * </p>
     * <code>{ dbEvalException: { retval: functionReturnValue, ok: num [, errno: num] [, errmsg:
     *str] } }</code>
     *
     * <p>Example: </p>
     * <code>print( "mycount: " + db.eval( function(){db.mycoll.find({},{_id:ObjId()}).length();}
     *);</code>
     *
     * @param {Function} jsfunction Javascript function to run on server.  Note this it not a
     *closure, but rather just "code".
     * @return result of your function, or null if error
     *
     */
    DB.prototype.eval = function(jsfunction) {
        print("WARNING: db.eval is deprecated");

        var cmd = {
            $eval: jsfunction
        };
        if (arguments.length > 1) {
            cmd.args = Array.from(arguments).slice(1);
        }

        var res = this._dbCommand(cmd);

        if (!res.ok)
            throw _getErrorWithCode(res, tojson(res));

        return res.retval;
    };

    DB.prototype.dbEval = DB.prototype.eval;

    /**
     *
     *  <p>
     *   Similar to SQL group by.  For example: </p>
     *
     *  <code>select a,b,sum(c) csum from coll where active=1 group by a,b</code>
     *
     *  <p>
     *    corresponds to the following in 10gen:
     *  </p>
     *
     *  <code>
        db.group(
            {
                ns: "coll",
                key: { a:true, b:true },
                // keyf: ...,
                cond: { active:1 },
                reduce: function(obj,prev) { prev.csum += obj.c; },
                initial: { csum: 0 }
            });
        </code>
     *
     *
     * <p>
     *  An array of grouped items is returned.  The array must fit in RAM, thus this function is not
     * suitable when the return set is extremely large.
     * </p>
     * <p>
     * To order the grouped data, simply sort it client side upon return.
     * <p>
       Defaults
         cond may be null if you want to run against all rows in the collection
         keyf is a function which takes an object and returns the desired key.  set either key or
     keyf (not both).
     * </p>
     */
    DB.prototype.groupeval = function(parmsObj) {

        var groupFunction = function() {
            var parms = args[0];
            var c = db[parms.ns].find(parms.cond || {});
            var map = new Map();
            var pks = parms.key ? Object.keySet(parms.key) : null;
            var pkl = pks ? pks.length : 0;
            var key = {};

            while (c.hasNext()) {
                var obj = c.next();
                if (pks) {
                    for (var i = 0; i < pkl; i++) {
                        var k = pks[i];
                        key[k] = obj[k];
                    }
                } else {
                    key = parms.$keyf(obj);
                }

                var aggObj = map.get(key);
                if (aggObj == null) {
                    var newObj = Object.extend({}, key);  // clone
                    aggObj = Object.extend(newObj, parms.initial);
                    map.put(key, aggObj);
                }
                parms.$reduce(obj, aggObj);
            }

            return map.values();
        };

        return this.eval(groupFunction, this._groupFixParms(parmsObj));
    };

    DB.prototype.groupcmd = function(parmsObj) {
        var ret = this.runCommand({"group": this._groupFixParms(parmsObj)});
        if (!ret.ok) {
            throw _getErrorWithCode(ret, "group command failed: " + tojson(ret));
        }
        return ret.retval;
    };

    DB.prototype.group = DB.prototype.groupcmd;

    DB.prototype._groupFixParms = function(parmsObj) {
        var parms = Object.extend({}, parmsObj);

        if (parms.reduce) {
            parms.$reduce = parms.reduce;  // must have $ to pass to db
            delete parms.reduce;
        }

        if (parms.keyf) {
            parms.$keyf = parms.keyf;
            delete parms.keyf;
        }

        return parms;
    };

    DB.prototype.resetError = function() {
        return this.runCommand({reseterror: 1});
    };

    DB.prototype.forceError = function() {
        return this.runCommand({forceerror: 1});
    };

    DB.prototype.getLastError = function(w, wtimeout) {
        var res = this.getLastErrorObj(w, wtimeout);
        if (!res.ok)
            throw _getErrorWithCode(ret, "getlasterror failed: " + tojson(res));
        return res.err;
    };
    DB.prototype.getLastErrorObj = function(w, wtimeout) {
        var cmd = {
            getlasterror: 1
        };
        if (w) {
            cmd.w = w;
            if (wtimeout)
                cmd.wtimeout = wtimeout;
        }
        var res = this.runCommand(cmd);

        if (!res.ok)
            throw _getErrorWithCode(res, "getlasterror failed: " + tojson(res));
        return res;
    };
    DB.prototype.getLastErrorCmd = DB.prototype.getLastErrorObj;

    /* Return the last error which has occurred, even if not the very last error.

       Returns:
        { err : <error message>, nPrev : <how_many_ops_back_occurred>, ok : 1 }

       result.err will be null if no error has occurred.
     */
    DB.prototype.getPrevError = function() {
        return this.runCommand({getpreverror: 1});
    };

    DB.prototype._getCollectionInfosSystemNamespaces = function(filter) {
        var all = [];

        var dbNamePrefix = this._name + ".";

        // Create a shallow copy of 'filter' in case we modify its 'name' property. Also defaults
        // 'filter' to {} if the parameter was not specified.
        filter = Object.extend({}, filter);
        if (typeof filter.name === "string") {
            // Queries on the 'name' field need to qualify the namespace with the database name for
            // consistency with the command variant.
            filter.name = dbNamePrefix + filter.name;
        }

        var c = this.getCollection("system.namespaces").find(filter);
        while (c.hasNext()) {
            var infoObj = c.next();

            if (infoObj.name.indexOf("$") >= 0 && infoObj.name.indexOf(".oplog.$") < 0)
                continue;

            // Remove the database name prefix from the collection info object.
            infoObj.name = infoObj.name.substring(dbNamePrefix.length);

            all.push(infoObj);
        }

        // Return list of objects sorted by collection name.
        return all.sort(function(coll1, coll2) {
            return coll1.name.localeCompare(coll2.name);
        });
    };

    DB.prototype._getCollectionInfosCommand = function(filter) {
        filter = filter || {};
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
    };

    /**
     * Returns a list that contains the names and options of this database's collections, sorted by
     * collection name. An optional filter can be specified to match only collections with certain
     * metadata.
     */
    DB.prototype.getCollectionInfos = function(filter) {
        var res = this._getCollectionInfosCommand(filter);
        if (res) {
            return res;
        }
        return this._getCollectionInfosSystemNamespaces(filter);
    };

    /**
     * Returns this database's list of collection names in sorted order.
     */
    DB.prototype.getCollectionNames = function() {
        return this.getCollectionInfos().map(function(infoObj) {
            return infoObj.name;
        });
    };

    DB.prototype.tojson = function() {
        return this._name;
    };

    DB.prototype.toString = function() {
        return this._name;
    };

    DB.prototype.isMaster = function() {
        return this.runCommand("isMaster");
    };

    var commandUnsupported = function(res) {
        return (!res.ok &&
                (res.errmsg.startsWith("no such cmd") || res.errmsg.startsWith("no such command") ||
                 res.code === 59 /* CommandNotFound */));
    };

    DB.prototype.currentOp = function(arg) {
        var q = {};
        if (arg) {
            if (typeof(arg) == "object")
                Object.extend(q, arg);
            else if (arg)
                q["$all"] = true;
        }

        var commandObj = {
            "currentOp": 1
        };
        Object.extend(commandObj, q);
        var res = this.adminCommand(commandObj);
        if (commandUnsupported(res)) {
            // always send legacy currentOp with default (null) read preference (SERVER-17951)
            var _readPref = this.getMongo().getReadPrefMode();
            try {
                this.getMongo().setReadPref(null);
                res = this.getSiblingDB("admin").$cmd.sys.inprog.findOne(q);
            } finally {
                this.getMongo().setReadPref(_readPref);
            }
        }
        return res;
    };
    DB.prototype.currentOP = DB.prototype.currentOp;

    DB.prototype.killOp = function(op) {
        if (!op)
            throw Error("no opNum to kill specified");
        var res = this.adminCommand({'killOp': 1, 'op': op});
        if (commandUnsupported(res)) {
            // fall back for old servers
            var _readPref = this.getMongo().getReadPrefMode();
            try {
                this.getMongo().setReadPref(null);
                res = this.getSiblingDB("admin").$cmd.sys.killop.findOne({'op': op});
            } finally {
                this.getMongo().setReadPref(_readPref);
            }
        }
        return res;
    };
    DB.prototype.killOP = DB.prototype.killOp;

    DB.tsToSeconds = function(x) {
        if (x.t && x.i)
            return x.t;
        return x / 4294967296;  // low 32 bits are ordinal #s within a second
    };

    /**
      Get a replication log information summary.
      <p>
      This command is for the database/cloud administer and not applicable to most databases.
      It is only used with the local database.  One might invoke from the JS shell:
      <pre>
           use local
           db.getReplicationInfo();
      </pre>
      It is assumed that this database is a replication master -- the information returned is
      about the operation log stored at local.oplog.$main on the replication master.  (It also
      works on a machine in a replica pair: for replica pairs, both machines are "masters" from
      an internal database perspective.
      <p>
      * @return Object timeSpan: time span of the oplog from start to end  if slave is more out
      *                          of date than that, it can't recover without a complete resync
    */
    DB.prototype.getReplicationInfo = function() {
        var localdb = this.getSiblingDB("local");

        var result = {};
        var oplog;
        var localCollections = localdb.getCollectionNames();
        if (localCollections.indexOf('oplog.rs') >= 0) {
            oplog = 'oplog.rs';
        } else if (localCollections.indexOf('oplog.$main') >= 0) {
            oplog = 'oplog.$main';
        } else {
            result.errmsg = "neither master/slave nor replica set replication detected";
            return result;
        }

        var ol = localdb.getCollection(oplog);
        var ol_stats = ol.stats();
        if (ol_stats && ol_stats.maxSize) {
            result.logSizeMB = ol_stats.maxSize / (1024 * 1024);
        } else {
            result.errmsg = "Could not get stats for local." + oplog + " collection. " +
                "collstats returned: " + tojson(ol_stats);
            return result;
        }

        result.usedMB = ol_stats.size / (1024 * 1024);
        result.usedMB = Math.ceil(result.usedMB * 100) / 100;

        var firstc = ol.find().sort({$natural: 1}).limit(1);
        var lastc = ol.find().sort({$natural: -1}).limit(1);
        if (!firstc.hasNext() || !lastc.hasNext()) {
            result.errmsg =
                "objects not found in local.oplog.$main -- is this a new and empty db instance?";
            result.oplogMainRowCount = ol.count();
            return result;
        }

        var first = firstc.next();
        var last = lastc.next();
        var tfirst = first.ts;
        var tlast = last.ts;

        if (tfirst && tlast) {
            tfirst = DB.tsToSeconds(tfirst);
            tlast = DB.tsToSeconds(tlast);
            result.timeDiff = tlast - tfirst;
            result.timeDiffHours = Math.round(result.timeDiff / 36) / 100;
            result.tFirst = (new Date(tfirst * 1000)).toString();
            result.tLast = (new Date(tlast * 1000)).toString();
            result.now = Date();
        } else {
            result.errmsg = "ts element not found in oplog objects";
        }

        return result;
    };

    DB.prototype.printReplicationInfo = function() {
        var result = this.getReplicationInfo();
        if (result.errmsg) {
            var isMaster = this.isMaster();
            if (isMaster.arbiterOnly) {
                print("cannot provide replication status from an arbiter.");
                return;
            } else if (!isMaster.ismaster) {
                print("this is a slave, printing slave replication info.");
                this.printSlaveReplicationInfo();
                return;
            }
            print(tojson(result));
            return;
        }
        print("configured oplog size:   " + result.logSizeMB + "MB");
        print("log length start to end: " + result.timeDiff + "secs (" + result.timeDiffHours +
              "hrs)");
        print("oplog first event time:  " + result.tFirst);
        print("oplog last event time:   " + result.tLast);
        print("now:                     " + result.now);
    };

    DB.prototype.printSlaveReplicationInfo = function() {
        var startOptimeDate = null;
        var primary = null;

        function getReplLag(st) {
            assert(startOptimeDate, "how could this be null (getReplLag startOptimeDate)");
            print("\tsyncedTo: " + st.toString());
            var ago = (startOptimeDate - st) / 1000;
            var hrs = Math.round(ago / 36) / 100;
            var suffix = "";
            if (primary) {
                suffix = "primary ";
            } else {
                suffix = "freshest member (no primary available at the moment)";
            }
            print("\t" + Math.round(ago) + " secs (" + hrs + " hrs) behind the " + suffix);
        }

        function getMaster(members) {
            for (i in members) {
                var row = members[i];
                if (row.state === 1) {
                    return row;
                }
            }

            return null;
        }

        function g(x) {
            assert(x, "how could this be null (printSlaveReplicationInfo gx)");
            print("source: " + x.host);
            if (x.syncedTo) {
                var st = new Date(DB.tsToSeconds(x.syncedTo) * 1000);
                getReplLag(st);
            } else {
                print("\tdoing initial sync");
            }
        }

        function r(x) {
            assert(x, "how could this be null (printSlaveReplicationInfo rx)");
            if (x.state == 1 || x.state == 7) {  // ignore primaries (1) and arbiters (7)
                return;
            }

            print("source: " + x.name);
            if (x.optime) {
                getReplLag(x.optimeDate);
            } else {
                print("\tno replication info, yet.  State: " + x.stateStr);
            }
        }

        var L = this.getSiblingDB("local");

        if (L.system.replset.count() != 0) {
            var status = this.adminCommand({'replSetGetStatus': 1});
            primary = getMaster(status.members);
            if (primary) {
                startOptimeDate = primary.optimeDate;
            }
            // no primary, find the most recent op among all members
            else {
                startOptimeDate = new Date(0, 0);
                for (i in status.members) {
                    if (status.members[i].optimeDate > startOptimeDate) {
                        startOptimeDate = status.members[i].optimeDate;
                    }
                }
            }

            for (i in status.members) {
                r(status.members[i]);
            }
        } else if (L.sources.count() != 0) {
            startOptimeDate = new Date();
            L.sources.find().forEach(g);
        } else {
            print("local.sources is empty; is this db a --slave?");
            return;
        }
    };

    DB.prototype.serverBuildInfo = function() {
        return this._adminCommand("buildinfo");
    };

    // Used to trim entries from the metrics.commands that have never been executed
    getActiveCommands = function(tree) {
        var result = {};
        for (var i in tree) {
            if (!tree.hasOwnProperty(i))
                continue;
            if (tree[i].hasOwnProperty("total")) {
                if (tree[i].total > 0) {
                    result[i] = tree[i];
                }
                continue;
            }
            if (i == "<UNKNOWN>") {
                if (tree[i] > 0) {
                    result[i] = tree[i];
                }
                continue;
            }
            // Handles nested commands
            var subStatus = getActiveCommands(tree[i]);
            if (Object.keys(subStatus).length > 0) {
                result[i] = tree[i];
            }
        }
        return result;
    };

    DB.prototype.serverStatus = function(options) {
        var cmd = {
            serverStatus: 1
        };
        if (options) {
            Object.extend(cmd, options);
        }
        var res = this._adminCommand(cmd);
        // Only prune if we have a metrics tree with commands.
        if (res.metrics && res.metrics.commands) {
            res.metrics.commands = getActiveCommands(res.metrics.commands);
        }
        return res;
    };

    DB.prototype.hostInfo = function() {
        return this._adminCommand("hostInfo");
    };

    DB.prototype.serverCmdLineOpts = function() {
        return this._adminCommand("getCmdLineOpts");
    };

    DB.prototype.version = function() {
        return this.serverBuildInfo().version;
    };

    DB.prototype.serverBits = function() {
        return this.serverBuildInfo().bits;
    };

    DB.prototype.listCommands = function() {
        var x = this.runCommand("listCommands");
        for (var name in x.commands) {
            var c = x.commands[name];

            var s = name + ": ";

            if (c.adminOnly)
                s += " adminOnly ";
            if (c.slaveOk)
                s += " slaveOk ";

            s += "\n  ";
            s += c.help.replace(/\n/g, '\n  ');
            s += "\n";

            print(s);
        }
    };

    DB.prototype.printShardingStatus = function(verbose) {
        printShardingStatus(this.getSiblingDB("config"), verbose);
    };

    DB.prototype.fsyncLock = function() {
        return this.adminCommand({fsync: 1, lock: true});
    };

    DB.prototype.fsyncUnlock = function() {
        var res = this.adminCommand({fsyncUnlock: 1});
        if (commandUnsupported(res)) {
            var _readPref = this.getMongo().getReadPrefMode();
            try {
                this.getMongo().setReadPref(null);
                res = this.getSiblingDB("admin").$cmd.sys.unlock.findOne();
            } finally {
                this.getMongo().setReadPref(_readPref);
            }
        }
        return res;
    };

    DB.autocomplete = function(obj) {
        var colls = obj.getCollectionNames();
        var ret = [];
        for (var i = 0; i < colls.length; i++) {
            if (colls[i].match(/^[a-zA-Z0-9_.\$]+$/))
                ret.push(colls[i]);
        }
        return ret;
    };

    DB.prototype.setSlaveOk = function(value) {
        if (value == undefined)
            value = true;
        this._slaveOk = value;
    };

    DB.prototype.getSlaveOk = function() {
        if (this._slaveOk != undefined)
            return this._slaveOk;
        return this._mongo.getSlaveOk();
    };

    DB.prototype.getQueryOptions = function() {
        var options = 0;
        if (this.getSlaveOk())
            options |= 4;
        return options;
    };

    /* Loads any scripts contained in system.js into the client shell.
    */
    DB.prototype.loadServerScripts = function() {
        var global = Function('return this')();
        this.system.js.find().forEach(function(u) {
            if (u.value.constructor === Code) {
                global[u._id] = eval("(" + u.value.code + ")");
            } else {
                global[u._id] = u.value;
            }
        });
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////// Security shell helpers below
    /////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    var _defaultWriteConcern = {
        w: 'majority',
        wtimeout: 30 * 1000
    };

    function getUserObjString(userObj) {
        var pwd = userObj.pwd;
        delete userObj.pwd;
        var toreturn = tojson(userObj);
        userObj.pwd = pwd;
        return toreturn;
    }

    DB.prototype._modifyCommandToDigestPasswordIfNecessary = function(cmdObj, username) {
        if (!cmdObj["pwd"]) {
            return;
        }
        if (cmdObj.hasOwnProperty("digestPassword")) {
            throw Error(
                "Cannot specify 'digestPassword' through the user management shell helpers, " +
                "use 'passwordDigestor' instead");
        }
        var passwordDigestor = cmdObj["passwordDigestor"] ? cmdObj["passwordDigestor"] : "client";
        if (passwordDigestor == "server") {
            cmdObj["digestPassword"] = true;
        } else if (passwordDigestor == "client") {
            cmdObj["pwd"] = _hashPassword(username, cmdObj["pwd"]);
            cmdObj["digestPassword"] = false;
        } else {
            throw Error("'passwordDigestor' must be either 'server' or 'client', got: '" +
                        passwordDigestor + "'");
        }
        delete cmdObj["passwordDigestor"];
    };

    DB.prototype.createUser = function(userObj, writeConcern) {
        var name = userObj["user"];
        var cmdObj = {
            createUser: name
        };
        cmdObj = Object.extend(cmdObj, userObj);
        delete cmdObj["user"];

        this._modifyCommandToDigestPasswordIfNecessary(cmdObj, name);

        cmdObj["writeConcern"] = writeConcern ? writeConcern : _defaultWriteConcern;

        var res = this.runCommand(cmdObj);

        if (res.ok) {
            print("Successfully added user: " + getUserObjString(userObj));
            return;
        }

        if (res.errmsg == "no such cmd: createUser") {
            throw Error("'createUser' command not found.  This is most likely because you are " +
                        "talking to an old (pre v2.6) MongoDB server");
        }

        if (res.errmsg == "timeout") {
            throw Error("timed out while waiting for user authentication to replicate - " +
                        "database will not be fully secured until replication finishes");
        }

        throw _getErrorWithCode(res, "couldn't add user: " + res.errmsg);
    };

    function _hashPassword(username, password) {
        if (typeof password != 'string') {
            throw Error("User passwords must be of type string. Was given password with type: " +
                        typeof(password));
        }
        return hex_md5(username + ":mongo:" + password);
    }

    /**
     * Used for updating users in systems with V1 style user information
     * (ie MongoDB v2.4 and prior)
     */
    DB.prototype._updateUserV1 = function(name, updateObject, writeConcern) {
        var setObj = {};
        if (updateObject.pwd) {
            setObj["pwd"] = _hashPassword(name, updateObject.pwd);
        }
        if (updateObject.extraData) {
            setObj["extraData"] = updateObject.extraData;
        }
        if (updateObject.roles) {
            setObj["roles"] = updateObject.roles;
        }

        this.system.users.update({user: name, userSource: null}, {$set: setObj});
        var errObj = this.getLastErrorObj(writeConcern['w'], writeConcern['wtimeout']);
        if (errObj.err) {
            throw _getErrorWithCode(errObj, "Updating user failed: " + errObj.err);
        }
    };

    DB.prototype.updateUser = function(name, updateObject, writeConcern) {
        var cmdObj = {
            updateUser: name
        };
        cmdObj = Object.extend(cmdObj, updateObject);
        cmdObj['writeConcern'] = writeConcern ? writeConcern : _defaultWriteConcern;
        this._modifyCommandToDigestPasswordIfNecessary(cmdObj, name);

        var res = this.runCommand(cmdObj);
        if (res.ok) {
            return;
        }

        if (res.errmsg == "no such cmd: updateUser") {
            this._updateUserV1(name, updateObject, cmdObj['writeConcern']);
            return;
        }

        throw _getErrorWithCode(res, "Updating user failed: " + res.errmsg);
    };

    DB.prototype.changeUserPassword = function(username, password, writeConcern) {
        this.updateUser(username, {pwd: password}, writeConcern);
    };

    DB.prototype.logout = function() {
        return this.getMongo().logout(this.getName());
    };

    // For backwards compatibility
    DB.prototype.removeUser = function(username, writeConcern) {
        print("WARNING: db.removeUser has been deprecated, please use db.dropUser instead");
        return this.dropUser(username, writeConcern);
    };

    DB.prototype.dropUser = function(username, writeConcern) {
        var cmdObj = {
            dropUser: username,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        };
        var res = this.runCommand(cmdObj);

        if (res.ok) {
            return true;
        }

        if (res.code == 11) {  // Code 11 = UserNotFound
            return false;
        }

        if (res.errmsg == "no such cmd: dropUsers") {
            return this._removeUserV1(username, cmdObj['writeConcern']);
        }

        throw _getErrorWithCode(res, res.errmsg);
    };

    /**
     * Used for removing users in systems with V1 style user information
     * (ie MongoDB v2.4 and prior)
     */
    DB.prototype._removeUserV1 = function(username, writeConcern) {
        this.getCollection("system.users").remove({user: username});

        var le = this.getLastErrorObj(writeConcern['w'], writeConcern['wtimeout']);

        if (le.err) {
            throw _getErrorWithCode(le, "Couldn't remove user: " + le.err);
        }

        if (le.n == 1) {
            return true;
        } else {
            return false;
        }
    };

    DB.prototype.dropAllUsers = function(writeConcern) {
        var res = this.runCommand({
            dropAllUsersFromDatabase: 1,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        });

        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }

        return res.n;
    };

    DB.prototype.__pwHash = function(nonce, username, pass) {
        return hex_md5(nonce + username + _hashPassword(username, pass));
    };

    DB.prototype._defaultAuthenticationMechanism = null;

    DB.prototype._getDefaultAuthenticationMechanism = function() {
        // Use the default auth mechanism if set on the command line.
        if (this._defaultAuthenticationMechanism != null)
            return this._defaultAuthenticationMechanism;

        // Use MONGODB-CR for v2.6 and earlier.
        maxWireVersion = this.isMaster().maxWireVersion;
        if (maxWireVersion == undefined || maxWireVersion < 3) {
            return "MONGODB-CR";
        }
        return "SCRAM-SHA-1";
    };

    DB.prototype._defaultGssapiServiceName = null;

    DB.prototype._authOrThrow = function() {
        var params;
        if (arguments.length == 2) {
            params = {
                user: arguments[0],
                pwd: arguments[1]
            };
        } else if (arguments.length == 1) {
            if (typeof(arguments[0]) != "object")
                throw Error("Single-argument form of auth expects a parameter object");
            params = Object.extend({}, arguments[0]);
        } else {
            throw Error(
                "auth expects either (username, password) or ({ user: username, pwd: password })");
        }

        if (params.mechanism === undefined)
            params.mechanism = this._getDefaultAuthenticationMechanism();

        if (params.db !== undefined) {
            throw Error("Do not override db field on db.auth(). Use getMongo().auth(), instead.");
        }

        if (params.mechanism == "GSSAPI" && params.serviceName == null &&
            this._defaultGssapiServiceName != null) {
            params.serviceName = this._defaultGssapiServiceName;
        }

        params.db = this.getName();
        var good = this.getMongo().auth(params);
        if (good) {
            // auth enabled, and should try to use isMaster and replSetGetStatus to build prompt
            this.getMongo().authStatus = {
                authRequired: true,
                isMaster: true,
                replSetGetStatus: true
            };
        }

        return good;
    };

    DB.prototype.auth = function() {
        var ex;
        try {
            this._authOrThrow.apply(this, arguments);
        } catch (ex) {
            print(ex);
            return 0;
        }
        return 1;
    };

    DB.prototype.grantRolesToUser = function(username, roles, writeConcern) {
        var cmdObj = {
            grantRolesToUser: username,
            roles: roles,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        };
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }
    };

    DB.prototype.revokeRolesFromUser = function(username, roles, writeConcern) {
        var cmdObj = {
            revokeRolesFromUser: username,
            roles: roles,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        };
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }
    };

    DB.prototype.getUser = function(username, args) {
        if (typeof username != "string") {
            throw Error("User name for getUser shell helper must be a string");
        }
        var cmdObj = {
            usersInfo: username
        };
        Object.extend(cmdObj, args);

        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }

        if (res.users.length == 0) {
            return null;
        }
        return res.users[0];
    };

    DB.prototype.getUsers = function(args) {
        var cmdObj = {
            usersInfo: 1
        };
        Object.extend(cmdObj, args);
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            var authSchemaIncompatibleCode = 69;
            if (res.code == authSchemaIncompatibleCode ||
                (res.code == null && res.errmsg == "no such cmd: usersInfo")) {
                // Working with 2.4 schema user data
                return this.system.users.find({}).toArray();
            }

            throw _getErrorWithCode(res, res.errmsg);
        }

        return res.users;
    };

    DB.prototype.createRole = function(roleObj, writeConcern) {
        var name = roleObj["role"];
        var cmdObj = {
            createRole: name
        };
        cmdObj = Object.extend(cmdObj, roleObj);
        delete cmdObj["role"];
        cmdObj["writeConcern"] = writeConcern ? writeConcern : _defaultWriteConcern;

        var res = this.runCommand(cmdObj);

        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }
        printjson(roleObj);
    };

    DB.prototype.updateRole = function(name, updateObject, writeConcern) {
        var cmdObj = {
            updateRole: name
        };
        cmdObj = Object.extend(cmdObj, updateObject);
        cmdObj['writeConcern'] = writeConcern ? writeConcern : _defaultWriteConcern;
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }
    };

    DB.prototype.dropRole = function(name, writeConcern) {
        var cmdObj = {
            dropRole: name,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        };
        var res = this.runCommand(cmdObj);

        if (res.ok) {
            return true;
        }

        if (res.code == 31) {  // Code 31 = RoleNotFound
            return false;
        }

        throw _getErrorWithCode(res, res.errmsg);
    };

    DB.prototype.dropAllRoles = function(writeConcern) {
        var res = this.runCommand({
            dropAllRolesFromDatabase: 1,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        });

        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }

        return res.n;
    };

    DB.prototype.grantRolesToRole = function(rolename, roles, writeConcern) {
        var cmdObj = {
            grantRolesToRole: rolename,
            roles: roles,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        };
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }
    };

    DB.prototype.revokeRolesFromRole = function(rolename, roles, writeConcern) {
        var cmdObj = {
            revokeRolesFromRole: rolename,
            roles: roles,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        };
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }
    };

    DB.prototype.grantPrivilegesToRole = function(rolename, privileges, writeConcern) {
        var cmdObj = {
            grantPrivilegesToRole: rolename,
            privileges: privileges,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        };
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }
    };

    DB.prototype.revokePrivilegesFromRole = function(rolename, privileges, writeConcern) {
        var cmdObj = {
            revokePrivilegesFromRole: rolename,
            privileges: privileges,
            writeConcern: writeConcern ? writeConcern : _defaultWriteConcern
        };
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }
    };

    DB.prototype.getRole = function(rolename, args) {
        if (typeof rolename != "string") {
            throw Error("Role name for getRole shell helper must be a string");
        }
        var cmdObj = {
            rolesInfo: rolename
        };
        Object.extend(cmdObj, args);
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }

        if (res.roles.length == 0) {
            return null;
        }
        return res.roles[0];
    };

    DB.prototype.getRoles = function(args) {
        var cmdObj = {
            rolesInfo: 1
        };
        Object.extend(cmdObj, args);
        var res = this.runCommand(cmdObj);
        if (!res.ok) {
            throw _getErrorWithCode(res, res.errmsg);
        }

        return res.roles;
    };

    DB.prototype.setWriteConcern = function(wc) {
        if (wc instanceof WriteConcern) {
            this._writeConcern = wc;
        } else {
            this._writeConcern = new WriteConcern(wc);
        }
    };

    DB.prototype.getWriteConcern = function() {
        if (this._writeConcern)
            return this._writeConcern;

        if (this._mongo.getWriteConcern())
            return this._mongo.getWriteConcern();

        return null;
    };

    DB.prototype.unsetWriteConcern = function() {
        delete this._writeConcern;
    };

    DB.prototype.getLogComponents = function() {
        return this.getMongo().getLogComponents();
    };

    DB.prototype.setLogLevel = function(logLevel, component) {
        return this.getMongo().setLogLevel(logLevel, component);
    };

}());



// ---- MODULE: port_db ---- 
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


// ---- MODULE: explainable ---- 
//
// A view of a collection against which operations are explained rather than executed
// normally.
//

var Explainable = (function() {

    var parseVerbosity = function(verbosity) {
        // Truthy non-strings are interpreted as "allPlansExecution" verbosity.
        if (verbosity && (typeof verbosity !== "string")) {
            return "allPlansExecution";
        }

        // Falsy non-strings are interpreted as "queryPlanner" verbosity.
        if (!verbosity && (typeof verbosity !== "string")) {
            return "queryPlanner";
        }

        // If we're here, then the verbosity is a string. We reject invalid strings.
        if (verbosity !== "queryPlanner" && verbosity !== "executionStats" &&
            verbosity !== "allPlansExecution") {
            throw Error("explain verbosity must be one of {" + "'queryPlanner'," +
                        "'executionStats'," + "'allPlansExecution'}");
        }

        return verbosity;
    };

    var throwOrReturn = function(explainResult) {
        if (("ok" in explainResult && !explainResult.ok) || explainResult.$err) {
            throw _getErrorWithCode(explainResult, "explain failed: " + tojson(explainResult));
        }

        return explainResult;
    };

    function constructor(collection, verbosity) {
        //
        // Private vars.
        //

        this._collection = collection;
        this._verbosity = parseVerbosity(verbosity);

        //
        // Public methods.
        //

        this.getCollection = function() {
            return this._collection;
        };

        this.getVerbosity = function() {
            return this._verbosity;
        };

        this.setVerbosity = function(verbosity) {
            this._verbosity = parseVerbosity(verbosity);
            return this;
        };

        this.help = function() {
            print("Explainable operations");
            print("\t.aggregate(...) - explain an aggregation operation");
            print("\t.count(...) - explain a count operation");
            print("\t.distinct(...) - explain a distinct operation");
            print("\t.find(...) - get an explainable query");
            print("\t.findAndModify(...) - explain a findAndModify operation");
            print("\t.group(...) - explain a group operation");
            print("\t.remove(...) - explain a remove operation");
            print("\t.update(...) - explain an update operation");
            print("Explainable collection methods");
            print("\t.getCollection()");
            print("\t.getVerbosity()");
            print("\t.setVerbosity(verbosity)");
            return __magicNoPrint;
        };

        //
        // Pretty representations.
        //

        this.toString = function() {
            return "Explainable(" + this._collection.getFullName() + ")";
        };

        this.shellPrint = function() {
            return this.toString();
        };

        //
        // Explainable operations.
        //

        /**
         * Adds "explain: true" to "extraOpts", and then passes through to the regular collection's
         * aggregate helper.
         */
        this.aggregate = function(pipeline, extraOpts) {
            if (!(pipeline instanceof Array)) {
                // support legacy varargs form. (Also handles db.foo.aggregate())
                pipeline = Array.from(arguments);
                extraOpts = {};
            }

            // Add the explain option.
            extraOpts = extraOpts || {};
            extraOpts.explain = true;

            return this._collection.aggregate(pipeline, extraOpts);
        };

        this.count = function(query) {
            return this.find(query).count();
        };

        /**
         * .explain().find() and .find().explain() mean the same thing. In both cases, we use
         * the DBExplainQuery abstraction in order to construct the proper explain command to send
         * to the server.
         */
        this.find = function() {
            var cursor = this._collection.find.apply(this._collection, arguments);
            return new DBExplainQuery(cursor, this._verbosity);
        };

        this.findAndModify = function(params) {
            var famCmd = Object.extend({"findAndModify": this._collection.getName()}, params);
            var explainCmd = {
                "explain": famCmd,
                "verbosity": this._verbosity
            };
            var explainResult = this._collection.runReadCommand(explainCmd);
            return throwOrReturn(explainResult);
        };

        this.group = function(params) {
            params.ns = this._collection.getName();
            var grpCmd = {
                "group": this._collection.getDB()._groupFixParms(params)
            };
            var explainCmd = {
                "explain": grpCmd,
                "verbosity": this._verbosity
            };
            var explainResult = this._collection.runReadCommand(explainCmd);
            return throwOrReturn(explainResult);
        };

        this.distinct = function(keyString, query, options) {
            var distinctCmd = {
                distinct: this._collection.getName(),
                key: keyString,
                query: query || {}
            };

            if (options && options.hasOwnProperty("collation")) {
                distinctCmd.collation = options.collation;
            }

            var explainCmd = {
                explain: distinctCmd,
                verbosity: this._verbosity
            };
            var explainResult = this._collection.runReadCommand(explainCmd);
            return throwOrReturn(explainResult);
        };

        this.remove = function() {
            var parsed = this._collection._parseRemove.apply(this._collection, arguments);
            var query = parsed.query;
            var justOne = parsed.justOne;
            var collation = parsed.collation;

            var bulk = this._collection.initializeOrderedBulkOp();
            var removeOp = bulk.find(query);

            if (collation) {
                removeOp.collation(collation);
            }

            if (justOne) {
                removeOp.removeOne();
            } else {
                removeOp.remove();
            }

            var explainCmd = bulk.convertToExplainCmd(this._verbosity);
            var explainResult = this._collection.runCommand(explainCmd);
            return throwOrReturn(explainResult);
        };

        this.update = function() {
            var parsed = this._collection._parseUpdate.apply(this._collection, arguments);
            var query = parsed.query;
            var obj = parsed.obj;
            var upsert = parsed.upsert;
            var multi = parsed.multi;
            var collation = parsed.collation;

            var bulk = this._collection.initializeOrderedBulkOp();
            var updateOp = bulk.find(query);

            if (upsert) {
                updateOp = updateOp.upsert();
            }

            if (collation) {
                updateOp.collation(collation);
            }

            if (multi) {
                updateOp.update(obj);
            } else {
                updateOp.updateOne(obj);
            }

            var explainCmd = bulk.convertToExplainCmd(this._verbosity);
            var explainResult = this._collection.runCommand(explainCmd);
            return throwOrReturn(explainResult);
        };
    }

    //
    // Public static methods.
    //

    constructor.parseVerbosity = parseVerbosity;
    constructor.throwOrReturn = throwOrReturn;

    return constructor;
})();

/**
 * This is the user-facing method for creating an Explainable from a collection.
 */
DBCollection.prototype.explain = function(verbosity) {
    return new Explainable(this, verbosity);
};



// ---- MODULE: explain_query ---- 
//
// A DBQuery which is explained rather than executed normally. Also could be thought of as
// an "explainable cursor". Explains of .find() operations run through this abstraction.
//

var DBExplainQuery = (function() {

    //
    // Private methods.
    //

    /**
     * In 2.6 and before, .explain(), .explain(false), or .explain(<falsy value>) instructed the
     * shell to reduce the explain verbosity by removing certain fields from the output. This
     * is implemented here for backwards compatibility.
     */
    function removeVerboseFields(obj) {
        if (typeof(obj) !== "object") {
            return;
        }

        delete obj.allPlans;
        delete obj.oldPlan;
        delete obj.stats;

        if (typeof(obj.length) === "number") {
            for (var i = 0; i < obj.length; i++) {
                removeVerboseFields(obj[i]);
            }
        }

        if (obj.shards) {
            for (var key in obj.shards) {
                removeVerboseFields(obj.shards[key]);
            }
        }

        if (obj.clauses) {
            removeVerboseFields(obj.clauses);
        }
    }

    /**
     * Many of the methods of an explain query just pass through to the underlying
     * non-explained DBQuery. Use this to generate a function which calls function 'name' on
     * 'destObj' and then returns this.
     */
    function createDelegationFunc(explainQuery, dbQuery, name) {
        return function() {
            dbQuery[name].apply(dbQuery, arguments);
            return explainQuery;
        };
    }

    /**
     * Where possible, the explain query will be sent to the server as an explain command.
     * However, if one of the nodes we are talking to (either a standalone or a shard in
     * a sharded cluster) is of a version that doesn't have the explain command, we will
     * use this function to fall back on the $explain query option.
     */
    function explainWithLegacyQueryOption(explainQuery) {
        // The wire protocol version indicates that the server does not have the explain
        // command. Add $explain to the query and send it to the server.
        var clone = explainQuery._query.clone();
        clone._addSpecial("$explain", true);
        var result = clone.next();

        // Remove some fields from the explain if verbosity is
        // just "queryPlanner".
        if ("queryPlanner" === explainQuery._verbosity) {
            removeVerboseFields(result);
        }

        return Explainable.throwOrReturn(result);
    }

    function constructor(query, verbosity) {
        //
        // Private vars.
        //

        this._query = query;
        this._verbosity = Explainable.parseVerbosity(verbosity);
        this._mongo = query._mongo;
        this._finished = false;

        // Used if this query is a count, not a find.
        this._isCount = false;
        this._applySkipLimit = false;

        //
        // Public delegation methods. These just pass through to the underlying
        // DBQuery.
        //

        var delegationFuncNames = [
            "addOption",
            "batchSize",
            "collation",
            "comment",
            "hint",
            "limit",
            "max",
            "maxTimeMS",
            "min",
            "readPref",
            "showDiskLoc",
            "skip",
            "snapshot",
            "sort",
        ];

        // Generate the delegation methods from the list of their names.
        var that = this;
        delegationFuncNames.forEach(function(name) {
            that[name] = createDelegationFunc(that, that._query, name);
        });

        //
        // Core public methods.
        //

        /**
         * Indicates that we are done building the query to explain, and sends the explain
         * command or query to the server.
         *
         * Returns the result of running the explain.
         */
        this.finish = function() {
            if (this._finished) {
                throw Error("query has already been explained");
            }

            // Mark this query as finished. Shouldn't be used for another explain.
            this._finished = true;

            // Explain always gets pretty printed.
            this._query._prettyShell = true;

            if (this._mongo.hasExplainCommand()) {
                // The wire protocol version indicates that the server has the explain command.
                // Convert this explain query into an explain command, and send the command to
                // the server.
                var innerCmd;
                if (this._isCount) {
                    // True means to always apply the skip and limit values.
                    innerCmd = this._query._convertToCountCmd(this._applySkipLimit);
                } else {
                    var canAttachReadPref = false;
                    innerCmd = this._query._convertToCommand(canAttachReadPref);
                }

                var explainCmd = {
                    explain: innerCmd
                };
                explainCmd["verbosity"] = this._verbosity;

                var explainDb = this._query._db;

                if ("$readPreference" in this._query._query) {
                    var prefObj = this._query._query.$readPreference;
                    explainCmd = explainDb._attachReadPreferenceToCommand(explainCmd, prefObj);
                }

                var explainResult =
                    explainDb.runReadCommand(explainCmd, null, this._query._options);

                if (!explainResult.ok && explainResult.code === ErrorCodes.CommandNotFound) {
                    // One of the shards doesn't have the explain command available. Retry using
                    // the legacy $explain format, which should be supported by all shards.
                    return explainWithLegacyQueryOption(this);
                }

                return Explainable.throwOrReturn(explainResult);
            } else {
                return explainWithLegacyQueryOption(this);
            }
        };

        this.next = function() {
            return this.finish();
        };

        this.hasNext = function() {
            return !this._finished;
        };

        this.forEach = function(func) {
            while (this.hasNext()) {
                func(this.next());
            }
        };

        /**
         * Returns the explain resulting from running this query as a count operation.
         *
         * If 'applySkipLimit' is true, then the skip and limit values set on this query values are
         * passed to the server; otherwise they are ignored.
         */
        this.count = function(applySkipLimit) {
            this._isCount = true;
            if (applySkipLimit) {
                this._applySkipLimit = true;
            }
            return this.finish();
        };

        /**
         * This gets called automatically by the shell in interactive mode. It should
         * print the result of running the explain.
         */
        this.shellPrint = function() {
            var result = this.finish();
            return tojson(result);
        };

        /**
         * Display help text.
         */
        this.help = function() {
            print("Explain query methods");
            print("\t.finish() - sends explain command to the server and returns the result");
            print("\t.forEach(func) - apply a function to the explain results");
            print("\t.hasNext() - whether this explain query still has a result to retrieve");
            print("\t.next() - alias for .finish()");
            print("Explain query modifiers");
            print("\t.addOption(n)");
            print("\t.batchSize(n)");
            print("\t.comment(comment)");
            print("\t.collation(collationSpec)");
            print("\t.count()");
            print("\t.hint(hintSpec)");
            print("\t.limit(n)");
            print("\t.maxTimeMS(n)");
            print("\t.max(idxDoc)");
            print("\t.min(idxDoc)");
            print("\t.readPref(mode, tagSet)");
            print("\t.showDiskLoc()");
            print("\t.skip(n)");
            print("\t.snapshot()");
            print("\t.sort(sortSpec)");
            return __magicNoPrint;
        };
    }

    return constructor;
})();



// ---- MODULE: mongo ---- 
// mongo.js

// NOTE 'Mongo' may be defined here or in MongoJS.cpp.  Add code to init, not to this constructor.
if (typeof Mongo == "undefined") {
    Mongo = function(host) {
        this.init(host);
    };
}

if (!Mongo.prototype) {
    throw Error("Mongo.prototype not defined");
}

if (!Mongo.prototype.find)
    Mongo.prototype.find = function(ns, query, fields, limit, skip, batchSize, options) {
        throw Error("find not implemented");
    };
if (!Mongo.prototype.insert)
    Mongo.prototype.insert = function(ns, obj) {
        throw Error("insert not implemented");
    };
if (!Mongo.prototype.remove)
    Mongo.prototype.remove = function(ns, pattern) {
        throw Error("remove not implemented");
    };
if (!Mongo.prototype.update)
    Mongo.prototype.update = function(ns, query, obj, upsert) {
        throw Error("update not implemented");
    };

if (typeof mongoInject == "function") {
    mongoInject(Mongo.prototype);
}

Mongo.prototype.setSlaveOk = function(value) {
    if (value == undefined)
        value = true;
    this.slaveOk = value;
};

Mongo.prototype.getSlaveOk = function() {
    return this.slaveOk || false;
};

Mongo.prototype.getDB = function(name) {
    if ((jsTest.options().keyFile) &&
        ((typeof this.authenticated == 'undefined') || !this.authenticated)) {
        jsTest.authenticate(this);
    }
    // There is a weird issue where typeof(db._name) !== "string" when the db name
    // is created from objects returned from native C++ methods.
    // This hack ensures that the db._name is always a string.
    if (typeof(name) === "object") {
        name = name.toString();
    }
    return new DB(this, name);
};

Mongo.prototype.getDBs = function() {
    var res = this.getDB("admin").runCommand({"listDatabases": 1});
    if (!res.ok)
        throw _getErrorWithCode(res, "listDatabases failed:" + tojson(res));
    return res;
};

Mongo.prototype.adminCommand = function(cmd) {
    return this.getDB("admin").runCommand(cmd);
};

/**
 * Returns all log components and current verbosity values
 */
Mongo.prototype.getLogComponents = function() {
    var res = this.adminCommand({getParameter: 1, logComponentVerbosity: 1});
    if (!res.ok)
        throw _getErrorWithCode(res, "getLogComponents failed:" + tojson(res));
    return res.logComponentVerbosity;
};

/**
 * Accepts optional second argument "component",
 * string of form "storage.journaling"
 */
Mongo.prototype.setLogLevel = function(logLevel, component) {
    componentNames = [];
    if (typeof component === "string") {
        componentNames = component.split(".");
    } else if (component !== undefined) {
        throw Error("setLogLevel component must be a string:" + tojson(component));
    }
    var vDoc = {
        verbosity: logLevel
    };

    // nest vDoc
    for (var key, obj; componentNames.length > 0;) {
        obj = {};
        key = componentNames.pop();
        obj[key] = vDoc;
        vDoc = obj;
    }
    var res = this.adminCommand({setParameter: 1, logComponentVerbosity: vDoc});
    if (!res.ok)
        throw _getErrorWithCode(res, "setLogLevel failed:" + tojson(res));
    return res;
};

Mongo.prototype.getDBNames = function() {
    return this.getDBs().databases.map(function(z) {
        return z.name;
    });
};

Mongo.prototype.getCollection = function(ns) {
    var idx = ns.indexOf(".");
    if (idx < 0)
        throw Error("need . in ns");
    var db = ns.substring(0, idx);
    var c = ns.substring(idx + 1);
    return this.getDB(db).getCollection(c);
};

Mongo.prototype.toString = function() {
    return "connection to " + this.host;
};
Mongo.prototype.tojson = Mongo.prototype.toString;

/**
 * Sets the read preference.
 *
 * @param mode {string} read preference mode to use. Pass null to disable read
 *     preference.
 * @param tagSet {Array.<Object>} optional. The list of tags to use, order matters.
 *     Note that this object only keeps a shallow copy of this array.
 */
Mongo.prototype.setReadPref = function(mode, tagSet) {
    if ((this._readPrefMode === "primary") && (typeof(tagSet) !== "undefined") &&
        (Object.keys(tagSet).length > 0)) {
        // we allow empty arrays/objects or no tagSet for compatibility reasons
        throw Error("Can not supply tagSet with readPref mode primary");
    }
    this._setReadPrefUnsafe(mode, tagSet);
};

// Set readPref without validating. Exposed so we can test the server's readPref validation.
Mongo.prototype._setReadPrefUnsafe = function(mode, tagSet) {
    this._readPrefMode = mode;
    this._readPrefTagSet = tagSet;
};

Mongo.prototype.getReadPrefMode = function() {
    return this._readPrefMode;
};

Mongo.prototype.getReadPrefTagSet = function() {
    return this._readPrefTagSet;
};

// Returns a readPreference object of the type expected by mongos.
Mongo.prototype.getReadPref = function() {
    var obj = {}, mode, tagSet;
    if (typeof(mode = this.getReadPrefMode()) === "string") {
        obj.mode = mode;
    } else {
        return null;
    }
    // Server Selection Spec: - if readPref mode is "primary" then the tags field MUST
    // be absent. Ensured by setReadPref.
    if (Array.isArray(tagSet = this.getReadPrefTagSet())) {
        obj.tags = tagSet;
    }

    return obj;
};

connect = function(url, user, pass) {
    if (user && !pass)
        throw Error("you specified a user and not a password.  " +
                    "either you need a password, or you're using the old connect api");

    // Validate connection string "url" as "hostName:portNumber/databaseName"
    //                                  or "hostName/databaseName"
    //                                  or "databaseName"
    // hostName may be an IPv6 address (with colons), in which case ":portNumber" is required
    //
    var urlType = typeof url;
    if (urlType == "undefined") {
        throw Error("Missing connection string");
    }
    if (urlType != "string") {
        throw Error("Incorrect type \"" + urlType + "\" for connection string \"" + tojson(url) +
                    "\"");
    }
    url = url.trim();
    if (0 == url.length) {
        throw Error("Empty connection string");
    }
    if (!url.startsWith("mongodb://")) {
        var colon = url.lastIndexOf(":");
        var slash = url.lastIndexOf("/");
        if (0 == colon || 0 == slash) {
            throw Error("Missing host name in connection string \"" + url + "\"");
        }
        if (colon == slash - 1 || colon == url.length - 1) {
            throw Error("Missing port number in connection string \"" + url + "\"");
        }
        if (colon != -1 && colon < slash) {
            var portNumber = url.substring(colon + 1, slash);
            if (portNumber.length > 5 || !/^\d*$/.test(portNumber) ||
                parseInt(portNumber) > 65535) {
                throw Error("Invalid port number \"" + portNumber + "\" in connection string \"" +
                            url + "\"");
            }
        }
        if (slash == url.length - 1) {
            throw Error("Missing database name in connection string \"" + url + "\"");
        }
    }

    chatty("connecting to: " + url);
    var db;
    if (url.startsWith("mongodb://")) {
        db = new Mongo(url);
        if (db.defaultDB.length == 0) {
            throw Error("Missing database name in connection string \"" + url + "\"");
        }
        db = db.getDB(db.defaultDB);
    } else if (slash == -1)
        db = new Mongo().getDB(url);
    else
        db = new Mongo(url.substring(0, slash)).getDB(url.substring(slash + 1));

    if (user && pass) {
        if (!db.auth(user, pass)) {
            throw Error("couldn't login");
        }
    }
    return db;
};

/** deprecated, use writeMode below
 *
 */
Mongo.prototype.useWriteCommands = function() {
    return (this.writeMode() != "legacy");
};

Mongo.prototype.forceWriteMode = function(mode) {
    this._writeMode = mode;
};

Mongo.prototype.hasWriteCommands = function() {
    var hasWriteCommands = (this.getMinWireVersion() <= 2 && 2 <= this.getMaxWireVersion());
    return hasWriteCommands;
};

Mongo.prototype.hasExplainCommand = function() {
    var hasExplain = (this.getMinWireVersion() <= 3 && 3 <= this.getMaxWireVersion());
    return hasExplain;
};

/**
 * {String} Returns the current mode set. Will be commands/legacy/compatibility
 *
 * Sends isMaster to determine if the connection is capable of using bulk write operations, and
 * caches the result.
 */

Mongo.prototype.writeMode = function() {

    if ('_writeMode' in this) {
        return this._writeMode;
    }

    // get default from shell params
    if (_writeMode)
        this._writeMode = _writeMode();

    // can't use "commands" mode unless server version is good.
    if (this.hasWriteCommands()) {
        // good with whatever is already set
    } else if (this._writeMode == "commands") {
        print("Cannot use commands write mode, degrading to compatibility mode");
        this._writeMode = "compatibility";
    }

    return this._writeMode;
};

/**
 * Returns true if the shell is configured to use find/getMore commands rather than the C++ client.
 *
 * Currently, the C++ client will always use OP_QUERY find and OP_GET_MORE.
 */
Mongo.prototype.useReadCommands = function() {
    return (this.readMode() === "commands");
};

/**
 * For testing, forces the shell to use the readMode specified in 'mode'. Must be either "commands"
 * (use the find/getMore commands), "legacy" (use legacy OP_QUERY/OP_GET_MORE wire protocol reads),
 * or "compatibility" (auto-detect mode based on wire version).
 */
Mongo.prototype.forceReadMode = function(mode) {
    if (mode !== "commands" && mode !== "compatibility" && mode !== "legacy") {
        throw new Error("Mode must be one of {commands, compatibility, legacy}, but got: " + mode);
    }

    this._readMode = mode;
};

/**
 * Get the readMode string (either "commands" for find/getMore commands, "legacy" for OP_QUERY find
 * and OP_GET_MORE, or "compatibility" for detecting based on wire version).
 */
Mongo.prototype.readMode = function() {
    // Get the readMode from the shell params if we don't have one yet.
    if (typeof _readMode === "function" && !this.hasOwnProperty("_readMode")) {
        this._readMode = _readMode();
    }

    if (this.hasOwnProperty("_readMode") && this._readMode !== "compatibility") {
        // We already have determined our read mode. Just return it.
        return this._readMode;
    } else {
        // We're in compatibility mode. Determine whether the server supports the find/getMore
        // commands. If it does, use commands mode. If not, degrade to legacy mode.
        try {
            var hasReadCommands = (this.getMinWireVersion() <= 4 && 4 <= this.getMaxWireVersion());
            if (hasReadCommands) {
                this._readMode = "commands";
            } else {
                print("Cannot use 'commands' readMode, degrading to 'legacy' mode");
                this._readMode = "legacy";
            }
        } catch (e) {
            // We failed trying to determine whether the remote node supports the find/getMore
            // commands. In this case, we keep _readMode as "compatibility" and the shell should
            // issue legacy reads. Next time around we will issue another isMaster to try to
            // determine the readMode decisively.
        }
    }

    return this._readMode;
};

//
// Write Concern can be set at the connection level, and is used for all write operations unless
// overridden at the collection level.
//

Mongo.prototype.setWriteConcern = function(wc) {
    if (wc instanceof WriteConcern) {
        this._writeConcern = wc;
    } else {
        this._writeConcern = new WriteConcern(wc);
    }
};

Mongo.prototype.getWriteConcern = function() {
    return this._writeConcern;
};

Mongo.prototype.unsetWriteConcern = function() {
    delete this._writeConcern;
};



// ---- MODULE: port_mongo ---- 
Mongo.prototype.init = function(host) {
	this.host = "127.0.0.1";

	if(typeof host !== "undefined")
		this.host = host;

	this.slaveOk = false;
	this.defaultDB = host.split("/").splice(1).join("/")

	//this._writeMode = "compatability"; //Disable sending update, insert and remove via command
};

Mongo.prototype.getMinWireVersion = function(){
	return 0;
}

Mongo.prototype.getMaxWireVersion = function(){
	return 10;
}

Mongo.prototype.auth = function(){
	throw Error("Auth not implemented. Pass the credentials to simple_connect for implicit authentication");
    // auto conn = getConnection(args);
    // if (!conn)
    //     uasserted(ErrorCodes::BadValue, "no connection");

    // BSONObj params;
    // switch (args.length()) {
    //     case 1:
    //         params = ValueWriter(cx, args.get(0)).toBSON();
    //         break;
    //     case 3:
    //         params =
    //             BSON(saslCommandMechanismFieldName << "MONGODB-CR" << saslCommandUserDBFieldName
    //                                                << ValueWriter(cx, args[0]).toString()
    //                                                << saslCommandUserFieldName
    //                                                << ValueWriter(cx, args[1]).toString()
    //                                                << saslCommandPasswordFieldName
    //                                                << ValueWriter(cx, args[2]).toString());
    //         break;
    //     default:
    //         uasserted(ErrorCodes::BadValue, "mongoAuth takes 1 object or 3 string arguments");
    // }

    // conn->auth(params);

    // args.rval().setBoolean(true);
// }
}

Mongo.prototype.runCommand = function(database, cmdObj, options){
	console.log("====RUNCOMMAND")
	assert(typeof database === "string", "the database parameter to runCommand must be a string");
	assert(typeof cmdObj === "object", "the cmdObj parameter to runCommand must be an object");
	assert(typeof options === "number", "the options parameter to runCommand must be a number");

	var stringifyFunctions = function (key, val){ if(typeof val === "function") return val.toString(); return val};

	var toSend = {database: database,
					command: JSON.stringify(cmdObj, stringifyFunctions),
					options: options,
					connection: this.getConnectionData()};

	var result = jsObjectToJSObjectWithBsonValues(Connection.runCommand(toSend, this));

	if(result === null)
		throw new Error("An error occured when running the command");

	return result;

	console.log("Normally would send to server. For development returning a listCollections result");
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

	var cursor = new Cursor(ns, cursorId, 0, 0, this.getConnectionData());

	if(typeof batchSize !== "undefined")
		cursor.setBatchSize(batchSize);

	return cursor;
}

Mongo.prototype.find = function(ns, query, fields, nToReturn, nToSkip, batchSize, options) {
   	assert(arguments.length === 7, "find needs 7 args");
   	assert(typeof arguments[1] === "object", "needs to be an object");

	var cursor = new Cursor(ns, query, nToReturn, nToSkip, fields, options, batchSize, this.getConnectionData());
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

function simple_connect(hostname, port, database, user, password, authDatabase, authMethod, performAuth, hiddenPassword){
	if(arguments.length < 3)
		throw Error("Hostname, port and database are required");

	if(typeof hostname !== "string")
		throw Error("Hostname must be a string");

	if(typeof port !== "number")
		throw Error("Port must be a number");

	if(typeof database !== "string")
		throw Error("Database must be a string")

	var mongo = new Mongo(hostname+":"+port+"/"+database);


	if(user && !password)
		throw Error("You specified a user, but no password");

	function isDefined(val, defVal){
		return typeof val === "undefined" ? defVal : val;
	}

	mongo.connectionData = {
		hostname: hostname,
		port: port,
		performAuth: performAuth ? true : false, //"cast" to boolean
		auth: {
			user: isDefined(user, null),
			password: !hiddenPassword ? isDefined(password, null) : null,
			connectionId: hiddenPassword ? isDefined(password, null) : null,

			authDatabase: isDefined(authDatabase, null),
			authMechanism: isDefined(authMethod, null),
		}
	}


	return mongo.getDB(database);
}


Mongo.prototype.getConnectionData = function(){
	if(typeof this.connectionData === "undefined")
		throw Error("Was not connected using simple_connect");

	return this.connectionData;
}


// ---- MODULE: port_rpc ---- 
"use strict";

var rpc = (function(){

	var ServerSelectionMetadata = (function(){
		/*
		 * WARNING: In CPP most of these return a status code. We don't and fail silently :(
		 */

		// Symbolic constant for the "$secondaryOk" metadata field. This field should be of boolean or
		// numeric type, and is treated as a boolean.
		var kSecondaryOkFieldName = "$secondaryOk";

		// Symbolic constant for the "$readPreference" metadata field. The field should be of Object type
		// when present.
		var kReadPreferenceFieldName = "$readPreference";

		var kQueryOptionsFieldName = "$queryOptions";

		var kDollarQueryWrapper = "$query";
		var kQueryWrapper = "query";


		/* add all the fields from the object specified to this object */
		function appendElements(appendTo, appendFrom){
			for(var i=0; i<Object.keys(appendFrom).length; i++){
				appendTo[Object.keys(appendFrom)[i]] = appendFrom[Object.keys(appendFrom)[i]];
			}
		}

		/**
		 * Reads a top-level $readPreference field from a wrapped command.
		 */
		function extractWrappedReadPreference(/*const BSONObj& */ wrappedCommand, /*BSONObjBuilder* */ metadataBob) {
		    if(typeof wrappedCommand[kReadPreferenceFieldName] !== "undefined")
		    	metadataBob[kReadPreferenceFieldName] = wrappedCommand[kReadPreferenceFieldName];
		}

		/**
		 * Reads a $readPreference from a $queryOptions subobject, if it exists, and writes it to
		 * metadataBob. Writes out the original command excluding the $queryOptions subobject.
		 */
		function extractUnwrappedReadPreference(/*const BSONObj& */ unwrappedCommand,
		                                      /*BSONObjBuilder* */ commandBob,
		                                      /*BSONObjBuilder* */ metadataBob) {
		    var queryOptionsEl = unwrappedCommand[kQueryOptionsFieldName];
		    var readPrefEl = {};

		    if(typeof queryOptionsEl  === "undefined"){
		    	appendElements(commandBob, unwrappedCommand);
		    	return;
		    }

		    // Write out the command excluding the $queryOptions field.
			for(var i=0; i<Object.keys(appendFrom).length; i++){
		        if (key != kQueryOptionsFieldName){
					commandBob[Object.keys(appendFrom)[i]] = unwrappedCommand[Object.keys(appendFrom)[i]];
				}
			}

			throw new Error("This has never been tested. Sorry. Uncomment to test.");
			readPrefEl = queryOptionsEl[kReadPreferenceFieldName];

		    // If there is a $queryOptions field, we expect there to be a $readPreference.
			if(typeof readPrefEl === "undefined"){
				throw Error("We expected a $readPreference and got none");
			}

			appendElements(metaDataBob, readPrefEl);
		}


	    function fieldName() {
	        return "$ssm";
	    }

	    /**
		 * Utility to unwrap a '$query' or 'query' wrapped command object. The first element of the
		 * returned tuple indicates whether the command was unwrapped, and the second element is either
		 * the unwrapped command (if it was wrapped), or the original command if it was not.
		 */
		/*StatusWith<std::tuple<bool, BSONObj>>*/
		function unwrapCommand(/*const BSONObj& */ maybeWrapped) {
		    var firstElFieldName = Object.keys(maybeWrapped)[0];

		    if ((firstElFieldName != kDollarQueryWrapper) &&
		        (firstElFieldName != kQueryWrapper)) {
		        return [false, maybeWrapped];
		    }

		    return [true, maybeWrapped[firstElFieldName]];
		}

		function upconvert(/*const BSONObj&*/ legacyCommand,
		                      /*const int*/ legacyQueryFlags,
		                      /*BSONObjBuilder**/ commandBob,
		                      /*BSONObjBuilder**/ metadataBob) {
			var ssmBob = {};

			// The secondaryOK option is equivalent to the slaveOk bit being set on legacy commands.
			if(legacyQueryFlags & QueryOptions.QueryOption_SlaveOk){
				ssmBob[kSecondaryOkFieldName] = 1;
			}

		    // First we need to check if we have a wrapped command. That is, a command of the form
		    // {'$query': { 'commandName': 1, ...}, '$someOption': 5, ....}. Curiously, the field name
		    // of the wrapped query can be either '$query', or 'query'.
		    var swUnwrapped = unwrapCommand(legacyCommand);

		    var wasWrapped = swUnwrapped[0];
		    var maybeUnwrapped = swUnwrapped[1];

		    if(wasWrapped){
		        // Check if legacyCommand has an invalid $maxTimeMS option.
		    	if(typeof legacyCommand["$maxTimeMS"] !== "undefined"){
		    		throw new Error("cannot use $maxTimeMS query option with " +
		                          "commands; use maxTimeMS command option " +
		                          "instead")
		    	}

		        // If the command was wrapped, we can write out the upconverted command now, as there
		        // is nothing else we need to remove from it.
		        appendElements(ssmBob, maybeUnwrapped);

		        extractWrappedReadPreference(legacyCommand, ssmBob);
		    } else {
		        // If the command was not wrapped, we need to check for a readPreference sent by mongos
		        // on the $queryOptions field of the command. If it is set, we remove it from the
		        // upconverted command, so we need to pass the command builder along.

		        extractUnwrappedReadPreference(maybeUnwrapped, commandBob, ssmBob);
		    }

		    if(!jQuery.isEmptyObject(ssmBob)){
		    	metadataBob[fieldName()] = ssmBob;
		    }
		}

		return {"upconvert": upconvert};
	})();

	var AuditMetadata = (function(){
		/* add all the fields from the object specified to this object */
		function appendElements(appendTo, appendFrom){
			for(var i=0; i<Object.keys(appendFrom).length; i++){
				appendTo[Object.keys(appendFrom)[i]] = appendFrom[Object.keys(appendFrom)[i]];
			}
		}

		function upconvert(/*const BSONObj &*/ command,
                            /*const int */ _,
                            /*BSONObjBuilder* */ commandBob,
                            /*BSONObjBuilder* */ __) {
			appendElements(commandBob, command);
		}

		return {"upconvert": upconvert};
	})();


	function upconvertRequestMetadata(/*BSONObj*/ legacyCmdObj, /*int*/ queryFlags){
		var metaDataBob = {};
		var ssmCommandBob = {};
		var auditCommandBob = {};


	    // Ordering is important here - ServerSelectionMetadata must be upconverted
	    // first, then AuditMetadata.
		ServerSelectionMetadata.upconvert(legacyCmdObj, queryFlags, ssmCommandBob, metaDataBob);
        AuditMetadata.upconvert(ssmCommandBob, queryFlags, auditCommandBob, metaDataBob);

        return [auditCommandBob, metaDataBob];
	}

	function makeReply(){
		throw new Error("not implemented");
	}

	var rpc_obj = {"upconvertRequestMetadata": upconvertRequestMetadata, makeReply: makeReply};

	if(window.__unittesting__){
		rpc_obj.__unittesting__ = {AuditMetadata: AuditMetadata, ServerSelectionMetadata: ServerSelectionMetadata}
	}

	return rpc_obj;
})();




// ---- MODULE: mr ---- 
// mr.js

MR = {};

MR.init = function() {
    $max = 0;
    $arr = [];
    emit = MR.emit;
    $numEmits = 0;
    $numReduces = 0;
    $numReducesToDB = 0;
    gc();  // this is just so that keep memory size sane
};

MR.cleanup = function() {
    MR.init();
    gc();
};

MR.emit = function(k, v) {
    $numEmits++;
    var num = nativeHelper.apply(get_num_, [k]);
    var data = $arr[num];
    if (!data) {
        data = {
            key: k,
            values: new Array(1000),
            count: 0
        };
        $arr[num] = data;
    }
    data.values[data.count++] = v;
    $max = Math.max($max, data.count);
};

MR.doReduce = function(useDB) {
    $numReduces++;
    if (useDB)
        $numReducesToDB++;
    $max = 0;
    for (var i = 0; i < $arr.length; i++) {
        var data = $arr[i];
        if (!data)
            continue;

        if (useDB) {
            var x = tempcoll.findOne({_id: data.key});
            if (x) {
                data.values[data.count++] = x.value;
            }
        }

        var r = $reduce(data.key, data.values.slice(0, data.count));
        if (r && r.length && r[0]) {
            data.values = r;
            data.count = r.length;
        } else {
            data.values[0] = r;
            data.count = 1;
        }

        $max = Math.max($max, data.count);

        if (useDB) {
            if (data.count == 1) {
                tempcoll.save({_id: data.key, value: data.values[0]});
            } else {
                tempcoll.save({_id: data.key, value: data.values.slice(0, data.count)});
            }
        }
    }
};

MR.check = function() {
    if ($max < 2000 && $arr.length < 1000) {
        return 0;
    }
    MR.doReduce();
    if ($max < 2000 && $arr.length < 1000) {
        return 1;
    }
    MR.doReduce(true);
    $arr = [];
    $max = 0;
    reset_num();
    gc();
    return 2;
};

MR.finalize = function() {
    tempcoll.find().forEach(function(z) {
        z.value = $finalize(z._id, z.value);
        tempcoll.save(z);
    });
};



// ---- MODULE: query ---- 
// query.js

if (typeof DBQuery == "undefined") {
    DBQuery = function(mongo, db, collection, ns, query, fields, limit, skip, batchSize, options) {

        this._mongo = mongo;            // 0
        this._db = db;                  // 1
        this._collection = collection;  // 2
        this._ns = ns;                  // 3

        this._query = query || {};  // 4
        this._fields = fields;      // 5
        this._limit = limit || 0;   // 6
        this._skip = skip || 0;     // 7
        this._batchSize = batchSize || 0;
        this._options = options || 0;

        this._cursor = null;
        this._numReturned = 0;
        this._special = false;
        this._prettyShell = false;
    };
    print("DBQuery probably won't have array access ");
}

DBQuery.prototype.help = function() {
    print("find(<predicate>, <projection>) modifiers");
    print("\t.sort({...})");
    print("\t.limit(<n>)");
    print("\t.skip(<n>)");
    print("\t.batchSize(<n>) - sets the number of docs to return per getMore");
    print("\t.collation({...})");
    print("\t.hint({...})");
    print("\t.readConcern(<level>)");
    print("\t.readPref(<mode>, <tagset>)");
    print(
        "\t.count(<applySkipLimit>) - total # of objects matching query. by default ignores skip,limit");
    print("\t.size() - total # of objects cursor would return, honors skip,limit");
    print(
        "\t.explain(<verbosity>) - accepted verbosities are {'queryPlanner', 'executionStats', 'allPlansExecution'}");
    print("\t.min({...})");
    print("\t.max({...})");
    print("\t.maxScan(<n>)");
    print("\t.maxTimeMS(<n>)");
    print("\t.comment(<comment>)");
    print("\t.snapshot()");
    print("\t.tailable(<isAwaitData>)");
    print("\t.noCursorTimeout()");
    print("\t.allowPartialResults()");
    print("\t.returnKey()");
    print("\t.showRecordId() - adds a $recordId field to each returned object");

    print("\nCursor methods");
    print("\t.toArray() - iterates through docs and returns an array of the results");
    print("\t.forEach(<func>)");
    print("\t.map(<func>)");
    print("\t.hasNext()");
    print("\t.next()");
    print("\t.close()");
    print(
        "\t.objsLeftInBatch() - returns count of docs left in current batch (when exhausted, a new getMore will be issued)");
    print("\t.itcount() - iterates through documents and counts them");
    print(
        "\t.getQueryPlan() - get query plans associated with shape. To get more info on query plans, " +
        "call getQueryPlan().help().");
    print("\t.pretty() - pretty print each document, possibly over multiple lines");
};

DBQuery.prototype.clone = function() {
    var q = new DBQuery(this._mongo,
                        this._db,
                        this._collection,
                        this._ns,
                        this._query,
                        this._fields,
                        this._limit,
                        this._skip,
                        this._batchSize,
                        this._options);
    q._special = this._special;
    return q;
};

DBQuery.prototype._ensureSpecial = function() {
    if (this._special)
        return;

    var n = {
        query: this._query
    };
    this._query = n;
    this._special = true;
};

DBQuery.prototype._checkModify = function() {
    if (this._cursor)
        throw Error("query already executed");
};

DBQuery.prototype._canUseFindCommand = function() {
    // Since runCommand() is implemented by running a findOne() against the $cmd collection, we have
    // to make sure that we don't try to run a find command against the $cmd collection.
    //
    // We also forbid queries with the exhaust option from running as find commands, because the
    // find command does not support exhaust.
    return (this._collection.getName().indexOf("$cmd") !== 0) &&
        (this._options & DBQuery.Option.exhaust) === 0;
};

DBQuery.prototype._exec = function() {
    if (!this._cursor) {
        assert.eq(0, this._numReturned);
        this._cursorSeen = 0;

        if (this._mongo.useReadCommands() && this._canUseFindCommand()) {
            var canAttachReadPref = true;
            var findCmd = this._convertToCommand(canAttachReadPref);
            var cmdRes = this._db.runReadCommand(findCmd, null, this._options);
            this._cursor = new DBCommandCursor(this._mongo, cmdRes, this._batchSize);
        } else {
            if (this._special && this._query.readConcern) {
                throw new Error("readConcern requires use of read commands");
            }

            if (this._special && this._query.collation) {
                throw new Error("collation requires use of read commands");
            }

            this._cursor = this._mongo.find(this._ns,
                                            this._query,
                                            this._fields,
                                            this._limit,
                                            this._skip,
                                            this._batchSize,
                                            this._options);
        }
    }
    return this._cursor;
};

/**
 * Internal helper used to convert this cursor into the format required by the find command.
 *
 * If canAttachReadPref is true, may attach a read preference to the resulting command using the
 * "wrapped form": { $query: { <cmd>: ... }, $readPreference: { ... } }.
 */
DBQuery.prototype._convertToCommand = function(canAttachReadPref) {
    var cmd = {};

    cmd["find"] = this._collection.getName();

    if (this._special) {
        if (this._query.query) {
            cmd["filter"] = this._query.query;
        }
    } else if (this._query) {
        cmd["filter"] = this._query;
    }

    if (this._skip) {
        cmd["skip"] = this._skip;
    }

    if (this._batchSize) {
        if (this._batchSize < 0) {
            cmd["batchSize"] = -this._batchSize;
            cmd["singleBatch"] = true;
        } else {
            cmd["batchSize"] = this._batchSize;
        }
    }

    if (this._limit) {
        if (this._limit < 0) {
            cmd["limit"] = -this._limit;
            cmd["singleBatch"] = true;
        } else {
            cmd["limit"] = this._limit;
            cmd["singleBatch"] = false;
        }
    }

    if ("orderby" in this._query) {
        cmd["sort"] = this._query.orderby;
    }

    if (this._fields) {
        cmd["projection"] = this._fields;
    }

    if ("$hint" in this._query) {
        cmd["hint"] = this._query.$hint;
    }

    if ("$comment" in this._query) {
        cmd["comment"] = this._query.$comment;
    }

    if ("$maxScan" in this._query) {
        cmd["maxScan"] = this._query.$maxScan;
    }

    if ("$maxTimeMS" in this._query) {
        cmd["maxTimeMS"] = this._query.$maxTimeMS;
    }

    if ("$max" in this._query) {
        cmd["max"] = this._query.$max;
    }

    if ("$min" in this._query) {
        cmd["min"] = this._query.$min;
    }

    if ("$returnKey" in this._query) {
        cmd["returnKey"] = this._query.$returnKey;
    }

    if ("$showDiskLoc" in this._query) {
        cmd["showRecordId"] = this._query.$showDiskLoc;
    }

    if ("$snapshot" in this._query) {
        cmd["snapshot"] = this._query.$snapshot;
    }

    if ("readConcern" in this._query) {
        cmd["readConcern"] = this._query.readConcern;
    }

    if ("collation" in this._query) {
        cmd["collation"] = this._query.collation;
    }

    if ((this._options & DBQuery.Option.tailable) != 0) {
        cmd["tailable"] = true;
    }

    if ((this._options & DBQuery.Option.oplogReplay) != 0) {
        cmd["oplogReplay"] = true;
    }

    if ((this._options & DBQuery.Option.noTimeout) != 0) {
        cmd["noCursorTimeout"] = true;
    }

    if ((this._options & DBQuery.Option.awaitData) != 0) {
        cmd["awaitData"] = true;
    }

    if ((this._options & DBQuery.Option.partial) != 0) {
        cmd["allowPartialResults"] = true;
    }

    if (canAttachReadPref) {
        // If there is a readPreference, use the wrapped command form.
        if ("$readPreference" in this._query) {
            var prefObj = this._query.$readPreference;
            cmd = this._db._attachReadPreferenceToCommand(cmd, prefObj);
        }
    }

    return cmd;
};

DBQuery.prototype.limit = function(limit) {
    this._checkModify();
    this._limit = limit;
    return this;
};

DBQuery.prototype.batchSize = function(batchSize) {
    this._checkModify();
    this._batchSize = batchSize;
    return this;
};

DBQuery.prototype.addOption = function(option) {
    this._options |= option;
    return this;
};

DBQuery.prototype.skip = function(skip) {
    this._checkModify();
    this._skip = skip;
    return this;
};

DBQuery.prototype.hasNext = function() {
    this._exec();

    if (this._limit > 0 && this._cursorSeen >= this._limit) {
        this._cursor.close();
        return false;
    }
    var o = this._cursor.hasNext();
    return o;
};

DBQuery.prototype.next = function() {
    this._exec();

    var o = this._cursor.hasNext();
    if (o)
        this._cursorSeen++;
    else
        throw Error("error hasNext: " + o);

    var ret = this._cursor.next();
    if (ret.$err) {
        throw _getErrorWithCode(ret, "error: " + tojson(ret));
    }

    this._numReturned++;
    return ret;
};

DBQuery.prototype.objsLeftInBatch = function() {
    this._exec();

    var ret = this._cursor.objsLeftInBatch();
    if (ret.$err)
        throw _getErrorWithCode(ret, "error: " + tojson(ret));

    return ret;
};

DBQuery.prototype.readOnly = function() {
    this._exec();
    this._cursor.readOnly();
    return this;
};

DBQuery.prototype.toArray = function() {
    if (this._arr)
        return this._arr;

    var a = [];
    while (this.hasNext())
        a.push(this.next());
    this._arr = a;
    return a;
};

DBQuery.prototype._convertToCountCmd = function(applySkipLimit) {
    var cmd = {
        count: this._collection.getName()
    };

    if (this._query) {
        if (this._special) {
            cmd.query = this._query.query;
            if (this._query.$maxTimeMS) {
                cmd.maxTimeMS = this._query.$maxTimeMS;
            }
            if (this._query.$hint) {
                cmd.hint = this._query.$hint;
            }
            if (this._query.readConcern) {
                cmd.readConcern = this._query.readConcern;
            }
            if (this._query.collation) {
                cmd.collation = this._query.collation;
            }
        } else {
            cmd.query = this._query;
        }
    }
    cmd.fields = this._fields || {};

    if (applySkipLimit) {
        if (this._limit)
            cmd.limit = this._limit;
        if (this._skip)
            cmd.skip = this._skip;
    }

    return cmd;
};

DBQuery.prototype.count = function(applySkipLimit) {
    var cmd = this._convertToCountCmd(applySkipLimit);

    var res = this._db.runReadCommand(cmd);
    if (res && res.n != null)
        return res.n;
    throw _getErrorWithCode(res, "count failed: " + tojson(res));
};

DBQuery.prototype.size = function() {
    return this.count(true);
};

DBQuery.prototype.countReturn = function() {
    var c = this.count();

    if (this._skip)
        c = c - this._skip;

    if (this._limit > 0 && this._limit < c)
        return this._limit;

    return c;
};

/**
* iterative count - only for testing
*/
DBQuery.prototype.itcount = function() {
    var num = 0;

    // Track how many bytes we've used this cursor to iterate iterated.  This function can be called
    // with some very large cursors.  SpiderMonkey appears happy to allow these objects to
    // accumulate, so regular gc() avoids an overly large memory footprint.
    //
    // TODO: migrate this function into c++
    var bytesSinceGC = 0;

    while (this.hasNext()) {
        num++;
        var nextDoc = this.next();
        bytesSinceGC += Object.bsonsize(nextDoc);

        // Garbage collect every 10 MB.
        if (bytesSinceGC > (10 * 1024 * 1024)) {
            bytesSinceGC = 0;
            gc();
        }
    }
    return num;
};

DBQuery.prototype.length = function() {
    return this.toArray().length;
};

DBQuery.prototype._addSpecial = function(name, value) {
    this._ensureSpecial();
    this._query[name] = value;
    return this;
};

DBQuery.prototype.sort = function(sortBy) {
    return this._addSpecial("orderby", sortBy);
};

DBQuery.prototype.hint = function(hint) {
    return this._addSpecial("$hint", hint);
};

DBQuery.prototype.min = function(min) {
    return this._addSpecial("$min", min);
};

DBQuery.prototype.max = function(max) {
    return this._addSpecial("$max", max);
};

/**
 * Deprecated. Use showRecordId().
 */
DBQuery.prototype.showDiskLoc = function() {
    return this.showRecordId();
};

DBQuery.prototype.showRecordId = function() {
    return this._addSpecial("$showDiskLoc", true);
};

DBQuery.prototype.maxTimeMS = function(maxTimeMS) {
    return this._addSpecial("$maxTimeMS", maxTimeMS);
};

DBQuery.prototype.readConcern = function(level) {
    var readConcernObj = {
        level: level
    };

    return this._addSpecial("readConcern", readConcernObj);
};

DBQuery.prototype.collation = function(collationSpec) {
    return this._addSpecial("collation", collationSpec);
};

/**
 * Sets the read preference for this cursor.
 *
 * @param mode {string} read preference mode to use.
 * @param tagSet {Array.<Object>} optional. The list of tags to use, order matters.
 *     Note that this object only keeps a shallow copy of this array.
 *
 * @return this cursor
 */
DBQuery.prototype.readPref = function(mode, tagSet) {
    var readPrefObj = {
        mode: mode
    };

    if (tagSet) {
        readPrefObj.tags = tagSet;
    }

    return this._addSpecial("$readPreference", readPrefObj);
};

DBQuery.prototype.forEach = function(func) {
    while (this.hasNext())
        func(this.next());
};

DBQuery.prototype.map = function(func) {
    var a = [];
    while (this.hasNext())
        a.push(func(this.next()));
    return a;
};

DBQuery.prototype.arrayAccess = function(idx) {
    return this.toArray()[idx];
};

DBQuery.prototype.comment = function(comment) {
    return this._addSpecial("$comment", comment);
};

DBQuery.prototype.explain = function(verbose) {
    var explainQuery = new DBExplainQuery(this, verbose);
    return explainQuery.finish();
};

DBQuery.prototype.snapshot = function() {
    return this._addSpecial("$snapshot", true);
};

DBQuery.prototype.returnKey = function() {
    return this._addSpecial("$returnKey", true);
};

DBQuery.prototype.maxScan = function(n) {
    return this._addSpecial("$maxScan", n);
};

DBQuery.prototype.pretty = function() {
    this._prettyShell = true;
    return this;
};

DBQuery.prototype.shellPrint = function() {
    try {
        var start = new Date().getTime();
        var n = 0;
        while (this.hasNext() && n < DBQuery.shellBatchSize) {
            var s = this._prettyShell ? tojson(this.next()) : tojson(this.next(), "", true);
            print(s);
            n++;
        }
        if (typeof _verboseShell !== 'undefined' && _verboseShell) {
            var time = new Date().getTime() - start;
            print("Fetched " + n + " record(s) in " + time + "ms");
        }
        if (this.hasNext()) {
            print("Type \"it\" for more");
            ___it___ = this;
        } else {
            ___it___ = null;
        }
    } catch (e) {
        print(e);
    }

};

/**
 * Returns a QueryPlan for the query.
 */
DBQuery.prototype.getQueryPlan = function() {
    return new QueryPlan(this);
};

DBQuery.prototype.toString = function() {
    return "DBQuery: " + this._ns + " -> " + tojson(this._query);
};

//
// CRUD specification find cursor extension
//

/**
* Get partial results from a mongos if some shards are down (instead of throwing an error).
*
* @method
* @see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query
* @return {DBQuery}
*/
DBQuery.prototype.allowPartialResults = function() {
    this._checkModify();
    this.addOption(DBQuery.Option.partial);
    return this;
};

/**
* The server normally times out idle cursors after an inactivity period (10 minutes)
* to prevent excess memory use. Set this option to prevent that.
*
* @method
* @see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query
* @return {DBQuery}
*/
DBQuery.prototype.noCursorTimeout = function() {
    this._checkModify();
    this.addOption(DBQuery.Option.noTimeout);
    return this;
};

/**
* Internal replication use only - driver should not set
*
* @method
* @see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query
* @return {DBQuery}
*/
DBQuery.prototype.oplogReplay = function() {
    this._checkModify();
    this.addOption(DBQuery.Option.oplogReplay);
    return this;
};

/**
* Limits the fields to return for all matching documents.
*
* @method
* @see http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/
* @param {object} document Document specifying the projection of the resulting documents.
* @return {DBQuery}
*/
DBQuery.prototype.projection = function(document) {
    this._checkModify();
    this._fields = document;
    return this;
};

/**
* Specify cursor as a tailable cursor, allowing to specify if it will use awaitData
*
* @method
* @see http://docs.mongodb.org/manual/tutorial/create-tailable-cursor/
* @param {boolean} [awaitData=true] cursor blocks for a few seconds to wait for data if no documents
*found.
* @return {DBQuery}
*/
DBQuery.prototype.tailable = function(awaitData) {
    this._checkModify();
    this.addOption(DBQuery.Option.tailable);

    // Set await data if either specifically set or not specified
    if (awaitData || awaitData == null) {
        this.addOption(DBQuery.Option.awaitData);
    }

    return this;
};

/**
* Specify a document containing modifiers for the query.
*
* @method
* @see http://docs.mongodb.org/manual/reference/operator/query-modifier/
* @param {object} document A document containing modifers to apply to the cursor.
* @return {DBQuery}
*/
DBQuery.prototype.modifiers = function(document) {
    this._checkModify();

    for (var name in document) {
        if (name[0] != '$') {
            throw new Error('All modifiers must start with a $ such as $maxScan or $returnKey');
        }
    }

    for (var name in document) {
        this._addSpecial(name, document[name]);
    }

    return this;
};

DBQuery.prototype.close = function() {
    this._cursor.close();
};

DBQuery.shellBatchSize = 20;

/**
 * Query option flag bit constants.
 * @see http://dochub.mongodb.org/core/mongowireprotocol#MongoWireProtocol-OPQUERY
 */
DBQuery.Option = {
    tailable: 0x2,
    slaveOk: 0x4,
    oplogReplay: 0x8,
    noTimeout: 0x10,
    awaitData: 0x20,
    exhaust: 0x40,
    partial: 0x80
};

function DBCommandCursor(mongo, cmdResult, batchSize) {
    if (cmdResult.ok != 1) {
        throw _getErrorWithCode(cmdResult, "error: " + tojson(cmdResult));
    }

    this._batch = cmdResult.cursor.firstBatch.reverse();  // modifies input to allow popping

    if (mongo.useReadCommands()) {
        this._useReadCommands = true;
        this._cursorid = cmdResult.cursor.id;
        this._batchSize = batchSize;

        this._ns = cmdResult.cursor.ns;
        this._db = mongo.getDB(this._ns.substr(0, this._ns.indexOf(".")));
        this._collName = this._ns.substr(this._ns.indexOf(".") + 1);

        if (cmdResult.cursor.id) {
            // Note that setting this._cursorid to 0 should be accompanied by
            // this._cursorHandle.zeroCursorId().
            this._cursorHandle = mongo.cursorHandleFromId(cmdResult.cursor.id);
        }
    } else {
        this._cursor = mongo.cursorFromId(cmdResult.cursor.ns, cmdResult.cursor.id, batchSize);
    }
}

DBCommandCursor.prototype = {};

DBCommandCursor.prototype.close = function() {
    if (!this._useReadCommands) {
        this._cursor.close();
    } else if (this._cursorid != 0) {
        var killCursorCmd = {
            killCursors: this._collName,
            cursors: [this._cursorid],
        };
        var cmdRes = this._db.runCommand(killCursorCmd);
        if (cmdRes.ok != 1) {
            throw _getErrorWithCode(cmdRes, "killCursors command failed: " + tojson(cmdRes));
        }

        this._cursorHandle.zeroCursorId();
        this._cursorid = NumberLong(0);
    }
};

/**
 * Fills out this._batch by running a getMore command. If the cursor is exhausted, also resets
 * this._cursorid to 0.
 *
 * Throws on error.
 */
DBCommandCursor.prototype._runGetMoreCommand = function() {
    // Construct the getMore command.
    var getMoreCmd = {
        getMore: this._cursorid,
        collection: this._collName
    };

    if (this._batchSize) {
        getMoreCmd["batchSize"] = this._batchSize;
    }

    // Deliver the getMore command, and check for errors in the response.
    var cmdRes = this._db.runCommand(getMoreCmd);
    if (cmdRes.ok != 1) {
        throw _getErrorWithCode(cmdRes, "getMore command failed: " + tojson(cmdRes));
    }

    if (this._ns !== cmdRes.cursor.ns) {
        throw Error("unexpected collection in getMore response: " + this._ns + " != " +
                    cmdRes.cursor.ns);
    }

    if (!cmdRes.cursor.id.compare(NumberLong("0"))) {
        this._cursorHandle.zeroCursorId();
        this._cursorid = NumberLong("0");
    } else if (this._cursorid.compare(cmdRes.cursor.id)) {
        throw Error("unexpected cursor id: " + this._cursorid.toString() + " != " +
                    cmdRes.cursor.id.toString());
    }

    // Successfully retrieved the next batch.
    this._batch = cmdRes.cursor.nextBatch.reverse();
};

DBCommandCursor.prototype._hasNextUsingCommands = function() {
    assert(this._useReadCommands);

    if (!this._batch.length) {
        if (!this._cursorid.compare(NumberLong("0"))) {
            return false;
        }

        this._runGetMoreCommand();
    }

    return this._batch.length > 0;
};

DBCommandCursor.prototype.hasNext = function() {
    if (this._useReadCommands) {
        return this._hasNextUsingCommands();
    }

    return this._batch.length || this._cursor.hasNext();
};

DBCommandCursor.prototype.next = function() {
    if (this._batch.length) {
        // $err wouldn't be in _firstBatch since ok was true.
        return this._batch.pop();
    } else if (this._useReadCommands) {
        // Have to call hasNext() here, as this is where we may issue a getMore in order to retrieve
        // the next batch of results.
        if (!this.hasNext())
            throw Error("error hasNext: false");
        return this._batch.pop();
    } else {
        if (!this._cursor.hasNext())
            throw Error("error hasNext: false");

        var ret = this._cursor.next();
        if (ret.$err)
            throw _getErrorWithCode(ret, "error: " + tojson(ret));
        return ret;
    }
};
DBCommandCursor.prototype.objsLeftInBatch = function() {
    if (this._useReadCommands) {
        return this._batch.length;
    } else if (this._batch.length) {
        return this._batch.length;
    } else {
        return this._cursor.objsLeftInBatch();
    }
};

DBCommandCursor.prototype.help = function() {
    // This is the same as the "Cursor Methods" section of DBQuery.help().
    print("\nCursor methods");
    print("\t.toArray() - iterates through docs and returns an array of the results");
    print("\t.forEach( func )");
    print("\t.map( func )");
    print("\t.hasNext()");
    print("\t.next()");
    print(
        "\t.objsLeftInBatch() - returns count of docs left in current batch (when exhausted, a new getMore will be issued)");
    print("\t.itcount() - iterates through documents and counts them");
    print("\t.pretty() - pretty print each document, possibly over multiple lines");
    print("\t.close()");
};

// Copy these methods from DBQuery
DBCommandCursor.prototype.toArray = DBQuery.prototype.toArray;
DBCommandCursor.prototype.forEach = DBQuery.prototype.forEach;
DBCommandCursor.prototype.map = DBQuery.prototype.map;
DBCommandCursor.prototype.itcount = DBQuery.prototype.itcount;
DBCommandCursor.prototype.shellPrint = DBQuery.prototype.shellPrint;
DBCommandCursor.prototype.pretty = DBQuery.prototype.pretty;

/**
 * QueryCache
 * Holds a reference to the cursor.
 * Proxy for planCache* query shape-specific commands.
 */
if ((typeof QueryPlan) == "undefined") {
    QueryPlan = function(cursor) {
        this._cursor = cursor;
    };
}

/**
 * Name of QueryPlan.
 * Same as collection.
 */
QueryPlan.prototype.getName = function() {
    return this._cursor._collection.getName();
};

/**
 * tojson prints the name of the collection
 */

QueryPlan.prototype.tojson = function(indent, nolint) {
    return tojson(this.getPlans());
};

/**
 * Displays help for a PlanCache object.
 */
QueryPlan.prototype.help = function() {
    var shortName = this.getName();
    print("QueryPlan help");
    print("\t.help() - show QueryPlan help");
    print("\t.clearPlans() - drops query shape from plan cache");
    print("\t.getPlans() - displays the cached plans for a query shape");
    return __magicNoPrint;
};

/**
 * List plans for a query shape.
 */
QueryPlan.prototype.getPlans = function() {
    return this._cursor._collection.getPlanCache().getPlansByQuery(this._cursor);
};

/**
 * Drop query shape from the plan cache.
 */
QueryPlan.prototype.clearPlans = function() {
    this._cursor._collection.getPlanCache().clearPlansByQuery(this._cursor);
    return;
};



// ---- MODULE: replsettest ---- 
/**
 * Sets up a replica set. To make the set running, call {@link #startSet},
 * followed by {@link #initiate} (and optionally,
 * {@link #awaitSecondaryNodes} to block till the  set is fully operational).
 * Note that some of the replica start up parameters are not passed here,
 * but to the #startSet method.
 *
 * @param {Object|string} opts If this value is a string, it specifies the connection string for
 *      a MongoD host to be used for recreating a ReplSetTest from. Otherwise, if it is an object,
 *      it must have the following contents:
 *
 *   {
 *     name {string}: name of this replica set. Default: 'testReplSet'
 *     host {string}: name of the host machine. Hostname will be used
 *        if not specified.
 *     useHostName {boolean}: if true, use hostname of machine,
 *        otherwise use localhost
 *     nodes {number|Object|Array.<Object>}: number of replicas. Default: 0.
 *        Can also be an Object (or Array).
 *        Format for Object:
 *          {
 *            <any string>: replica member option Object. @see MongoRunner.runMongod
 *            <any string2>: and so on...
 *          }
 *          If object has a special "rsConfig" field then those options will be used for each
 *          replica set member config options when used to initialize the replica set, or
 *          building the config with getReplSetConfig()
 *
 *        Format for Array:
 *           An array of replica member option Object. @see MongoRunner.runMongod
 *
 *        Note: For both formats, a special boolean property 'arbiter' can be
 *          specified to denote a member is an arbiter.
 *
 *        Note: A special "bridgeOptions" property can be specified in both the object and array
 *           formats to configure the options for the mongobridge corresponding to that node. These
 *           options are merged with the opts.bridgeOptions options, where the node-specific options
 *           take precedence.
 *
 *     nodeOptions {Object}: Options to apply to all nodes in the replica set.
 *        Format for Object:
 *          { cmdline-param-with-no-arg : "",
 *            param-with-arg : arg }
 *        This turns into "mongod --cmdline-param-with-no-arg --param-with-arg arg"
 *
 *     oplogSize {number}: Default: 40
 *     useSeedList {boolean}: Use the connection string format of this set
 *        as the replica set name (overrides the name property). Default: false
 *     keyFile {string}
 *     protocolVersion {number}: protocol version of replset used by the replset initiation.
 *
 *     useBridge {boolean}: If true, then a mongobridge process is started for each node in the
 *        replica set. Both the replica set configuration and the connections returned by startSet()
 *        will be references to the proxied connections. Defaults to false.
 *
 *     bridgeOptions {Object}: Options to apply to all mongobridge processes. Defaults to {}.
 *
 *     settings {object}: Setting used in the replica set config document.
 *        Example:
 *              settings: { chainingAllowed: false, ... }
 *   }
 *
 * Member variables:
 *  nodes {Array.<Mongo>} - connection to replica set members
 */
var ReplSetTest = function(opts) {
    'use strict';

    if (!(this instanceof ReplSetTest)) {
        return new ReplSetTest(opts);
    }

    // Capture the 'this' reference
    var self = this;

    // Replica set health states
    var Health = {
        UP: 1,
        DOWN: 0
    };

    var _alldbpaths;
    var _configSettings;

    // mongobridge related variables. Only available if the bridge option is selected.
    var _useBridge;
    var _bridgeOptions;
    var _unbridgedPorts;
    var _unbridgedNodes;

    // Publicly exposed variables

    /**
     * Populates a reference to all reachable nodes.
     */
    function _clearLiveNodes() {
        self.liveNodes = {
            master: null,
            slaves: []
        };
    }

    /**
     * Returns the config document reported from the specified connection.
     */
    function _replSetGetConfig(conn) {
        return assert.commandWorked(conn.adminCommand({replSetGetConfig: 1})).config;
    }

    /**
     * Invokes the 'ismaster' command on each individual node and returns whether the node is the
     * current RS master.
     */
    function _callIsMaster() {
        _clearLiveNodes();

        self.nodes.forEach(function(node) {
            try {
                var n = node.getDB('admin').runCommand({ismaster: 1});
                if (n.ismaster == true) {
                    self.liveNodes.master = node;
                } else {
                    node.setSlaveOk();
                    self.liveNodes.slaves.push(node);
                }
            } catch (err) {
                print("ReplSetTest Could not call ismaster on node " + node + ": " + tojson(err));
            }
        });

        return self.liveNodes.master || false;
    }

    /**
     * Wait for a rs indicator to go to a particular state or states.
     *
     * @param node is a single node or list of nodes, by id or conn
     * @param states is a single state or list of states
     * @param ind is the indicator specified
     * @param timeout how long to wait for the state to be reached
     */
    function _waitForIndicator(node, states, ind, timeout) {
        if (node.length) {
            var nodes = node;
            for (var i = 0; i < nodes.length; i++) {
                if (states.length)
                    _waitForIndicator(nodes[i], states[i], ind, timeout);
                else
                    _waitForIndicator(nodes[i], states, ind, timeout);
            }

            return;
        }

        timeout = timeout || 30000;

        if (!node.getDB) {
            node = self.nodes[node];
        }

        if (!states.length) {
            states = [states];
        }

        print("ReplSetTest waitForIndicator " + ind + " on " + node);
        printjson(states);
        print("ReplSetTest waitForIndicator from node " + node);

        var lastTime = null;
        var currTime = new Date().getTime();
        var status;

        assert.soon(function() {
            try {
                var conn = _callIsMaster();
                if (!conn) {
                    conn = self.liveNodes.slaves[0];
                }

                // Try again to load connection
                if (!conn)
                    return false;

                var getStatusFunc = function() {
                    status = conn.getDB('admin').runCommand({replSetGetStatus: 1});
                };

                if (self.keyFile) {
                    // Authenticate connection used for running replSetGetStatus if needed
                    authutil.asCluster(conn, self.keyFile, getStatusFunc);
                } else {
                    getStatusFunc();
                }
            } catch (ex) {
                print("ReplSetTest waitForIndicator could not get status: " + tojson(ex));
                return false;
            }

            var printStatus = false;
            if (lastTime == null || (currTime = new Date().getTime()) - (1000 * 5) > lastTime) {
                if (lastTime == null) {
                    print("ReplSetTest waitForIndicator Initial status (timeout : " + timeout +
                          ") :");
                }

                printjson(status);
                lastTime = new Date().getTime();
                printStatus = true;
            }

            if (typeof status.members == 'undefined') {
                return false;
            }

            for (var i = 0; i < status.members.length; i++) {
                if (printStatus) {
                    print("Status for : " + status.members[i].name + ", checking " + node.host +
                          "/" + node.name);
                }

                if (status.members[i].name == node.host || status.members[i].name == node.name) {
                    for (var j = 0; j < states.length; j++) {
                        if (printStatus) {
                            print("Status -- " + " current state: " + status.members[i][ind] +
                                  ",  target state : " + states[j]);
                        }

                        if (typeof(states[j]) != "number") {
                            throw new Error("State was not an number -- type:" + typeof(states[j]) +
                                            ", value:" + states[j]);
                        }
                        if (status.members[i][ind] == states[j]) {
                            return true;
                        }
                    }
                }
            }

            return false;

        }, "waiting for state indicator " + ind + " for " + timeout + "ms", timeout);

        print("ReplSetTest waitForIndicator final status:");
        printjson(status);
    }

    /**
     * Wait for a health indicator to go to a particular state or states.
     *
     * @param node is a single node or list of nodes, by id or conn
     * @param state is a single state or list of states. ReplSetTest.Health.DOWN can
     *     only be used in cases when there is a primary available or slave[0] can
     *     respond to the isMaster command.
     */
    function _waitForHealth(node, state, timeout) {
        _waitForIndicator(node, state, "health", timeout);
    }

    /**
     * Surrounds a function call by a try...catch to convert any exception to a print statement
     * and return false.
     */
    function _convertExceptionToReturnStatus(func, msg) {
        try {
            return func();
        } catch (e) {
            if (msg) {
                print(msg);
            }
            print("ReplSetTest caught exception " + e);
            return false;
        }
    }

    /**
     * Wraps assert.soon to try...catch any function passed in.
     */
    function _assertSoonNoExcept(func, msg, timeout) {
        assert.soon((() => _convertExceptionToReturnStatus(func)), msg, timeout);
    }

    /**
     * Returns the optime for the specified host by issuing replSetGetStatus.
     */
    function _getLastOpTime(conn) {
        var replSetStatus =
            assert.commandWorked(conn.getDB("admin").runCommand({replSetGetStatus: 1}));
        var connStatus = replSetStatus.members.filter(m => m.self)[0];
        return connStatus.optime;
    }

    /**
     * Returns the OpTime timestamp for the specified host by issuing replSetGetStatus.
     */
    function _getLastOpTimeTimestamp(conn) {
        var myOpTime = _getLastOpTime(conn);
        if (!myOpTime) {
            // Must be an ARBITER
            return undefined;
        }

        return myOpTime.ts ? myOpTime.ts : myOpTime;
    }

    /**
     * Returns the last committed OpTime for the replicaset as known by the host.
     * This function may return an OpTime with Timestamp(0,0) and Term(0) if there is no
     * last committed OpTime.
     */
    function _getLastCommittedOpTime(conn) {
        var replSetStatus =
            assert.commandWorked(conn.getDB("admin").runCommand({replSetGetStatus: 1}));
        return (replSetStatus.OpTimes || replSetStatus.optimes).lastCommittedOpTime || {
            ts: Timestamp(0, 0),
            t: NumberLong(0)
        };
    }

    /**
     * Returns the {readConcern: majority} OpTime for the host.
     * This is the OpTime of the host's "majority committed" snapshot.
     * This function may return an OpTime with Timestamp(0,0) and Term(0) if read concern majority
     * is not enabled, or if there has not been a committed snapshot yet.
     */
    function _getReadConcernMajorityOpTime(conn) {
        var replSetStatus =
            assert.commandWorked(conn.getDB("admin").runCommand({replSetGetStatus: 1}));
        return (replSetStatus.OpTimes || replSetStatus.optimes).readConcernMajorityOpTime || {
            ts: Timestamp(0, 0),
            t: NumberLong(0)
        };
    }

    function _isEarlierOpTime(ot1, ot2) {
        // Make sure both optimes have a timestamp and a term.
        ot1 = ot1.t ? ot1 : {
            ts: ot1,
            t: NumberLong(-1)
        };
        ot2 = ot2.t ? ot2 : {
            ts: ot2,
            t: NumberLong(-1)
        };

        // If both optimes have a term that's not -1 and one has a lower term, return that optime.
        if (!friendlyEqual(ot1.t, NumberLong(-1)) && !friendlyEqual(ot2.t, NumberLong(-1))) {
            if (!friendlyEqual(ot1.t, ot2.t)) {
                return ot1.t < ot2.t;
            }
        }

        // Otherwise, choose the optime with the lower timestamp.
        return ot1.ts < ot2.ts;
    }

    /**
     * Returns list of nodes as host:port strings.
     */
    this.nodeList = function() {
        var list = [];
        for (var i = 0; i < this.ports.length; i++) {
            list.push(this.host + ":" + this.ports[i]);
        }

        return list;
    };

    this.getNodeId = function(node) {
        if (node.toFixed) {
            return parseInt(node);
        }

        for (var i = 0; i < this.nodes.length; i++) {
            if (this.nodes[i] == node) {
                return i;
            }
        }

        if (node instanceof ObjectId) {
            for (i = 0; i < this.nodes.length; i++) {
                if (this.nodes[i].runId == node) {
                    return i;
                }
            }
        }

        if (node.nodeId != null) {
            return parseInt(node.nodeId);
        }

        return undefined;
    };

    this.getPort = function(n) {
        var n = this.getNodeId(n);
        return this.ports[n];
    };

    this._addPath = function(p) {
        if (!_alldbpaths)
            _alldbpaths = [p];
        else
            _alldbpaths.push(p);

        return p;
    };

    this.getReplSetConfig = function() {
        var cfg = {};
        cfg._id = this.name;

        if (this.protocolVersion !== undefined && this.protocolVersion !== null) {
            cfg.protocolVersion = this.protocolVersion;
        }

        cfg.members = [];

        for (var i = 0; i < this.ports.length; i++) {
            var member = {};
            member._id = i;

            var port = this.ports[i];
            member.host = this.host + ":" + port;

            var nodeOpts = this.nodeOptions["n" + i];
            if (nodeOpts) {
                if (nodeOpts.arbiter) {
                    member.arbiterOnly = true;
                }

                if (nodeOpts.rsConfig) {
                    Object.extend(member, nodeOpts.rsConfig);
                }
            }

            cfg.members.push(member);
        }

        if (jsTestOptions().useLegacyReplicationProtocol) {
            cfg.protocolVersion = 0;
        }

        if (_configSettings) {
            cfg.settings = _configSettings;
        }

        return cfg;
    };

    this.getURL = function() {
        var hosts = [];

        for (var i = 0; i < this.ports.length; i++) {
            hosts.push(this.host + ":" + this.ports[i]);
        }

        return this.name + "/" + hosts.join(",");
    };

    /**
     * Starts each node in the replica set with the given options.
     *
     * @param options - The options passed to {@link MongoRunner.runMongod}
     */
    this.startSet = function(options) {
        print("ReplSetTest starting set");

        var nodes = [];
        for (var n = 0; n < this.ports.length; n++) {
            nodes.push(this.start(n, options));
        }

        this.nodes = nodes;
        return this.nodes;
    };

    /**
     * Blocks until the secondary nodes have completed recovery and their roles are known.
     */
    this.awaitSecondaryNodes = function(timeout) {
        timeout = timeout || 60000;

        _assertSoonNoExcept(function() {
            // Reload who the current slaves are
            self.getPrimary(timeout);

            var slaves = self.liveNodes.slaves;
            var len = slaves.length;
            var ready = true;

            for (var i = 0; i < len; i++) {
                var isMaster = slaves[i].getDB("admin").runCommand({ismaster: 1});
                var arbiter = (isMaster.arbiterOnly == undefined ? false : isMaster.arbiterOnly);
                ready = ready && (isMaster.secondary || arbiter);
            }

            return ready;
        }, "Awaiting secondaries", timeout);
    };

    /**
     * Blocking call, which will wait for a primary to be elected for some pre-defined timeout and
     * if primary is available will return a connection to it. Otherwise throws an exception.
     */
    this.getPrimary = function(timeout) {
        timeout = timeout || 60000;
        var primary = null;

        _assertSoonNoExcept(function() {
            primary = _callIsMaster();
            return primary;
        }, "Finding primary", timeout);

        return primary;
    };

    this.awaitNoPrimary = function(msg, timeout) {
        msg = msg || "Timed out waiting for there to be no primary in replset: " + this.name;
        timeout = timeout || 30000;

        _assertSoonNoExcept(function() {
            return _callIsMaster() == false;
        }, msg, timeout);
    };

    this.getSecondaries = function(timeout) {
        var master = this.getPrimary(timeout);
        var secs = [];
        for (var i = 0; i < this.nodes.length; i++) {
            if (this.nodes[i] != master) {
                secs.push(this.nodes[i]);
            }
        }

        return secs;
    };

    this.getSecondary = function(timeout) {
        return this.getSecondaries(timeout)[0];
    };

    this.status = function(timeout) {
        var master = _callIsMaster();
        if (!master) {
            master = this.liveNodes.slaves[0];
        }

        return master.getDB("admin").runCommand({replSetGetStatus: 1});
    };

    /**
     * Adds a node to the replica set managed by this instance.
     */
    this.add = function(config) {
        var nextPort = allocatePort();
        print("ReplSetTest Next port: " + nextPort);

        this.ports.push(nextPort);
        printjson(this.ports);

        if (_useBridge) {
            _unbridgedPorts.push(allocatePort());
        }

        var nextId = this.nodes.length;
        printjson(this.nodes);

        print("ReplSetTest nextId: " + nextId);
        return this.start(nextId, config);
    };

    this.remove = function(nodeId) {
        nodeId = this.getNodeId(nodeId);
        this.nodes.splice(nodeId, 1);
        this.ports.splice(nodeId, 1);

        if (_useBridge) {
            _unbridgedPorts.splice(nodeId, 1);
            _unbridgedNodes.splice(nodeId, 1);
        }
    };

    this._setDefaultConfigOptions = function(config) {
        if (jsTestOptions().useLegacyReplicationProtocol &&
            !config.hasOwnProperty("protocolVersion")) {
            config.protocolVersion = 0;
        }
    };

    this.initiate = function(cfg, initCmd, timeout) {
        var master = this.nodes[0].getDB("admin");
        var config = cfg || this.getReplSetConfig();
        var cmd = {};
        var cmdKey = initCmd || 'replSetInitiate';
        timeout = timeout || 120000;

        this._setDefaultConfigOptions(config);

        cmd[cmdKey] = config;
        printjson(cmd);

        assert.commandWorked(master.runCommand(cmd), tojson(cmd));
        this.awaitSecondaryNodes(timeout);

        // Setup authentication if running test with authentication
        if ((jsTestOptions().keyFile) && cmdKey == 'replSetInitiate') {
            master = this.getPrimary();
            jsTest.authenticateNodes(this.nodes);
        }
    };

    /**
     * Gets the current replica set config from the specified node index. If no nodeId is specified,
     * uses the primary node.
     */
    this.getReplSetConfigFromNode = function(nodeId) {
        if (nodeId == undefined) {
            // Use 90 seconds timeout for finding a primary
            return _replSetGetConfig(self.getPrimary(90 * 1000));
        }

        if (!isNumber(nodeId)) {
            throw Error(nodeId + ' is not a number');
        }

        return _replSetGetConfig(self.nodes[nodeId]);
    };

    this.reInitiate = function() {
        var config = this.getReplSetConfig();
        var newVersion = this.getReplSetConfigFromNode().version + 1;
        config.version = newVersion;

        this._setDefaultConfigOptions(config);

        try {
            assert.commandWorked(this.getPrimary().adminCommand({replSetReconfig: config}));
        } catch (e) {
            if (tojson(e).indexOf("error doing query: failed") < 0) {
                throw e;
            }
        }
    };

    /**
     * Waits for the last oplog entry on the primary to be visible in the committed snapshop view
     * of the oplog on *all* secondaries.
     * Returns last oplog entry.
     */
    this.awaitLastOpCommitted = function() {
        var rst = this;
        var master = rst.getPrimary();
        var masterOpTime = _getLastOpTime(master);

        print("Waiting for op with OpTime " + tojson(masterOpTime) +
              " to be committed on all secondaries");

        _assertSoonNoExcept(function() {
            for (var i = 0; i < rst.nodes.length; i++) {
                var node = rst.nodes[i];

                // Continue if we're connected to an arbiter
                var res = assert.commandWorked(node.adminCommand({replSetGetStatus: 1}));
                if (res.myState == ReplSetTest.State.ARBITER) {
                    continue;
                }
                var rcmOpTime = _getReadConcernMajorityOpTime(node);
                if (friendlyEqual(rcmOpTime, {ts: Timestamp(0, 0), t: NumberLong(0)})) {
                    return false;
                }
                if (_isEarlierOpTime(rcmOpTime, masterOpTime)) {
                    return false;
                }
            }

            return true;
        }, "Op with OpTime " + tojson(masterOpTime) + " failed to be committed on all secondaries");

        return masterOpTime;
    };

    this.awaitReplication = function(timeout) {
        timeout = timeout || 30000;

        var masterLatestOpTime;

        // Blocking call, which will wait for the last optime written on the master to be available
        var awaitLastOpTimeWrittenFn = function() {
            var master = self.getPrimary();
            _assertSoonNoExcept(function() {
                try {
                    masterLatestOpTime = _getLastOpTimeTimestamp(master);
                } catch (e) {
                    print("ReplSetTest caught exception " + e);
                    return false;
                }

                return true;
            }, "awaiting oplog query", 30000);
        };

        awaitLastOpTimeWrittenFn();

        // get the latest config version from master. if there is a problem, grab master and try
        // again
        var configVersion;
        var masterOpTime;
        var masterName;
        var master;

        try {
            master = this.getPrimary();
            configVersion = this.getReplSetConfigFromNode().version;
            masterOpTime = _getLastOpTimeTimestamp(master);
            masterName = master.toString().substr(14);  // strip "connection to "
        } catch (e) {
            master = this.getPrimary();
            configVersion = this.getReplSetConfigFromNode().version;
            masterOpTime = _getLastOpTimeTimestamp(master);
            masterName = master.toString().substr(14);  // strip "connection to "
        }

        print("ReplSetTest awaitReplication: starting: timestamp for primary, " + masterName +
              ", is " + tojson(masterLatestOpTime) + ", last oplog entry is " +
              tojsononeline(masterOpTime));

        _assertSoonNoExcept(function() {
            try {
                print("ReplSetTest awaitReplication: checking secondaries against timestamp " +
                      tojson(masterLatestOpTime));
                var secondaryCount = 0;
                for (var i = 0; i < self.liveNodes.slaves.length; i++) {
                    var slave = self.liveNodes.slaves[i];
                    var slaveName = slave.toString().substr(14);  // strip "connection to "

                    var slaveConfigVersion =
                        slave.getDB("local")['system.replset'].findOne().version;

                    if (configVersion != slaveConfigVersion) {
                        print("ReplSetTest awaitReplication: secondary #" + secondaryCount + ", " +
                              slaveName + ", has config version #" + slaveConfigVersion +
                              ", but expected config version #" + configVersion);

                        if (slaveConfigVersion > configVersion) {
                            master = this.getPrimary();
                            configVersion =
                                master.getDB("local")['system.replset'].findOne().version;
                            masterOpTime = _getLastOpTimeTimestamp(master);
                            masterName = master.toString().substr(14);  // strip "connection to "

                            print("ReplSetTest awaitReplication: timestamp for primary, " +
                                  masterName + ", is " + tojson(masterLatestOpTime) +
                                  ", last oplog entry is " + tojsononeline(masterOpTime));
                        }

                        return false;
                    }

                    // Continue if we're connected to an arbiter
                    var res = assert.commandWorked(slave.adminCommand({replSetGetStatus: 1}));
                    if (res.myState == ReplSetTest.State.ARBITER) {
                        continue;
                    }

                    ++secondaryCount;
                    print("ReplSetTest awaitReplication: checking secondary #" + secondaryCount +
                          ": " + slaveName);

                    slave.getDB("admin").getMongo().setSlaveOk();

                    var ts = _getLastOpTimeTimestamp(slave);
                    if (masterLatestOpTime.t < ts.t ||
                        (masterLatestOpTime.t == ts.t && masterLatestOpTime.i < ts.i)) {
                        masterLatestOpTime = _getLastOpTimeTimestamp(master);
                        print("ReplSetTest awaitReplication: timestamp for " + slaveName +
                              " is newer, resetting latest to " + tojson(masterLatestOpTime));
                        return false;
                    }

                    if (!friendlyEqual(masterLatestOpTime, ts)) {
                        print("ReplSetTest awaitReplication: timestamp for secondary #" +
                              secondaryCount + ", " + slaveName + ", is " + tojson(ts) +
                              " but latest is " + tojson(masterLatestOpTime));
                        print("ReplSetTest awaitReplication: secondary #" + secondaryCount + ", " +
                              slaveName + ", is NOT synced");
                        return false;
                    }

                    print("ReplSetTest awaitReplication: secondary #" + secondaryCount + ", " +
                          slaveName + ", is synced");
                }

                print("ReplSetTest awaitReplication: finished: all " + secondaryCount +
                      " secondaries synced at timestamp " + tojson(masterLatestOpTime));
                return true;
            } catch (e) {
                print("ReplSetTest awaitReplication: caught exception " + e + ';\n' + e.stack);

                // We might have a new master now
                awaitLastOpTimeWrittenFn();

                print("ReplSetTest awaitReplication: resetting: timestamp for primary " +
                      self.liveNodes.master + " is " + tojson(masterLatestOpTime));

                return false;
            }
        }, "awaiting replication", timeout);
    };

    this.getHashes = function(db) {
        this.getPrimary();
        var res = {};
        res.master = this.liveNodes.master.getDB(db).runCommand("dbhash");
        res.slaves = this.liveNodes.slaves.map(function(z) {
            return z.getDB(db).runCommand("dbhash");
        });
        return res;
    };

    /**
     * Starts up a server.  Options are saved by default for subsequent starts.
     *
     *
     * Options { remember : true } re-applies the saved options from a prior start.
     * Options { noRemember : true } ignores the current properties.
     * Options { appendOptions : true } appends the current options to those remembered.
     * Options { startClean : true } clears the data directory before starting.
     *
     * @param {int|conn|[int|conn]} n array or single server number (0, 1, 2, ...) or conn
     * @param {object} [options]
     * @param {boolean} [restart] If false, the data directory will be cleared
     *   before the server starts.  Default: false.
     *
     */
    this.start = function(n, options, restart, wait) {
        if (n.length) {
            var nodes = n;
            var started = [];

            for (var i = 0; i < nodes.length; i++) {
                if (this.start(nodes[i], Object.merge({}, options), restart, wait)) {
                    started.push(nodes[i]);
                }
            }

            return started;
        }

        // TODO: should we do something special if we don't currently know about this node?
        n = this.getNodeId(n);

        print("ReplSetTest n is : " + n);

        var defaults = {
            useHostName: this.useHostName,
            oplogSize: this.oplogSize,
            keyFile: this.keyFile,
            port: _useBridge ? _unbridgedPorts[n] : this.ports[n],
            noprealloc: "",
            smallfiles: "",
            replSet: this.useSeedList ? this.getURL() : this.name,
            dbpath: "$set-$node"
        };

        //
        // Note : this replaces the binVersion of the shared startSet() options the first time
        // through, so the full set is guaranteed to have different versions if size > 1.  If using
        // start() independently, independent version choices will be made
        //
        if (options && options.binVersion) {
            options.binVersion = MongoRunner.versionIterator(options.binVersion);
        }

        options = Object.merge(defaults, options);
        options = Object.merge(options, this.nodeOptions["n" + n]);
        delete options.rsConfig;

        options.restart = options.restart || restart;

        var pathOpts = {
            node: n,
            set: this.name
        };
        options.pathOpts = Object.merge(options.pathOpts || {}, pathOpts);

        if (tojson(options) != tojson({}))
            printjson(options);

        print("ReplSetTest " + (restart ? "(Re)" : "") + "Starting....");

        if (_useBridge) {
            var bridgeOptions = Object.merge(_bridgeOptions, options.bridgeOptions || {});
            bridgeOptions = Object.merge(
                bridgeOptions,
                {
                  hostName: this.host,
                  port: this.ports[n],
                  // The mongod processes identify themselves to mongobridge as host:port, where the
                  // host is the actual hostname of the machine and not localhost.
                  dest: getHostName() + ":" + _unbridgedPorts[n],
                });

            this.nodes[n] = new MongoBridge(bridgeOptions);
        }

        var conn = MongoRunner.runMongod(options);
        if (!conn) {
            throw new Error("Failed to start node " + n);
        }

        // Make sure to call _addPath, otherwise folders won't be cleaned.
        this._addPath(conn.dbpath);

        if (_useBridge) {
            this.nodes[n].connectToBridge();
            _unbridgedNodes[n] = conn;
        } else {
            this.nodes[n] = conn;
        }

        // Add replica set specific attributes.
        this.nodes[n].nodeId = n;

        printjson(this.nodes);

        wait = wait || false;
        if (!wait.toFixed) {
            if (wait)
                wait = 0;
            else
                wait = -1;
        }

        if (wait >= 0) {
            // Wait for node to start up.
            _waitForHealth(this.nodes[n], Health.UP, wait);
        }

        return this.nodes[n];
    };

    /**
     * Restarts a db without clearing the data directory by default.  If the server is not
     * stopped first, this function will not work.
     *
     * Option { startClean : true } forces clearing the data directory.
     * Option { auth : Object } object that contains the auth details for admin credentials.
     *   Should contain the fields 'user' and 'pwd'
     *
     * @param {int|conn|[int|conn]} n array or single server number (0, 1, 2, ...) or conn
     */
    this.restart = function(n, options, signal, wait) {
        // Can specify wait as third parameter, if using default signal
        if (signal == true || signal == false) {
            wait = signal;
            signal = undefined;
        }

        this.stop(n, signal, options);

        var started = this.start(n, options, true, wait);

        if (jsTestOptions().keyFile) {
            if (started.length) {
                // if n was an array of conns, start will return an array of connections
                for (var i = 0; i < started.length; i++) {
                    jsTest.authenticate(started[i]);
                }
            } else {
                jsTest.authenticate(started);
            }
        }
        return started;
    };

    this.stopMaster = function(signal, opts) {
        var master = this.getPrimary();
        var master_id = this.getNodeId(master);
        return this.stop(master_id, signal, opts);
    };

    /**
     * Stops a particular node or nodes, specified by conn or id
     *
     * @param {number|Mongo} n the index or connection object of the replica set member to stop.
     * @param {number} signal the signal number to use for killing
     * @param {Object} opts @see MongoRunner.stopMongod
     */
    this.stop = function(n, signal, opts) {
        // Flatten array of nodes to stop
        if (n.length) {
            var nodes = n;

            var stopped = [];
            for (var i = 0; i < nodes.length; i++) {
                if (this.stop(nodes[i], signal, opts))
                    stopped.push(nodes[i]);
            }

            return stopped;
        }

        // Can specify wait as second parameter, if using default signal
        if (signal == true || signal == false) {
            signal = undefined;
        }

        n = this.getNodeId(n);

        var port = _useBridge ? _unbridgedPorts[n] : this.ports[n];
        print('ReplSetTest stop *** Shutting down mongod in port ' + port + ' ***');
        var ret = MongoRunner.stopMongod(port, signal, opts);

        print('ReplSetTest stop *** Mongod in port ' + port + ' shutdown with code (' + ret +
              ') ***');

        if (_useBridge) {
            this.nodes[n].stop();
        }

        return ret;
    };

    /**
     * Kill all members of this replica set.
     *
     * @param {number} signal The signal number to use for killing the members
     * @param {boolean} forRestart will not cleanup data directory
     * @param {Object} opts @see MongoRunner.stopMongod
     */
    this.stopSet = function(signal, forRestart, opts) {
        for (var i = 0; i < this.ports.length; i++) {
            this.stop(i, signal, opts);
        }

        if (forRestart) {
            return;
        }

        if (_alldbpaths) {
            print("ReplSetTest stopSet deleting all dbpaths");
            for (var i = 0; i < _alldbpaths.length; i++) {
                resetDbpath(_alldbpaths[i]);
            }
        }

        _forgetReplSet(this.name);

        print('ReplSetTest stopSet *** Shut down repl set - test worked ****');
    };

    /**
     * Walks all oplogs and ensures matching entries.
     */
    this.ensureOplogsMatch = function() {
        var OplogReader = function(mongo) {
            this.next = function() {
                if (!this.cursor)
                    throw Error("reader is not open!");

                var nextDoc = this.cursor.next();
                if (nextDoc)
                    this.lastDoc = nextDoc;
                return nextDoc;
            };

            this.getLastDoc = function() {
                if (this.lastDoc)
                    return this.lastDoc;
                return this.next();
            };

            this.hasNext = function() {
                if (!this.cursor)
                    throw Error("reader is not open!");
                return this.cursor.hasNext();
            };

            this.query = function(ts) {
                var coll = this.getOplogColl();
                var query = {
                    "ts": {"$gte": ts ? ts : new Timestamp()}
                };
                this.cursor = coll.find(query).sort({$natural: 1});
                this.cursor.addOption(DBQuery.Option.oplogReplay);
            };

            this.getFirstDoc = function() {
                return this.getOplogColl().find().sort({$natural: 1}).limit(-1).next();
            };

            this.getOplogColl = function() {
                return this.mongo.getDB("local")["oplog.rs"];
            };

            this.lastDoc = null;
            this.cursor = null;
            this.mongo = mongo;
        };

        if (this.nodes.length && this.nodes.length > 1) {
            var readers = [];
            var largestTS = null;
            var nodes = this.nodes;
            var rsSize = nodes.length;
            for (var i = 0; i < rsSize; i++) {
                readers[i] = new OplogReader(nodes[i]);
                var currTS = readers[i].getFirstDoc().ts;
                if (currTS.t > largestTS.t || (currTS.t == largestTS.t && currTS.i > largestTS.i)) {
                    largestTS = currTS;
                }
            }

            // start all oplogReaders at the same place.
            for (i = 0; i < rsSize; i++) {
                readers[i].query(largestTS);
            }

            var firstReader = readers[0];
            while (firstReader.hasNext()) {
                var ts = firstReader.next().ts;
                for (i = 1; i < rsSize; i++) {
                    assert.eq(
                        ts, readers[i].next().ts, " non-matching ts for node: " + readers[i].mongo);
                }
            }

            // ensure no other node has more oplog
            for (i = 1; i < rsSize; i++) {
                assert.eq(
                    false, readers[i].hasNext(), "" + readers[i] + " shouldn't have more oplog.");
            }
        }
    };

    /**
     * Wait for a state indicator to go to a particular state or states.
     *
     * @param node is a single node or list of nodes, by id or conn
     * @param state is a single state or list of states
     *
     */
    this.waitForState = function(node, state, timeout) {
        _waitForIndicator(node, state, "state", timeout);
    };

    /**
     * Waits until there is a master node.
     */
    this.waitForMaster = function(timeout) {
        var master;
        _assertSoonNoExcept(function() {
            return (master = self.getPrimary());
        }, "waiting for master", timeout);

        return master;
    };

    //
    // ReplSetTest constructors
    //

    /**
     * Constructor, which initializes the ReplSetTest object by starting new instances.
     */
    function _constructStartNewInstances(opts) {
        self.name = opts.name || "testReplSet";
        print('Starting new replica set ' + self.name);

        self.useHostName = opts.useHostName == undefined ? true : opts.useHostName;
        self.host = self.useHostName ? (opts.host || getHostName()) : 'localhost';
        self.oplogSize = opts.oplogSize || 40;
        self.useSeedList = opts.useSeedList || false;
        self.keyFile = opts.keyFile;
        self.protocolVersion = opts.protocolVersion;

        _useBridge = opts.useBridge || false;
        _bridgeOptions = opts.bridgeOptions || {};

        _configSettings = opts.settings || false;

        self.nodeOptions = {};

        var numNodes;

        if (isObject(opts.nodes)) {
            var len = 0;
            for (var i in opts.nodes) {
                var options = self.nodeOptions["n" + len] =
                    Object.merge(opts.nodeOptions, opts.nodes[i]);
                if (i.startsWith("a")) {
                    options.arbiter = true;
                }

                len++;
            }

            numNodes = len;
        } else if (Array.isArray(opts.nodes)) {
            for (var i = 0; i < opts.nodes.length; i++) {
                self.nodeOptions["n" + i] = Object.merge(opts.nodeOptions, opts.nodes[i]);
            }

            numNodes = opts.nodes.length;
        } else {
            for (var i = 0; i < opts.nodes; i++) {
                self.nodeOptions["n" + i] = opts.nodeOptions;
            }

            numNodes = opts.nodes;
        }

        self.ports = allocatePorts(numNodes);
        self.nodes = [];

        if (_useBridge) {
            _unbridgedPorts = allocatePorts(numNodes);
            _unbridgedNodes = [];
        }
    }

    /**
     * Constructor, which instantiates the ReplSetTest object from an existing set.
     */
    function _constructFromExistingSeedNode(seedNode) {
        var conf = _replSetGetConfig(new Mongo(seedNode));
        print('Recreating replica set from config ' + tojson(conf));

        var existingNodes = conf.members.map(member => member.host);
        self.ports = existingNodes.map(node => node.split(':')[1]);
        self.nodes = existingNodes.map(node => new Mongo(node));
    }

    if (typeof opts === 'string' || opts instanceof String) {
        _constructFromExistingSeedNode(opts);
    } else {
        _constructStartNewInstances(opts);
    }
};

/**
 * Set of states that the replica set can be in. Used for the wait functions.
 */
ReplSetTest.State = {
    PRIMARY: 1,
    SECONDARY: 2,
    RECOVERING: 3,
    // Note there is no state 4
    STARTUP_2: 5,
    UNKNOWN: 6,
    ARBITER: 7,
    DOWN: 8,
    ROLLBACK: 9,
    REMOVED: 10,
};

/**
 * Waits for the specified hosts to enter a certain state.
 */
ReplSetTest.awaitRSClientHosts = function(conn, host, hostOk, rs, timeout) {
    var hostCount = host.length;
    if (hostCount) {
        for (var i = 0; i < hostCount; i++) {
            ReplSetTest.awaitRSClientHosts(conn, host[i], hostOk, rs);
        }

        return;
    }

    timeout = timeout || 60000;

    if (hostOk == undefined)
        hostOk = {
            ok: true
        };
    if (host.host)
        host = host.host;
    if (rs)
        rs = rs.name;

    print("Awaiting " + host + " to be " + tojson(hostOk) + " for " + conn + " (rs: " + rs + ")");

    var tests = 0;

    assert.soon(function() {
        var rsClientHosts = conn.adminCommand('connPoolStats').replicaSets;
        if (tests++ % 10 == 0) {
            printjson(rsClientHosts);
        }

        for (var rsName in rsClientHosts) {
            if (rs && rs != rsName)
                continue;

            for (var i = 0; i < rsClientHosts[rsName].hosts.length; i++) {
                var clientHost = rsClientHosts[rsName].hosts[i];
                if (clientHost.addr != host)
                    continue;

                // Check that *all* host properties are set correctly
                var propOk = true;
                for (var prop in hostOk) {
                    if (isObject(hostOk[prop])) {
                        if (!friendlyEqual(hostOk[prop], clientHost[prop])) {
                            propOk = false;
                            break;
                        }
                    } else if (clientHost[prop] != hostOk[prop]) {
                        propOk = false;
                        break;
                    }
                }

                if (propOk) {
                    return true;
                }
            }
        }

        return false;
    }, 'timed out waiting for replica set client to recognize hosts', timeout);
};



// ---- MODULE: servers ---- 
var MongoRunner, _startMongod, startMongoProgram, runMongoProgram, startMongoProgramNoConnect,
    myPort;

(function() {
    "use strict";

    var shellVersion = function(){return "4.2";};

    var _parsePath = function() {
        var dbpath = "";
        for (var i = 0; i < arguments.length; ++i)
            if (arguments[i] == "--dbpath")
                dbpath = arguments[i + 1];

        if (dbpath == "")
            throw Error("No dbpath specified");

        return dbpath;
    };

    var _parsePort = function() {
        var port = "";
        for (var i = 0; i < arguments.length; ++i)
            if (arguments[i] == "--port")
                port = arguments[i + 1];

        if (port == "")
            throw Error("No port specified");
        return port;
    };

    var createMongoArgs = function(binaryName, args) {
        if (!Array.isArray(args)) {
            throw new Error("The second argument to createMongoArgs must be an array");
        }

        var fullArgs = [binaryName];

        if (args.length == 1 && isObject(args[0])) {
            var o = args[0];
            for (var k in o) {
                if (o.hasOwnProperty(k)) {
                    if (k == "v" && isNumber(o[k])) {
                        var n = o[k];
                        if (n > 0) {
                            if (n > 10)
                                n = 10;
                            var temp = "-";
                            while (n-- > 0)
                                temp += "v";
                            fullArgs.push(temp);
                        }
                    } else {
                        fullArgs.push("--" + k);
                        if (o[k] != "")
                            fullArgs.push("" + o[k]);
                    }
                }
            }
        } else {
            for (var i = 0; i < args.length; i++)
                fullArgs.push(args[i]);
        }

        return fullArgs;
    };

    MongoRunner = function() {};

    MongoRunner.dataDir = "/data/db";
    MongoRunner.dataPath = "/data/db/";

    MongoRunner.VersionSub = function(pattern, version) {
        this.pattern = pattern;
        this.version = version;
    };

    /**
     * Returns an array of version elements from a version string.
     *
     * "3.3.4-fade3783" -> ["3", "3", "4-fade3783" ]
     * "3.2" -> [ "3", "2" ]
     * 3 -> exception: versions must have at least two components.
     */
    var convertVersionStringToArray = function(versionString) {
        assert("" !== versionString, "Version strings must not be empty");
        var versionArray = versionString.split('.');
        assert.gt(versionArray.length,
                  1,
                  "MongoDB versions must have at least two components to compare, but \"" +
                      versionString + "\" has " + versionArray.length);
        return versionArray;
    };

    /**
     * Returns the major version string from a version string.
     *
     * 3.3.4-fade3783 -> 3.3
     * 3.2 -> 3.2
     * 3 -> exception: versions must have at least two components.
     */
    var extractMajorVersionFromVersionString = function(versionString) {
        return convertVersionStringToArray(versionString).slice(0, 2).join('.');
    };

    // These patterns allow substituting the binary versions used for each version string to support
    // the
    // dev/stable MongoDB release cycle.
    //
    // If you add a new version substitution to this list, you should add it to the lists of
    // versions being checked in 'verify_versions_test.js' to verify it is susbstituted correctly.
    MongoRunner.binVersionSubs = [
        new MongoRunner.VersionSub("latest", shellVersion()),
        new MongoRunner.VersionSub(extractMajorVersionFromVersionString(shellVersion()),
                                   shellVersion()),
        // To-be-updated when we branch for the next release.
        new MongoRunner.VersionSub("last-stable", "3.2")
    ];

    MongoRunner.getBinVersionFor = function(version) {

        // If this is a version iterator, iterate the version via toString()
        if (version instanceof MongoRunner.versionIterator.iterator) {
            version = version.toString();
        }

        if (version == null)
            version = "";
        version = version.trim();
        if (version === "")
            version = "latest";

        // See if this version is affected by version substitutions
        for (var i = 0; i < MongoRunner.binVersionSubs.length; i++) {
            var sub = MongoRunner.binVersionSubs[i];
            if (sub.pattern == version) {
                return sub.version;
            }
        }

        return version;
    };

    /**
     * Returns true if two version strings could represent the same version. This is true
     * if, after passing the versions through getBinVersionFor, the the versions have the
     * same value for each version component up through the length of the shorter version.
     *
     * That is, 3.2.4 compares equal to 3.2, but 3.2.4 does not compare equal to 3.2.3.
     */
    MongoRunner.areBinVersionsTheSame = function(versionA, versionB) {

        versionA = convertVersionStringToArray(MongoRunner.getBinVersionFor(versionA));
        versionB = convertVersionStringToArray(MongoRunner.getBinVersionFor(versionB));

        var elementsToCompare = Math.min(versionA.length, versionB.length);
        for (var i = 0; i < elementsToCompare; ++i) {
            if (versionA[i] != versionB[i]) {
                return false;
            }
        }
        return true;
    };

    MongoRunner.logicalOptions = {
        runId: true,
        env: true,
        pathOpts: true,
        remember: true,
        noRemember: true,
        appendOptions: true,
        restart: true,
        noCleanData: true,
        cleanData: true,
        startClean: true,
        forceLock: true,
        useLogFiles: true,
        logFile: true,
        useHostName: true,
        useHostname: true,
        noReplSet: true,
        forgetPort: true,
        arbiter: true,
        noJournalPrealloc: true,
        noJournal: true,
        binVersion: true,
        waitForConnect: true,
        bridgeOptions: true
    };

    MongoRunner.toRealPath = function(path, pathOpts) {

        // Replace all $pathOptions with actual values
        pathOpts = pathOpts || {};
        path = path.replace(/\$dataPath/g, MongoRunner.dataPath);
        path = path.replace(/\$dataDir/g, MongoRunner.dataDir);
        for (var key in pathOpts) {
            path = path.replace(RegExp("\\$" + RegExp.escape(key), "g"), pathOpts[key]);
        }

        // Relative path
        // Detect Unix and Windows absolute paths
        // as well as Windows drive letters
        // Also captures Windows UNC paths

        if (!path.match(/^(\/|\\|[A-Za-z]:)/)) {
            if (path != "" && !path.endsWith("/"))
                path += "/";

            path = MongoRunner.dataPath + path;
        }

        return path;

    };

    MongoRunner.toRealDir = function(path, pathOpts) {

        path = MongoRunner.toRealPath(path, pathOpts);

        if (path.endsWith("/"))
            path = path.substring(0, path.length - 1);

        return path;
    };

    MongoRunner.toRealFile = MongoRunner.toRealDir;

    /**
     * Returns an iterator object which yields successive versions on toString(), starting from a
     * random initial position, from an array of versions.
     *
     * If passed a single version string or an already-existing version iterator, just returns the
     * object itself, since it will yield correctly on toString()
     *
     * @param {Array.<String>}|{String}|{versionIterator}
     */
    MongoRunner.versionIterator = function(arr, isRandom) {

        // If this isn't an array of versions, or is already an iterator, just use it
        if (typeof arr == "string")
            return arr;
        if (arr.isVersionIterator)
            return arr;

        if (isRandom == undefined)
            isRandom = false;

        // Starting pos
        var i = isRandom ? parseInt(Random.rand() * arr.length) : 0;

        return new MongoRunner.versionIterator.iterator(i, arr);
    };

    MongoRunner.versionIterator.iterator = function(i, arr) {

        this.toString = function() {
            i = (i + 1) % arr.length;
            print("Returning next version : " + i + " (" + arr[i] + ") from " + tojson(arr) +
                  "...");
            return arr[i];
        };

        this.isVersionIterator = true;

    };

    /**
     * Converts the args object by pairing all keys with their value and appending
     * dash-dash (--) to the keys. The only exception to this rule are keys that
     * are defined in MongoRunner.logicalOptions, of which they will be ignored.
     *
     * @param {string} binaryName
     * @param {Object} args
     *
     * @return {Array.<String>} an array of parameter strings that can be passed
     *   to the binary.
     */
    MongoRunner.arrOptions = function(binaryName, args) {

        var fullArgs = [""];

        // isObject returns true even if "args" is an array, so the else branch of this statement is
        // dead code.  See SERVER-14220.
        if (isObject(args) || (args.length == 1 && isObject(args[0]))) {
            var o = isObject(args) ? args : args[0];

            // If we've specified a particular binary version, use that
            if (o.binVersion && o.binVersion != "" && o.binVersion != shellVersion()) {
                binaryName += "-" + o.binVersion;
            }

            // Manage legacy options
            var isValidOptionForBinary = function(option, value) {

                if (!o.binVersion)
                    return true;

                // Version 1.x options
                if (o.binVersion.startsWith("1.")) {
                    return ["nopreallocj"].indexOf(option) < 0;
                }

                return true;
            };

            var addOptionsToFullArgs = function(k, v) {
                if (v === undefined || v === null)
                    return;

                fullArgs.push("--" + k);

                if (v != "") {
                    fullArgs.push("" + v);
                }
            };

            for (var k in o) {
                // Make sure our logical option should be added to the array of options
                if (!o.hasOwnProperty(k) || k in MongoRunner.logicalOptions ||
                    !isValidOptionForBinary(k, o[k]))
                    continue;

                if ((k == "v" || k == "verbose") && isNumber(o[k])) {
                    var n = o[k];
                    if (n > 0) {
                        if (n > 10)
                            n = 10;
                        var temp = "-";
                        while (n-- > 0)
                            temp += "v";
                        fullArgs.push(temp);
                    }
                } else if (k === "setParameter" && isObject(o[k])) {
                    // If the value associated with the setParameter option is an object, we want
                    // to add all key-value pairs in that object as separate --setParameters.
                    Object.keys(o[k]).forEach(function(paramKey) {
                        addOptionsToFullArgs(k, "" + paramKey + "=" + o[k][paramKey]);
                    });
                } else {
                    addOptionsToFullArgs(k, o[k]);
                }
            }
        } else {
            for (var i = 0; i < args.length; i++)
                fullArgs.push(args[i]);
        }

        fullArgs[0] = binaryName;
        return fullArgs;
    };

    MongoRunner.arrToOpts = function(arr) {

        var opts = {};
        for (var i = 1; i < arr.length; i++) {
            if (arr[i].startsWith("-")) {
                var opt = arr[i].replace(/^-/, "").replace(/^-/, "");

                if (arr.length > i + 1 && !arr[i + 1].startsWith("-")) {
                    opts[opt] = arr[i + 1];
                    i++;
                } else {
                    opts[opt] = "";
                }

                if (opt.replace(/v/g, "") == "") {
                    opts["verbose"] = opt.length;
                }
            }
        }

        return opts;
    };

    MongoRunner.savedOptions = {};

    MongoRunner.mongoOptions = function(opts) {
        // Don't remember waitForConnect
        var waitForConnect = opts.waitForConnect;
        delete opts.waitForConnect;

        // If we're a mongo object
        if (opts.getDB) {
            opts = {
                restart: opts.runId
            };
        }

        // Initialize and create a copy of the opts
        opts = Object.merge(opts || {}, {});

        if (!opts.restart)
            opts.restart = false;

        // RunId can come from a number of places
        // If restart is passed as an old connection
        if (opts.restart && opts.restart.getDB) {
            opts.runId = opts.restart.runId;
            opts.restart = true;
        }
        // If it's the runId itself
        else if (isObject(opts.restart)) {
            opts.runId = opts.restart;
            opts.restart = true;
        }

        if (isObject(opts.remember)) {
            opts.runId = opts.remember;
            opts.remember = true;
        } else if (opts.remember == undefined) {
            // Remember by default if we're restarting
            opts.remember = opts.restart;
        }

        // If we passed in restart : <conn> or runId : <conn>
        if (isObject(opts.runId) && opts.runId.runId)
            opts.runId = opts.runId.runId;

        if (opts.restart && opts.remember) {
            opts = Object.merge(MongoRunner.savedOptions[opts.runId], opts);
        }

        // Create a new runId
        opts.runId = opts.runId || ObjectId();

        if (opts.forgetPort) {
            delete opts.port;
        }

        // Normalize and get the binary version to use
        if (opts.hasOwnProperty('binVersion')) {
            opts.binVersion = MongoRunner.getBinVersionFor(opts.binVersion);
        }

        // Default for waitForConnect is true
        opts.waitForConnect =
            (waitForConnect == undefined || waitForConnect == null) ? true : waitForConnect;

        opts.port = opts.port || allocatePort();

        opts.pathOpts =
            Object.merge(opts.pathOpts || {}, {port: "" + opts.port, runId: "" + opts.runId});

        var shouldRemember =
            (!opts.restart && !opts.noRemember) || (opts.restart && opts.appendOptions);
        if (shouldRemember) {
            MongoRunner.savedOptions[opts.runId] = Object.merge(opts, {});
        }

        return opts;
    };

    /**
     * @option {object} opts
     *
     *   {
     *     dbpath {string}
     *     useLogFiles {boolean}: use with logFile option.
     *     logFile {string}: path to the log file. If not specified and useLogFiles
     *       is true, automatically creates a log file inside dbpath.
     *     noJournalPrealloc {boolean}
     *     noJournal {boolean}
     *     keyFile
     *     replSet
     *     oplogSize
     *   }
     */
    MongoRunner.mongodOptions = function(opts) {

        opts = MongoRunner.mongoOptions(opts);

        opts.dbpath = MongoRunner.toRealDir(opts.dbpath || "$dataDir/mongod-$port", opts.pathOpts);

        opts.pathOpts = Object.merge(opts.pathOpts, {dbpath: opts.dbpath});

        if (!opts.logFile && opts.useLogFiles) {
            opts.logFile = opts.dbpath + "/mongod.log";
        } else if (opts.logFile) {
            opts.logFile = MongoRunner.toRealFile(opts.logFile, opts.pathOpts);
        }

        if (opts.logFile !== undefined) {
            opts.logpath = opts.logFile;
        }

        if (jsTestOptions().noJournalPrealloc || opts.noJournalPrealloc)
            opts.nopreallocj = "";

        if ((jsTestOptions().noJournal || opts.noJournal) && !('journal' in opts) &&
            !('configsvr' in opts)) {
            opts.nojournal = "";
        }

        if (jsTestOptions().keyFile && !opts.keyFile) {
            opts.keyFile = jsTestOptions().keyFile;
        }

        if (opts.hasOwnProperty("enableEncryption")) {
            // opts.enableEncryption, if set, must be an empty string
            if (opts.enableEncryption !== "") {
                throw new Error("The enableEncryption option must be an empty string if it is " +
                                "specified");
            }
        } else if (jsTestOptions().enableEncryption !== undefined) {
            if (jsTestOptions().enableEncryption !== "") {
                throw new Error("The enableEncryption option must be an empty string if it is " +
                                "specified");
            }
            opts.enableEncryption = "";
        }

        if (opts.hasOwnProperty("encryptionKeyFile")) {
            // opts.encryptionKeyFile, if set, must be a string
            if (typeof opts.encryptionKeyFile !== "string") {
                throw new Error("The encryptionKeyFile option must be a string if it is specified");
            }
        } else if (jsTestOptions().encryptionKeyFile !== undefined) {
            if (typeof(jsTestOptions().encryptionKeyFile) !== "string") {
                throw new Error("The encryptionKeyFile option must be a string if it is specified");
            }
            opts.encryptionKeyFile = jsTestOptions().encryptionKeyFile;
        }

        if (opts.hasOwnProperty("auditDestination")) {
            // opts.auditDestination, if set, must be a string
            if (typeof opts.auditDestination !== "string") {
                throw new Error("The auditDestination option must be a string if it is specified");
            }
        } else if (jsTestOptions().auditDestination !== undefined) {
            if (typeof(jsTestOptions().auditDestination) !== "string") {
                throw new Error("The auditDestination option must be a string if it is specified");
            }
            opts.auditDestination = jsTestOptions().auditDestination;
        }

        if (opts.hasOwnProperty("enableMajorityReadConcern")) {
            // opts.enableMajorityReadConcern, if set, must be an empty string
            if (opts.enableMajorityReadConcern !== "") {
                throw new Error("The enableMajorityReadConcern option must be an empty string if " +
                                "it is specified");
            }
        } else if (jsTestOptions().enableMajorityReadConcern !== undefined) {
            if (jsTestOptions().enableMajorityReadConcern !== "") {
                throw new Error("The enableMajorityReadConcern option must be an empty string if " +
                                "it is specified");
            }
            opts.enableMajorityReadConcern = "";
        }

        if (opts.noReplSet)
            opts.replSet = null;
        if (opts.arbiter)
            opts.oplogSize = 1;

        return opts;
    };

    MongoRunner.mongosOptions = function(opts) {
        opts = MongoRunner.mongoOptions(opts);

        // Normalize configdb option to be host string if currently a host
        if (opts.configdb && opts.configdb.getDB) {
            opts.configdb = opts.configdb.host;
        }

        opts.pathOpts =
            Object.merge(opts.pathOpts, {configdb: opts.configdb.replace(/:|\/|,/g, "-")});

        if (!opts.logFile && opts.useLogFiles) {
            opts.logFile =
                MongoRunner.toRealFile("$dataDir/mongos-$configdb-$port.log", opts.pathOpts);
        } else if (opts.logFile) {
            opts.logFile = MongoRunner.toRealFile(opts.logFile, opts.pathOpts);
        }

        if (opts.logFile !== undefined) {
            opts.logpath = opts.logFile;
        }

        var testOptions = jsTestOptions();
        if (testOptions.keyFile && !opts.keyFile) {
            opts.keyFile = testOptions.keyFile;
        }

        if (opts.hasOwnProperty("auditDestination")) {
            // opts.auditDestination, if set, must be a string
            if (typeof opts.auditDestination !== "string") {
                throw new Error("The auditDestination option must be a string if it is specified");
            }
        } else if (testOptions.auditDestination !== undefined) {
            if (typeof(testOptions.auditDestination) !== "string") {
                throw new Error("The auditDestination option must be a string if it is specified");
            }
            opts.auditDestination = testOptions.auditDestination;
        }

        if (!opts.hasOwnProperty('binVersion') && testOptions.mongosBinVersion) {
            opts.binVersion = MongoRunner.getBinVersionFor(testOptions.mongosBinVersion);
        }

        return opts;
    };

    /**
     * Starts a mongod instance.
     *
     * @param {Object} opts
     *
     *   {
     *     useHostName {boolean}: Uses hostname of machine if true.
     *     forceLock {boolean}: Deletes the lock file if set to true.
     *     dbpath {string}: location of db files.
     *     cleanData {boolean}: Removes all files in dbpath if true.
     *     startClean {boolean}: same as cleanData.
     *     noCleanData {boolean}: Do not clean files (cleanData takes priority).
     *     binVersion {string}: version for binary (also see MongoRunner.binVersionSubs).
     *
     *     @see MongoRunner.mongodOptions for other options
     *   }
     *
     * @return {Mongo} connection object to the started mongod instance.
     *
     * @see MongoRunner.arrOptions
     */
    MongoRunner.runMongod = function(opts) {

        opts = opts || {};
        var env = undefined;
        var useHostName = true;
        var runId = null;
        var waitForConnect = true;
        var fullOptions = opts;

        if (isObject(opts)) {
            opts = MongoRunner.mongodOptions(opts);
            fullOptions = opts;

            if (opts.useHostName != undefined) {
                useHostName = opts.useHostName;
            } else if (opts.useHostname != undefined) {
                useHostName = opts.useHostname;
            } else {
                useHostName = true;  // Default to true
            }
            env = opts.env;
            runId = opts.runId;
            waitForConnect = opts.waitForConnect;

            if (opts.forceLock)
                removeFile(opts.dbpath + "/mongod.lock");
            if ((opts.cleanData || opts.startClean) || (!opts.restart && !opts.noCleanData)) {
                print("Resetting db path '" + opts.dbpath + "'");
                resetDbpath(opts.dbpath);
            }

            opts = MongoRunner.arrOptions("mongod", opts);
        }

        var mongod = MongoRunner._startWithArgs(opts, env, waitForConnect);
        if (!mongod) {
            return null;
        }

        mongod.commandLine = MongoRunner.arrToOpts(opts);
        mongod.name = (useHostName ? getHostName() : "localhost") + ":" + mongod.commandLine.port;
        mongod.host = mongod.name;
        mongod.port = parseInt(mongod.commandLine.port);
        mongod.runId = runId || ObjectId();
        mongod.dbpath = fullOptions.dbpath;
        mongod.savedOptions = MongoRunner.savedOptions[mongod.runId];
        mongod.fullOptions = fullOptions;

        return mongod;
    };

    MongoRunner.runMongos = function(opts) {
        opts = opts || {};

        var env = undefined;
        var useHostName = false;
        var runId = null;
        var waitForConnect = true;
        var fullOptions = opts;

        if (isObject(opts)) {
            opts = MongoRunner.mongosOptions(opts);
            fullOptions = opts;

            useHostName = opts.useHostName || opts.useHostname;
            runId = opts.runId;
            waitForConnect = opts.waitForConnect;
            env = opts.env;

            opts = MongoRunner.arrOptions("mongos", opts);
        }

        var mongos = MongoRunner._startWithArgs(opts, env, waitForConnect);
        if (!mongos) {
            return null;
        }

        mongos.commandLine = MongoRunner.arrToOpts(opts);
        mongos.name = (useHostName ? getHostName() : "localhost") + ":" + mongos.commandLine.port;
        mongos.host = mongos.name;
        mongos.port = parseInt(mongos.commandLine.port);
        mongos.runId = runId || ObjectId();
        mongos.savedOptions = MongoRunner.savedOptions[mongos.runId];
        mongos.fullOptions = fullOptions;

        return mongos;
    };

    MongoRunner.StopError = function(message, returnCode) {
        this.name = "StopError";
        this.returnCode = returnCode || "non-zero";
        this.message = message || "MongoDB process stopped with exit code: " + this.returnCode;
        this.stack = this.toString() + "\n" + (new Error()).stack;
    };

    MongoRunner.StopError.prototype = Object.create(Error.prototype);
    MongoRunner.StopError.prototype.constructor = MongoRunner.StopError;

    // Constants for exit codes of MongoDB processes
    MongoRunner.EXIT_CLEAN = 0;
    MongoRunner.EXIT_BADOPTIONS = 2;
    MongoRunner.EXIT_REPLICATION_ERROR = 3;
    MongoRunner.EXIT_NEED_UPGRADE = 4;
    MongoRunner.EXIT_SHARDING_ERROR = 5;
    MongoRunner.EXIT_KILL = 12;
    MongoRunner.EXIT_ABRUPT = 14;
    MongoRunner.EXIT_NTSERVICE_ERROR = 20;
    MongoRunner.EXIT_JAVA = 21;
    MongoRunner.EXIT_OOM_MALLOC = 42;
    MongoRunner.EXIT_OOM_REALLOC = 43;
    MongoRunner.EXIT_FS = 45;
    MongoRunner.EXIT_CLOCK_SKEW = 47;  // OpTime clock skew; deprecated
    MongoRunner.EXIT_NET_ERROR = 48;
    MongoRunner.EXIT_WINDOWS_SERVICE_STOP = 49;
    MongoRunner.EXIT_POSSIBLE_CORRUPTION = 60;
    MongoRunner.EXIT_UNCAUGHT = 100;  // top level exception that wasn't caught
    MongoRunner.EXIT_TEST = 101;

    /**
     * Kills a mongod process.
     *
     * @param {number} port the port of the process to kill
     * @param {number} signal The signal number to use for killing
     * @param {Object} opts Additional options. Format:
     *    {
     *      auth: {
     *        user {string}: admin user name
     *        pwd {string}: admin password
     *      }
     *    }
     *
     * Note: The auth option is required in a authenticated mongod running in Windows since
     *  it uses the shutdown command, which requires admin credentials.
     */
    MongoRunner.stopMongod = function(port, signal, opts) {

        if (!port) {
            print("Cannot stop mongo process " + port);
            return null;
        }

        signal = parseInt(signal) || 15;
        opts = opts || {};

        var allowedExitCodes = [MongoRunner.EXIT_CLEAN];

        if (_isWindows()) {
            // Return code of processes killed with TerminateProcess on Windows
            allowedExitCodes.push(1);
        } else {
            // Return code of processes killed with SIGKILL on POSIX systems
            allowedExitCodes.push(-9);
        }

        if (opts.allowedExitCodes) {
            allowedExitCodes = allowedExitCodes.concat(opts.allowedExitCodes);
        }

        if (port.port)
            port = parseInt(port.port);

        if (port instanceof ObjectId) {
            var opts = MongoRunner.savedOptions(port);
            if (opts)
                port = parseInt(opts.port);
        }

        var returnCode = _stopMongoProgram(parseInt(port), signal, opts);

        if (!Array.contains(allowedExitCodes, returnCode)) {
            throw new MongoRunner.StopError(
            // clang-format off
            `MongoDB process on port ${port} exited with error code ${returnCode}`,
            // clang-format on
            returnCode
        );
        }

        return returnCode;
    };

    MongoRunner.stopMongos = MongoRunner.stopMongod;

    /**
     * Starts an instance of the specified mongo tool
     *
     * @param {String} binaryName The name of the tool to run
     * @param {Object} opts options to pass to the tool
     *    {
     *      binVersion {string}: version of tool to run
     *    }
     *
     * @see MongoRunner.arrOptions
     */
    MongoRunner.runMongoTool = function(binaryName, opts) {

        var opts = opts || {};
        // Normalize and get the binary version to use
        opts.binVersion = MongoRunner.getBinVersionFor(opts.binVersion);

        // Recent versions of the mongo tools support a --dialTimeout flag to set for how
        // long they retry connecting to a mongod or mongos process. We have them retry
        // connecting for up to 30 seconds to handle when the tests are run on a
        // resource-constrained host machine.
        if (!opts.hasOwnProperty('dialTimeout') &&
            MongoRunner.getBinVersionFor(opts.binVersion) === '') {
            opts['dialTimeout'] = '30';
        }

        var argsArray = MongoRunner.arrOptions(binaryName, opts);

        return runMongoProgram.apply(null, argsArray);

    };

    // Given a test name figures out a directory for that test to use for dump files and makes sure
    // that directory exists and is empty.
    MongoRunner.getAndPrepareDumpDirectory = function(testName) {
        var dir = MongoRunner.dataPath + testName + "_external/";
        resetDbpath(dir);
        return dir;
    };

    // Start a mongod instance and return a 'Mongo' object connected to it.
    // This function's arguments are passed as command line arguments to mongod.
    // The specified 'dbpath' is cleared if it exists, created if not.
    // var conn = _startMongodEmpty("--port", 30000, "--dbpath", "asdf");
    var _startMongodEmpty = function() {
        var args = createMongoArgs("mongod", Array.from(arguments));

        var dbpath = _parsePath.apply(null, args);
        resetDbpath(dbpath);

        return startMongoProgram.apply(null, args);
    };

    _startMongod = function() {
        print("startMongod WARNING DELETES DATA DIRECTORY THIS IS FOR TESTING ONLY");
        return _startMongodEmpty.apply(null, arguments);
    };

    /**
     * Returns a new argArray with any test-specific arguments added.
     */
    function appendSetParameterArgs(argArray) {
        var programName = argArray[0];
        if (programName.endsWith('mongod') || programName.endsWith('mongos') ||
            programName.startsWith('mongod-') || programName.startsWith('mongos-')) {
            if (jsTest.options().enableTestCommands) {
                argArray.push(...['--setParameter', "enableTestCommands=1"]);
            }
            if (jsTest.options().authMechanism && jsTest.options().authMechanism != "SCRAM-SHA-1") {
                var hasAuthMechs = false;
                for (var i in argArray) {
                    if (typeof argArray[i] === 'string' &&
                        argArray[i].indexOf('authenticationMechanisms') != -1) {
                        hasAuthMechs = true;
                        break;
                    }
                }
                if (!hasAuthMechs) {
                    argArray.push(...[
                        '--setParameter',
                        "authenticationMechanisms=" + jsTest.options().authMechanism
                    ]);
                }
            }
            if (jsTest.options().auth) {
                argArray.push(...['--setParameter', "enableLocalhostAuthBypass=false"]);
            }

            // mongos only options. Note: excludes mongos with version suffix (ie. mongos-3.0).
            if (programName.endsWith('mongos')) {
                // apply setParameters for mongos
                if (jsTest.options().setParametersMongos) {
                    var params = jsTest.options().setParametersMongos.split(",");
                    if (params && params.length > 0) {
                        params.forEach(function(p) {
                            if (p)
                                argArray.push(...['--setParameter', p]);
                        });
                    }
                }
            }
            // mongod only options. Note: excludes mongos with version suffix (ie. mongos-3.0).
            else if (programName.endsWith('mongod')) {
                // set storageEngine for mongod
                if (jsTest.options().storageEngine) {
                    if (argArray.indexOf("--storageEngine") < 0) {
                        argArray.push(...['--storageEngine', jsTest.options().storageEngine]);
                    }
                }
                if (jsTest.options().wiredTigerEngineConfigString) {
                    argArray.push(...[
                        '--wiredTigerEngineConfigString',
                        jsTest.options().wiredTigerEngineConfigString
                    ]);
                }
                if (jsTest.options().wiredTigerCollectionConfigString) {
                    argArray.push(...[
                        '--wiredTigerCollectionConfigString',
                        jsTest.options().wiredTigerCollectionConfigString
                    ]);
                }
                if (jsTest.options().wiredTigerIndexConfigString) {
                    argArray.push(...[
                        '--wiredTigerIndexConfigString',
                        jsTest.options().wiredTigerIndexConfigString
                    ]);
                }
                // apply setParameters for mongod
                if (jsTest.options().setParameters) {
                    var params = jsTest.options().setParameters.split(",");
                    if (params && params.length > 0) {
                        params.forEach(function(p) {
                            if (p)
                                argArray.push(...['--setParameter', p]);
                        });
                    }
                }
            }
        }
        return argArray;
    }

    /**
     * Start a mongo process with a particular argument array.
     * If we aren't waiting for connect, return {pid: <pid>}.
     * If we are not waiting for connect:
     *     returns connection to process on success;
     *     otherwise returns null if we fail to connect.
     */
    MongoRunner._startWithArgs = function(argArray, env, waitForConnect) {
        // TODO: Make there only be one codepath for starting mongo processes

        argArray = appendSetParameterArgs(argArray);
        var port = _parsePort.apply(null, argArray);
        var pid = -1;
        if (env === undefined) {
            pid = _startMongoProgram.apply(null, argArray);
        } else {
            pid = _startMongoProgram({args: argArray, env: env});
        }

        if (!waitForConnect) {
            return {
                pid: pid,
            };
        }

        var conn = null;
        assert.soon(function() {
            try {
                conn = new Mongo("127.0.0.1:" + port);
                conn.pid = pid;
                return true;
            } catch (e) {
                if (!checkProgram(pid)) {
                    print("Could not start mongo program at " + port + ", process ended");

                    // Break out
                    return true;
                }
            }
            return false;
        }, "unable to connect to mongo program on port " + port, 600 * 1000);

        return conn;
    };

    /**
     * DEPRECATED
     *
     * Start mongod or mongos and return a Mongo() object connected to there.
     * This function's first argument is "mongod" or "mongos" program name, \
     * and subsequent arguments to this function are passed as
     * command line arguments to the program.
     */
    startMongoProgram = function() {
        var port = _parsePort.apply(null, arguments);

        // Enable test commands.
        // TODO: Make this work better with multi-version testing so that we can support
        // enabling this on 2.4 when testing 2.6
        var args = Array.from(arguments);
        args = appendSetParameterArgs(args);
        var pid = _startMongoProgram.apply(null, args);

        var m;
        assert.soon(function() {
            try {
                m = new Mongo("127.0.0.1:" + port);
                return true;
            } catch (e) {
                if (!checkProgram(pid)) {
                    print("Could not start mongo program at " + port + ", process ended");

                    // Break out
                    m = null;
                    return true;
                }
            }
            return false;
        }, "unable to connect to mongo program on port " + port, 600 * 1000);

        return m;
    };

    runMongoProgram = function() {
        var args = Array.from(arguments);
        args = appendSetParameterArgs(args);
        var progName = args[0];

        if (jsTestOptions().auth) {
            args = args.slice(1);
            args.unshift(progName,
                         '-u',
                         jsTestOptions().authUser,
                         '-p',
                         jsTestOptions().authPassword,
                         '--authenticationDatabase=admin');
        }

        if (progName == 'mongo' && !_useWriteCommandsDefault()) {
            progName = args[0];
            args = args.slice(1);
            args.unshift(progName, '--useLegacyWriteOps');
        }

        return _runMongoProgram.apply(null, args);
    };

    // Start a mongo program instance.  This function's first argument is the
    // program name, and subsequent arguments to this function are passed as
    // command line arguments to the program.  Returns pid of the spawned program.
    startMongoProgramNoConnect = function() {
        var args = Array.from(arguments);
        args = appendSetParameterArgs(args);
        var progName = args[0];

        if (jsTestOptions().auth) {
            args = args.slice(1);
            args.unshift(progName,
                         '-u',
                         jsTestOptions().authUser,
                         '-p',
                         jsTestOptions().authPassword,
                         '--authenticationDatabase=admin');
        }

        if (progName == 'mongo' && !_useWriteCommandsDefault()) {
            args = args.slice(1);
            args.unshift(progName, '--useLegacyWriteOps');
        }

        return _startMongoProgram.apply(null, args);
    };

    myPort = function() {
        var m = db.getMongo();
        if (m.host.match(/:/))
            return m.host.match(/:(.*)/)[1];
        else
            return 27017;
    };

}());



// ---- MODULE: servers_misc ---- 
ToolTest = function(name, extraOptions) {
    this.name = name;
    this.options = extraOptions;
    this.port = allocatePort();
    this.baseName = "jstests_tool_" + name;
    this.root = MongoRunner.dataPath + this.baseName;
    this.dbpath = this.root + "/";
    this.ext = this.root + "_external/";
    this.extFile = this.root + "_external/a";
    resetDbpath(this.dbpath);
    resetDbpath(this.ext);
};

ToolTest.prototype.startDB = function(coll) {
    assert(!this.m, "db already running");

    var options = {
        port: this.port,
        dbpath: this.dbpath,
        nohttpinterface: "",
        noprealloc: "",
        smallfiles: "",
        bind_ip: "127.0.0.1"
    };

    Object.extend(options, this.options);

    this.m = startMongoProgram.apply(null, MongoRunner.arrOptions("mongod", options));
    this.db = this.m.getDB(this.baseName);
    if (coll)
        return this.db.getCollection(coll);
    return this.db;
};

ToolTest.prototype.stop = function() {
    if (!this.m)
        return;
    _stopMongoProgram(this.port);
    this.m = null;
    this.db = null;

    print('*** ' + this.name + " completed successfully ***");
};

ToolTest.prototype.runTool = function() {
    var a = ["mongo" + arguments[0]];

    var hasdbpath = false;

    for (var i = 1; i < arguments.length; i++) {
        a.push(arguments[i]);
        if (arguments[i] == "--dbpath")
            hasdbpath = true;
    }

    if (!hasdbpath) {
        a.push("--host");
        a.push("127.0.0.1:" + this.port);
    }

    return runMongoProgram.apply(null, a);
};

ReplTest = function(name, ports) {
    this.name = name;
    this.ports = ports || allocatePorts(2);
};

ReplTest.prototype.getPort = function(master) {
    if (master)
        return this.ports[0];
    return this.ports[1];
};

ReplTest.prototype.getPath = function(master) {
    var p = MongoRunner.dataPath + this.name + "-";
    if (master)
        p += "master";
    else
        p += "slave";
    return p;
};

ReplTest.prototype.getOptions = function(master, extra, putBinaryFirst, norepl) {

    if (!extra)
        extra = {};

    if (!extra.oplogSize)
        extra.oplogSize = "40";

    var a = [];
    if (putBinaryFirst)
        a.push("mongod");
    a.push("--nohttpinterface", "--noprealloc", "--bind_ip", "127.0.0.1", "--smallfiles");

    a.push("--port");
    a.push(this.getPort(master));

    a.push("--dbpath");
    a.push(this.getPath(master));

    if (jsTestOptions().noJournal && !('journal' in extra))
        a.push("--nojournal");
    if (jsTestOptions().noJournalPrealloc)
        a.push("--nopreallocj");
    if (jsTestOptions().keyFile) {
        a.push("--keyFile");
        a.push(jsTestOptions().keyFile);
    }

    if (!norepl) {
        if (master) {
            a.push("--master");
        } else {
            a.push("--slave");
            a.push("--source");
            a.push("127.0.0.1:" + this.ports[0]);
        }
    }

    for (var k in extra) {
        var v = extra[k];
        if (k in MongoRunner.logicalOptions)
            continue;
        a.push("--" + k);
        if (v != null && v !== "")
            a.push(v);
    }

    return a;
};

ReplTest.prototype.start = function(master, options, restart, norepl) {
    var lockFile = this.getPath(master) + "/mongod.lock";
    removeFile(lockFile);
    var o = this.getOptions(master, options, restart, norepl);

    if (restart) {
        var conn = startMongoProgram.apply(null, o);
        if (!master) {
            conn.setSlaveOk();
        }
        return conn;
    } else {
        var conn = _startMongod.apply(null, o);
        if (jsTestOptions().keyFile || jsTestOptions().auth) {
            jsTest.authenticate(conn);
        }
        if (!master) {
            conn.setSlaveOk();
        }
        return conn;
    }
};

ReplTest.prototype.stop = function(master, signal) {
    if (arguments.length == 0) {
        this.stop(true);
        this.stop(false);
        return;
    }

    print('*** ' + this.name + " completed successfully ***");
    return _stopMongoProgram(this.getPort(master), signal || 15);
};

/**
 * Returns a port number that has not been given out to any other caller from the same mongo shell.
 */
allocatePort = (function() {
    // Defer initializing these variables until the first call, as TestData attributes may be
    // initialized as part of the --eval argument (e.g. by resmoke.py), which will not be evaluated
    // until after this has loaded.
    var maxPort;
    var nextPort;

    return function() {
        // The default port was chosen in an attempt to have a large number of unassigned ports that
        // are also outside the ephemeral port range.
        nextPort = nextPort || jsTestOptions().minPort || 20000;
        maxPort = maxPort || jsTestOptions().maxPort || Math.pow(2, 16) - 1;

        if (nextPort === maxPort) {
            throw new Error("Exceeded maximum port range in allocatePort()");
        }
        return nextPort++;
    };
})();

/**
 * Returns a list of 'numPorts' port numbers that have not been given out to any other caller from
 * the same mongo shell.
 */
allocatePorts = function(numPorts) {
    var ports = [];
    for (var i = 0; i < numPorts; i++) {
        ports.push(allocatePort());
    }

    return ports;
};

function startParallelShell(jsCode, port, noConnect) {
    var args = ["mongo"];

    if (typeof db == "object") {
        var hostAndPort = db.getMongo().host.split(':');
        var host = hostAndPort[0];
        args.push("--host", host);
        if (!port && hostAndPort.length >= 2) {
            var port = hostAndPort[1];
        }
    }
    if (port) {
        args.push("--port", port);
    }

    // Convert function into call-string
    if (typeof(jsCode) == "function") {
        jsCode = "(" + jsCode.toString() + ")();";
    } else if (typeof(jsCode) == "string") {
    }
    // do nothing
    else {
        throw Error("bad first argument to startParallelShell");
    }

    if (noConnect) {
        args.push("--nodb");
    } else if (typeof(db) == "object") {
        jsCode = "db = db.getSiblingDB('" + db.getName() + "');" + jsCode;
    }

    if (TestData) {
        jsCode = "TestData = " + tojson(TestData) + ";" + jsCode;
    }

    args.push("--eval", jsCode);

    var pid = startMongoProgramNoConnect.apply(null, args);

    // Returns a function that when called waits for the parallel shell to exit and returns the exit
    // code of the process. By default an error is thrown if the parallel shell exits with a nonzero
    // exit code.
    return function(options) {
        if (arguments.length > 0) {
            if (typeof options !== "object") {
                throw new Error("options must be an object");
            }
            if (options === null) {
                throw new Error("options cannot be null");
            }
        }
        var exitCode = waitProgram(pid);
        if (arguments.length === 0 || options.checkExitSuccess) {
            assert.eq(0, exitCode, "encountered an error in the parallel shell");
        }
        return exitCode;
    };
}

var testingReplication = false;



// ---- MODULE: shardingtest ---- 
/**
 * Starts up a sharded cluster with the given specifications. The cluster
 * will be fully operational after the execution of this constructor function.
 *
 * @param {Object} params Contains the key-value pairs for the cluster
 *   configuration. Accepted keys are:
 *
 *   {
 *     name {string}: name for this test
 *     verbose {number}: the verbosity for the mongos
 *     keyFile {string}: the location of the keyFile
 *     chunkSize {number}: the chunk size to use as configuration for the cluster
 *     nopreallocj {boolean|number}:
 *
 *     mongos {number|Object|Array.<Object>}: number of mongos or mongos
 *       configuration object(s)(*). @see MongoRunner.runMongos
 *
 *     rs {Object|Array.<Object>}: replica set configuration object. Can
 *       contain:
 *       {
 *         nodes {number}: number of replica members. Defaults to 3.
 *         protocolVersion {number}: protocol version of replset used by the
 *             replset initiation.
 *         initiateTimeout {number}: timeout in milliseconds to specify
 *              to ReplSetTest.prototype.initiate().
 *         For other options, @see ReplSetTest#start
 *       }
 *
 *     shards {number|Object|Array.<Object>}: number of shards or shard
 *       configuration object(s)(*). @see MongoRunner.runMongod
 *
 *     config {number|Object|Array.<Object>}: number of config server or
 *       config server configuration object(s)(*). @see MongoRunner.runMongod
 *
 *     (*) There are two ways For multiple configuration objects.
 *       (1) Using the object format. Example:
 *
 *           { d0: { verbose: 5 }, d1: { auth: '' }, rs2: { oplogsize: 10 }}
 *
 *           In this format, d = mongod, s = mongos & c = config servers
 *
 *       (2) Using the array format. Example:
 *
 *           [{ verbose: 5 }, { auth: '' }]
 *
 *       Note: you can only have single server shards for array format.
 *
 *       Note: A special "bridgeOptions" property can be specified in both the object and array
 *          formats to configure the options for the mongobridge corresponding to that node. These
 *          options are merged with the params.bridgeOptions options, where the node-specific
 *          options take precedence.
 *
 *     other: {
 *       nopreallocj: same as above
 *       rs: same as above
 *       chunkSize: same as above
 *
 *       shardOptions {Object}: same as the shards property above.
 *          Can be used to specify options that are common all shards.
 *
 *       configOptions {Object}: same as the config property above.
 *          Can be used to specify options that are common all config servers.
 *       mongosOptions {Object}: same as the mongos property above.
 *          Can be used to specify options that are common all mongos.
 *       enableBalancer {boolean} : if true, enable the balancer
 *       manualAddShard {boolean}: shards will not be added if true.
 *
 *       useBridge {boolean}: If true, then a mongobridge process is started for each node in the
 *          sharded cluster. Defaults to false.
 *
 *       bridgeOptions {Object}: Options to apply to all mongobridge processes. Defaults to {}.
 *
 *       // replica Set only:
 *       rsOptions {Object}: same as the rs property above. Can be used to
 *         specify options that are common all replica members.
 *       useHostname {boolean}: if true, use hostname of machine,
 *         otherwise use localhost
 *       numReplicas {number}
 *     }
 *   }
 *
 * Member variables:
 * s {Mongo} - connection to the first mongos
 * s0, s1, ... {Mongo} - connection to different mongos
 * rs0, rs1, ... {ReplSetTest} - test objects to replica sets
 * shard0, shard1, ... {Mongo} - connection to shards (not available for replica sets)
 * d0, d1, ... {Mongo} - same as shard0, shard1, ...
 * config0, config1, ... {Mongo} - connection to config servers
 * c0, c1, ... {Mongo} - same as config0, config1, ...
 * configRS - If the config servers are a replset, this will contain the config ReplSetTest object
 */
var ShardingTest = function(params) {

    if (!(this instanceof ShardingTest)) {
        return new ShardingTest(params);
    }

    // Capture the 'this' reference
    var self = this;

    // Used for counting the test duration
    var _startTime = new Date();

    // Populated with the paths of all shard hosts (config servers + hosts) and is used for
    // cleaning up the data files on shutdown
    var _alldbpaths = [];

    // Publicly exposed variables

    /**
     * Attempts to open a connection to the specified connection string or throws if unable to
     * connect.
     */
    function _connectWithRetry(url) {
        var conn;
        assert.soon(function() {
            try {
                conn = new Mongo(url);
                return true;
            } catch (e) {
                print("Error connecting to " + url + ": " + e);
                return false;
            }
        });

        return conn;
    }

    /**
     * Constructs a human-readable string representing a chunk's range.
     */
    function _rangeToString(r) {
        return tojsononeline(r.min) + " -> " + tojsononeline(r.max);
    }

    /**
     * Checks whether the specified collection is sharded by consulting the config metadata.
     */
    function _isSharded(collName) {
        var collName = "" + collName;
        var dbName;

        if (typeof collName.getCollectionNames == 'function') {
            dbName = "" + collName;
            collName = undefined;
        }

        if (dbName) {
            var x = self.config.databases.findOne({_id: dbname});
            if (x)
                return x.partitioned;
            else
                return false;
        }

        if (collName) {
            var x = self.config.collections.findOne({_id: collName});
            if (x)
                return true;
            else
                return false;
        }
    }

    function connectionURLTheSame(a, b) {
        if (a == b)
            return true;

        if (!a || !b)
            return false;

        if (a.host)
            return connectionURLTheSame(a.host, b);
        if (b.host)
            return connectionURLTheSame(a, b.host);

        if (a.name)
            return connectionURLTheSame(a.name, b);
        if (b.name)
            return connectionURLTheSame(a, b.name);

        if (a.indexOf("/") < 0 && b.indexOf("/") < 0) {
            a = a.split(":");
            b = b.split(":");

            if (a.length != b.length)
                return false;

            if (a.length == 2 && a[1] != b[1])
                return false;

            if (a[0] == "localhost" || a[0] == "127.0.0.1")
                a[0] = getHostName();
            if (b[0] == "localhost" || b[0] == "127.0.0.1")
                b[0] = getHostName();

            return a[0] == b[0];
        } else {
            var a0 = a.split("/")[0];
            var b0 = b.split("/")[0];
            return a0 == b0;
        }
    }

    assert(connectionURLTheSame("foo", "foo"));
    assert(!connectionURLTheSame("foo", "bar"));

    assert(connectionURLTheSame("foo/a,b", "foo/b,a"));
    assert(!connectionURLTheSame("foo/a,b", "bar/a,b"));

    // ShardingTest API

    this.getRSEntry = function(setName) {
        for (var i = 0; i < this._rs.length; i++)
            if (this._rs[i].setName == setName)
                return this._rs[i];
        throw Error("can't find rs: " + setName);
    };

    this.getDB = function(name) {
        return this.s.getDB(name);
    };

    /*
     * Finds the _id of the primary shard for database 'dbname', e.g., 'test-rs0'
     */
    this.getPrimaryShardIdForDatabase = function(dbname) {
        var x = this.config.databases.findOne({_id: "" + dbname});
        if (x) {
            return x.primary;
        }

        var countDBsFound = 0;
        this.config.databases.find().forEach(function(db) {
            countDBsFound++;
            printjson(db);
        });
        throw Error("couldn't find dbname: " + dbname + " in config.databases. Total DBs: " +
                    countDBsFound);
    };

    this.getNonPrimaries = function(dbname) {
        var x = this.config.databases.findOne({_id: dbname});
        if (!x) {
            this.config.databases.find().forEach(printjson);
            throw Error("couldn't find dbname: " + dbname + " total: " +
                        this.config.databases.count());
        }

        return this.config.shards.find({_id: {$ne: x.primary}}).map(z => z._id);
    };

    this.getConnNames = function() {
        var names = [];
        for (var i = 0; i < this._connections.length; i++) {
            names.push(this._connections[i].name);
        }
        return names;
    };

    /*
     * Find the connection to the primary shard for database 'dbname'.
     */
    this.getPrimaryShard = function(dbname) {
        var dbPrimaryShardId = this.getPrimaryShardIdForDatabase(dbname);
        var primaryShard = this.config.shards.findOne({_id: dbPrimaryShardId});

        if (primaryShard) {
            shardConnectionString = primaryShard.host;
            var rsName = shardConnectionString.substring(0, shardConnectionString.indexOf("/"));

            for (var i = 0; i < this._connections.length; i++) {
                var c = this._connections[i];
                if (connectionURLTheSame(shardConnectionString, c.name) ||
                    connectionURLTheSame(rsName, c.name))
                    return c;
            }
        }

        throw Error("can't find server connection for db '" + dbname + "'s primary shard: " +
                    tojson(primaryShard));
    };

    this.normalize = function(x) {
        var z = this.config.shards.findOne({host: x});
        if (z)
            return z._id;
        return x;
    };

    /*
     * Find a different shard connection than the one given.
     */
    this.getOther = function(one) {
        if (this._connections.length < 2) {
            throw Error("getOther only works with 2 shards");
        }

        if (one._mongo) {
            one = one._mongo;
        }

        for (var i = 0; i < this._connections.length; i++) {
            if (this._connections[i] != one) {
                return this._connections[i];
            }
        }

        return null;
    };

    this.getAnother = function(one) {
        if (this._connections.length < 2) {
            throw Error("getAnother() only works with multiple servers");
        }

        if (one._mongo) {
            one = one._mongo;
        }

        for (var i = 0; i < this._connections.length; i++) {
            if (this._connections[i] == one)
                return this._connections[(i + 1) % this._connections.length];
        }
    };

    this.getFirstOther = function(one) {
        for (var i = 0; i < this._connections.length; i++) {
            if (this._connections[i] != one) {
                return this._connections[i];
            }
        }

        throw Error("impossible");
    };

    this.stop = function(opts) {
        for (var i = 0; i < this._mongos.length; i++) {
            this.stopMongos(i, opts);
        }

        for (var i = 0; i < this._connections.length; i++) {
            if (this._rs[i]) {
                this._rs[i].test.stopSet(15, undefined, opts);
            } else {
                this.stopMongod(i, opts);
            }
        }

        if (this.configRS) {
            this.configRS.stopSet(undefined, undefined, opts);
        } else {
            // Old style config triplet
            for (var i = 0; i < this._configServers.length; i++) {
                this.stopConfigServer(i, opts);
            }
        }

        for (var i = 0; i < _alldbpaths.length; i++) {
            resetDbpath(MongoRunner.dataPath + _alldbpaths[i]);
        }

        var timeMillis = new Date().getTime() - _startTime.getTime();

        print('*** ShardingTest ' + this._testName + " completed successfully in " +
              (timeMillis / 1000) + " seconds ***");
    };

    this.getDBPaths = function() {
        return _alldbpaths.map((path) => {
            return MongoRunner.dataPath + path;
        });
    };

    this.adminCommand = function(cmd) {
        var res = this.admin.runCommand(cmd);
        if (res && res.ok == 1)
            return true;

        throw _getErrorWithCode(res, "command " + tojson(cmd) + " failed: " + tojson(res));
    };

    this.printChangeLog = function() {
        this.config.changelog.find().forEach(function(z) {
            var msg = z.server + "\t" + z.time + "\t" + z.what;
            for (var i = z.what.length; i < 15; i++)
                msg += " ";

            msg += " " + z.ns + "\t";
            if (z.what == "split") {
                msg += _rangeToString(z.details.before) + " -->> (" +
                    _rangeToString(z.details.left) + "), (" + _rangeToString(z.details.right) + ")";
            } else if (z.what == "multi-split") {
                msg += _rangeToString(z.details.before) + "  -->> (" + z.details.number + "/" +
                    z.details.of + " " + _rangeToString(z.details.chunk) + ")";
            } else {
                msg += tojsononeline(z.details);
            }

            print("ShardingTest " + msg);
        });
    };

    this.getChunksString = function(ns) {
        var q = {};
        if (ns) {
            q.ns = ns;
        }

        var s = "";
        this.config.chunks.find(q)
            .sort({ns: 1, min: 1})
            .forEach(function(z) {
                s += "  " + z._id + "\t" + z.lastmod.t + "|" + z.lastmod.i + "\t" + tojson(z.min) +
                    " -> " + tojson(z.max) + " " + z.shard + "  " + z.ns + "\n";
            });

        return s;
    };

    this.printChunks = function(ns) {
        print("ShardingTest " + this.getChunksString(ns));
    };

    this.printShardingStatus = function(verbose) {
        printShardingStatus(this.config, verbose);
    };

    this.printCollectionInfo = function(ns, msg) {
        var out = "";
        if (msg) {
            out += msg + "\n";
        }
        out += "sharding collection info: " + ns + "\n";

        for (var i = 0; i < this._connections.length; i++) {
            var c = this._connections[i];
            out += "  mongod " + c + " " +
                tojson(c.getCollection(ns).getShardVersion(), " ", true) + "\n";
        }

        for (var i = 0; i < this._mongos.length; i++) {
            var c = this._mongos[i];
            out += "  mongos " + c + " " +
                tojson(c.getCollection(ns).getShardVersion(), " ", true) + "\n";
        }

        out += this.getChunksString(ns);

        print("ShardingTest " + out);
    };

    this.sync = function() {
        this.adminCommand("connpoolsync");
    };

    this.onNumShards = function(collName, dbName) {
        dbName = dbName || "test";

        // We should sync since we're going directly to mongod here
        this.sync();

        var num = 0;
        for (var i = 0; i < this._connections.length; i++) {
            if (this._connections[i].getDB(dbName).getCollection(collName).count() > 0) {
                num++;
            }
        }

        return num;
    };

    this.shardCounts = function(collName, dbName) {
        dbName = dbName || "test";

        // We should sync since we're going directly to mongod here
        this.sync();

        var counts = {};
        for (var i = 0; i < this._connections.length; i++) {
            counts[i] = this._connections[i].getDB(dbName).getCollection(collName).count();
        }

        return counts;
    };

    this.chunkCounts = function(collName, dbName) {
        dbName = dbName || "test";

        var x = {};
        this.config.shards.find().forEach(function(z) {
            x[z._id] = 0;
        });

        this.config.chunks.find({ns: dbName + "." + collName})
            .forEach(function(z) {
                if (x[z.shard])
                    x[z.shard]++;
                else
                    x[z.shard] = 1;
            });

        return x;
    };

    this.chunkDiff = function(collName, dbName) {
        var c = this.chunkCounts(collName, dbName);

        var min = 100000000;
        var max = 0;
        for (var s in c) {
            if (c[s] < min)
                min = c[s];
            if (c[s] > max)
                max = c[s];
        }

        print("ShardingTest input: " + tojson(c) + " min: " + min + " max: " + max);
        return max - min;
    };

    /**
     * Waits up to one minute for the difference in chunks between the most loaded shard and least
     * loaded shard to be 0 or 1, indicating that the collection is well balanced. This should only
     * be called after creating a big enough chunk difference to trigger balancing.
     */
    this.awaitBalance = function(collName, dbName, timeToWait) {
        timeToWait = timeToWait || 60000;

        assert.soon(function() {
            var x = self.chunkDiff(collName, dbName);
            print("chunk diff: " + x);
            return x < 2;
        }, "no balance happened", timeToWait);
    };

    this.getShardNames = function() {
        var shards = [];
        this.s.getCollection("config.shards")
            .find()
            .forEach(function(shardDoc) {
                shards.push(shardDoc._id);
            });
        return shards;
    };

    this.getShard = function(coll, query, includeEmpty) {
        var shards = this.getShardsForQuery(coll, query, includeEmpty);
        assert.eq(shards.length, 1);
        return shards[0];
    };

    /**
     * Returns the shards on which documents matching a particular query reside.
     */
    this.getShardsForQuery = function(coll, query, includeEmpty) {
        if (!coll.getDB) {
            coll = this.s.getCollection(coll);
        }

        var explain = coll.find(query).explain("executionStats");
        var shards = [];

        var execStages = explain.executionStats.executionStages;
        var plannerShards = explain.queryPlanner.winningPlan.shards;

        if (execStages.shards) {
            for (var i = 0; i < execStages.shards.length; i++) {
                var hasResults = execStages.shards[i].executionStages.nReturned &&
                    execStages.shards[i].executionStages.nReturned > 0;
                if (includeEmpty || hasResults) {
                    shards.push(plannerShards[i].connectionString);
                }
            }
        }

        for (var i = 0; i < shards.length; i++) {
            for (var j = 0; j < this._connections.length; j++) {
                if (connectionURLTheSame(this._connections[j], shards[i])) {
                    shards[i] = this._connections[j];
                    break;
                }
            }
        }

        return shards;
    };

    this.shardColl = function(collName, key, split, move, dbName, waitForDelete) {
        split = (split != false ? (split || key) : split);
        move = (split != false && move != false ? (move || split) : false);

        if (collName.getDB)
            dbName = "" + collName.getDB();
        else
            dbName = dbName || "test";

        var c = dbName + "." + collName;
        if (collName.getDB) {
            c = "" + collName;
        }

        var isEmpty = (this.s.getCollection(c).count() == 0);

        if (!_isSharded(dbName)) {
            this.s.adminCommand({enableSharding: dbName});
        }

        var result = this.s.adminCommand({shardcollection: c, key: key});
        if (!result.ok) {
            printjson(result);
            assert(false);
        }

        if (split == false) {
            return;
        }

        result = this.s.adminCommand({split: c, middle: split});
        if (!result.ok) {
            printjson(result);
            assert(false);
        }

        if (move == false) {
            return;
        }

        var result;
        for (var i = 0; i < 5; i++) {
            var otherShard = this.getOther(this.getPrimaryShard(dbName)).name;
            result = this.s.adminCommand(
                {movechunk: c, find: move, to: otherShard, _waitForDelete: waitForDelete});
            if (result.ok)
                break;

            sleep(5 * 1000);
        }

        printjson(result);
        assert(result.ok);
    };

    this.stopBalancer = function(timeout, interval) {
        if (typeof db == "undefined") {
            db = undefined;
        }

        var oldDB = db;
        db = this.config;
        timeout = timeout || 60000;

        try {
            assert.writeOK(sh.setBalancerState(false));
            sh.waitForBalancer(false, timeout, interval);
        } finally {
            db = oldDB;
        }
    };

    this.startBalancer = function(timeout, interval) {
        if (typeof db == "undefined") {
            db = undefined;
        }

        var oldDB = db;
        db = this.config;
        timeout = timeout || 60000;

        try {
            assert.writeOK(sh.setBalancerState(true));
            sh.waitForBalancer(true, timeout, interval);
        } finally {
            db = oldDB;
        }
    };

    /*
     * Returns true after the balancer has completed a balancing round.
     *
     * Checks that three pings were sent to config.mongos. The balancer writes a ping
     * at the start and end of a balancing round. If the balancer is in the middle of
     * a round, there could be three pings before the first full balancing round
     * completes: end ping of a round, and start and end pings of the following round.
     */
    this.waitForBalancerRound = function() {
        if (typeof db == "undefined") {
            db = undefined;
        }
        var oldDB = db;
        db = this.config;

        var getPings = function() {
            return sh._getConfigDB().mongos.find().toArray();
        };

        try {
            // If sh.waitForPingChange returns a non-empty array, config.mongos
            // was not successfully updated and no balancer round was reported.
            for (var i = 0; i < 3; ++i) {
                if (sh.waitForPingChange(getPings()).length != 0) {
                    return false;
                }
            }

            db = oldDB;
            return true;
        } catch (e) {
            print("Error running waitForPingChange: " + tojson(e));
            db = oldDB;
            return false;
        }
    };

    this.isAnyBalanceInFlight = function() {
        if (this.config.locks.find({_id: {$ne: "balancer"}, state: 2}).count() > 0)
            return true;

        var allCurrent = this.s.getDB("admin").currentOp().inprog;
        for (var i = 0; i < allCurrent.length; i++) {
            if (allCurrent[i].desc && allCurrent[i].desc.indexOf("cleanupOldData") == 0)
                return true;
        }
        return false;
    };

    /**
     * Kills the mongos with index n.
     */
    this.stopMongos = function(n, opts) {
        if (otherParams.useBridge) {
            MongoRunner.stopMongos(unbridgedMongos[n], undefined, opts);
            this["s" + n].stop();
        } else {
            MongoRunner.stopMongos(this["s" + n], undefined, opts);
        }
    };

    /**
     * Kills the shard mongod with index n.
     */
    this.stopMongod = function(n, opts) {
        if (otherParams.useBridge) {
            MongoRunner.stopMongod(unbridgedConnections[n], undefined, opts);
            this["d" + n].stop();
        } else {
            MongoRunner.stopMongod(this["d" + n], undefined, opts);
        }
    };

    /**
     * Kills the config server mongod with index n.
     */
    this.stopConfigServer = function(n, opts) {
        if (otherParams.useBridge) {
            MongoRunner.stopMongod(unbridgedConfigServers[n], undefined, opts);
            this._configServers[n].stop();
        } else {
            MongoRunner.stopMongod(this._configServers[n], undefined, opts);
        }
    };

    /**
     * Stops and restarts a mongos process.
     *
     * If opts is specified, the new mongos is started using those options. Otherwise, it is started
     * with its previous parameters.
     *
     * Warning: Overwrites the old s (if n = 0) admin, config, and sn member variables.
     */
    this.restartMongos = function(n, opts) {
        var mongos;

        if (otherParams.useBridge) {
            mongos = unbridgedMongos[n];
        } else {
            mongos = this["s" + n];
        }

        opts = opts || mongos;
        opts.port = opts.port || mongos.port;

        this.stopMongos(n);

        if (otherParams.useBridge) {
            var bridgeOptions =
                (opts !== mongos) ? opts.bridgeOptions : mongos.fullOptions.bridgeOptions;
            bridgeOptions = Object.merge(otherParams.bridgeOptions, bridgeOptions || {});
            bridgeOptions = Object.merge(
                bridgeOptions,
                {
                  hostName: otherParams.useHostname ? hostName : "localhost",
                  port: this._mongos[n].port,
                  // The mongos processes identify themselves to mongobridge as host:port, where the
                  // host is the actual hostname of the machine and not localhost.
                  dest: hostName + ":" + opts.port,
                });

            this._mongos[n] = new MongoBridge(bridgeOptions);
        }

        var newConn = MongoRunner.runMongos(opts);
        if (!newConn) {
            throw new Error("Failed to restart mongos " + n);
        }

        if (otherParams.useBridge) {
            this._mongos[n].connectToBridge();
            unbridgedMongos[n] = newConn;
        } else {
            this._mongos[n] = newConn;
        }

        this['s' + n] = this._mongos[n];
        if (n == 0) {
            this.s = this._mongos[n];
            this.admin = this._mongos[n].getDB('admin');
            this.config = this._mongos[n].getDB('config');
        }
    };

    /**
     * Stops and restarts a shard mongod process.
     *
     * If opts is specified, the new mongod is started using those options. Otherwise, it is started
     * with its previous parameters. The 'beforeRestartCallback' parameter is an optional function
     * that will be run after the MongoD is stopped, but before it is restarted. The intended uses
     * of the callback are modifications to the dbpath of the mongod that must be made while it is
     * stopped.
     *
     * Warning: Overwrites the old dn/shardn member variables.
     */
    this.restartMongod = function(n, opts, beforeRestartCallback) {
        var mongod;

        if (otherParams.useBridge) {
            mongod = unbridgedConnections[n];
        } else {
            mongod = this["d" + n];
        }

        opts = opts || mongod;
        opts.port = opts.port || mongod.port;

        this.stopMongod(n);

        if (otherParams.useBridge) {
            var bridgeOptions =
                (opts !== mongod) ? opts.bridgeOptions : mongod.fullOptions.bridgeOptions;
            bridgeOptions = Object.merge(otherParams.bridgeOptions, bridgeOptions || {});
            bridgeOptions = Object.merge(
                bridgeOptions,
                {
                  hostName: otherParams.useHostname ? hostName : "localhost",
                  port: this._connections[n].port,
                  // The mongod processes identify themselves to mongobridge as host:port, where the
                  // host is the actual hostname of the machine and not localhost.
                  dest: hostName + ":" + opts.port,
                });

            this._connections[n] = new MongoBridge(bridgeOptions);
        }

        if (arguments.length >= 3) {
            if (typeof(beforeRestartCallback) !== "function") {
                throw new Error("beforeRestartCallback must be a function but was of type " +
                                typeof(beforeRestartCallback));
            }
            beforeRestartCallback();
        }

        opts.restart = true;

        var newConn = MongoRunner.runMongod(opts);
        if (!newConn) {
            throw new Error("Failed to restart shard " + n);
        }

        if (otherParams.useBridge) {
            this._connections[n].connectToBridge();
            unbridgedConnections[n] = newConn;
        } else {
            this._connections[n] = newConn;
        }

        this["shard" + n] = this._connections[n];
        this["d" + n] = this._connections[n];
    };

    /**
     * Stops and restarts a config server mongod process.
     *
     * If opts is specified, the new mongod is started using those options. Otherwise, it is started
     * with its previous parameters.
     *
     * Warning: Overwrites the old cn/confign member variables.
     */
    this.restartConfigServer = function(n) {
        var mongod;

        if (otherParams.useBridge) {
            mongod = unbridgedConfigServers[n];
        } else {
            mongod = this["c" + n];
        }

        this.stopConfigServer(n);

        if (otherParams.useBridge) {
            var bridgeOptions =
                Object.merge(otherParams.bridgeOptions, mongod.fullOptions.bridgeOptions || {});
            bridgeOptions = Object.merge(
                bridgeOptions,
                {
                  hostName: otherParams.useHostname ? hostName : "localhost",
                  port: this._configServers[n].port,
                  // The mongod processes identify themselves to mongobridge as host:port, where the
                  // host is the actual hostname of the machine and not localhost.
                  dest: hostName + ":" + mongod.port,
                });

            this._configServers[n] = new MongoBridge(bridgeOptions);
        }

        mongod.restart = true;
        var newConn = MongoRunner.runMongod(mongod);
        if (!newConn) {
            throw new Error("Failed to restart config server " + n);
        }

        if (otherParams.useBridge) {
            this._configServers[n].connectToBridge();
            unbridgedConfigServers[n] = newConn;
        } else {
            this._configServers[n] = newConn;
        }

        this["config" + n] = this._configServers[n];
        this["c" + n] = this._configServers[n];
    };

    /**
     * Helper method for setting primary shard of a database and making sure that it was successful.
     * Note: first mongos needs to be up.
     */
    this.ensurePrimaryShard = function(dbName, shardName) {
        var db = this.s0.getDB('admin');
        var res = db.adminCommand({movePrimary: dbName, to: shardName});
        assert(res.ok || res.errmsg == "it is already the primary", tojson(res));
    };

    // ShardingTest initialization

    assert(isObject(params), 'ShardingTest configuration must be a JSON object');

    var testName = params.name || "test";
    var otherParams = Object.merge(params, params.other || {});

    var numShards = otherParams.hasOwnProperty('shards') ? otherParams.shards : 2;
    var verboseLevel = otherParams.hasOwnProperty('verbose') ? otherParams.verbose : 1;
    var numMongos = otherParams.hasOwnProperty('mongos') ? otherParams.mongos : 1;
    var numConfigs = otherParams.hasOwnProperty('config') ? otherParams.config : 3;

    // Allow specifying mixed-type options like this:
    // { mongos : [ { noprealloc : "" } ],
    //   config : [ { smallfiles : "" } ],
    //   shards : { rs : true, d : true } }
    if (Array.isArray(numShards)) {
        for (var i = 0; i < numShards.length; i++) {
            otherParams["d" + i] = numShards[i];
        }

        numShards = numShards.length;
    } else if (isObject(numShards)) {
        var tempCount = 0;
        for (var i in numShards) {
            otherParams[i] = numShards[i];
            tempCount++;
        }

        numShards = tempCount;
    }

    if (Array.isArray(numMongos)) {
        for (var i = 0; i < numMongos.length; i++) {
            otherParams["s" + i] = numMongos[i];
        }

        numMongos = numMongos.length;
    } else if (isObject(numMongos)) {
        var tempCount = 0;
        for (var i in numMongos) {
            otherParams[i] = numMongos[i];
            tempCount++;
        }

        numMongos = tempCount;
    }

    if (Array.isArray(numConfigs)) {
        for (var i = 0; i < numConfigs.length; i++) {
            otherParams["c" + i] = numConfigs[i];
        }

        numConfigs = numConfigs.length;
    } else if (isObject(numConfigs)) {
        var tempCount = 0;
        for (var i in numConfigs) {
            otherParams[i] = numConfigs[i];
            tempCount++;
        }

        numConfigs = tempCount;
    }

    otherParams.extraOptions = otherParams.extraOptions || {};
    otherParams.useHostname = otherParams.useHostname == undefined ? true : otherParams.useHostname;
    otherParams.useBridge = otherParams.useBridge || false;
    otherParams.bridgeOptions = otherParams.bridgeOptions || {};

    if (otherParams.chunkSize && numMongos === 0) {
        throw Error('Cannot set chunk size without any running mongos instances');
    }

    var keyFile = otherParams.keyFile || otherParams.extraOptions.keyFile;
    var hostName = getHostName();

    this._testName = testName;
    this._otherParams = otherParams;

    var pathOpts = {
        testName: testName
    };

    for (var k in otherParams) {
        if (k.startsWith("rs") && otherParams[k] != undefined) {
            break;
        }
    }

    this._connections = [];
    this._rs = [];
    this._rsObjects = [];

    if (otherParams.useBridge) {
        var unbridgedConnections = [];
        var unbridgedConfigServers = [];
        var unbridgedMongos = [];
    }

    // Start the MongoD servers (shards)
    for (var i = 0; i < numShards; i++) {
        if (otherParams.rs || otherParams["rs" + i]) {
            var setName = testName + "-rs" + i;

            var rsDefaults = {
                useHostname: otherParams.useHostname,
                noJournalPrealloc: otherParams.nopreallocj,
                oplogSize: 16,
                shardsvr: '',
                pathOpts: Object.merge(pathOpts, {shard: i})
            };

            rsDefaults = Object.merge(rsDefaults, otherParams.rs);
            rsDefaults = Object.merge(rsDefaults, otherParams.rsOptions);
            rsDefaults = Object.merge(rsDefaults, otherParams["rs" + i]);
            rsDefaults.nodes = rsDefaults.nodes || otherParams.numReplicas;
            var rsSettings = rsDefaults.settings;
            delete rsDefaults.settings;

            var numReplicas = rsDefaults.nodes || 3;
            delete rsDefaults.nodes;

            var protocolVersion = rsDefaults.protocolVersion;
            delete rsDefaults.protocolVersion;
            var initiateTimeout = rsDefaults.initiateTimeout;
            delete rsDefaults.initiateTimeout;

            var rs = new ReplSetTest({
                name: setName,
                nodes: numReplicas,
                useHostName: otherParams.useHostname,
                useBridge: otherParams.useBridge,
                bridgeOptions: otherParams.bridgeOptions,
                keyFile: keyFile,
                protocolVersion: protocolVersion,
                settings: rsSettings
            });

            this._rs[i] = {
                setName: setName,
                test: rs,
                nodes: rs.startSet(rsDefaults),
                url: rs.getURL()
            };

            rs.initiate(null, null, initiateTimeout);

            this["rs" + i] = rs;
            this._rsObjects[i] = rs;

            _alldbpaths.push(null);
            this._connections.push(null);

            if (otherParams.useBridge) {
                unbridgedConnections.push(null);
            }
        } else {
            var options = {
                useHostname: otherParams.useHostname,
                noJournalPrealloc: otherParams.nopreallocj,
                pathOpts: Object.merge(pathOpts, {shard: i}),
                dbpath: "$testName$shard",
                shardsvr: '',
                keyFile: keyFile
            };

            if (otherParams.shardOptions && otherParams.shardOptions.binVersion) {
                otherParams.shardOptions.binVersion =
                    MongoRunner.versionIterator(otherParams.shardOptions.binVersion);
            }

            options = Object.merge(options, otherParams.shardOptions);
            options = Object.merge(options, otherParams["d" + i]);

            options.port = options.port || allocatePort();

            if (otherParams.useBridge) {
                var bridgeOptions =
                    Object.merge(otherParams.bridgeOptions, options.bridgeOptions || {});
                bridgeOptions = Object.merge(
                    bridgeOptions,
                    {
                      hostName: otherParams.useHostname ? hostName : "localhost",
                      // The mongod processes identify themselves to mongobridge as host:port, where
                      // the host is the actual hostname of the machine and not localhost.
                      dest: hostName + ":" + options.port,
                    });

                var bridge = new MongoBridge(bridgeOptions);
            }

            var conn = MongoRunner.runMongod(options);
            if (!conn) {
                throw new Error("Failed to start shard " + i);
            }

            if (otherParams.useBridge) {
                bridge.connectToBridge();
                this._connections.push(bridge);
                unbridgedConnections.push(conn);
            } else {
                this._connections.push(conn);
            }

            _alldbpaths.push(testName + i);
            this["shard" + i] = this._connections[i];
            this["d" + i] = this._connections[i];

            this._rs[i] = null;
            this._rsObjects[i] = null;
        }
    }

    // Do replication on replica sets if required
    for (var i = 0; i < numShards; i++) {
        if (!otherParams.rs && !otherParams["rs" + i]) {
            continue;
        }

        var rs = this._rs[i].test;
        rs.getPrimary().getDB("admin").foo.save({x: 1});

        if (keyFile) {
            authutil.asCluster(rs.nodes,
                               keyFile,
                               function() {
                                   rs.awaitReplication();
                               });
        }

        rs.awaitSecondaryNodes();

        var rsConn = new Mongo(rs.getURL());
        rsConn.name = rs.getURL();

        this._connections[i] = rsConn;
        this["shard" + i] = rsConn;
        rsConn.rs = rs;
    }

    this._configServers = [];

    // Using replica set for config servers
    var rstOptions = {
        useHostName: otherParams.useHostname,
        useBridge: otherParams.useBridge,
        bridgeOptions: otherParams.bridgeOptions,
        keyFile: keyFile,
        name: testName + "-configRS",
    };

    // when using CSRS, always use wiredTiger as the storage engine
    var startOptions = {
        pathOpts: pathOpts,
        // Ensure that journaling is always enabled for config servers.
        journal: "",
        configsvr: "",
        noJournalPrealloc: otherParams.nopreallocj,
        storageEngine: "wiredTiger",
    };

    if (otherParams.configOptions && otherParams.configOptions.binVersion) {
        otherParams.configOptions.binVersion =
            MongoRunner.versionIterator(otherParams.configOptions.binVersion);
    }

    startOptions = Object.merge(startOptions, otherParams.configOptions);
    rstOptions = Object.merge(rstOptions, otherParams.configReplSetTestOptions);

    var nodeOptions = [];
    for (var i = 0; i < numConfigs; ++i) {
        nodeOptions.push(otherParams["c" + i] || {});
    }

    rstOptions.nodes = nodeOptions;

    // Start the config server
    this.configRS = new ReplSetTest(rstOptions);
    this.configRS.startSet(startOptions);

    var config = this.configRS.getReplSetConfig();
    config.configsvr = true;
    config.settings = config.settings || {};
    var initiateTimeout = otherParams.rsOptions && otherParams.rsOptions.initiateTimeout;
    this.configRS.initiate(config, null, initiateTimeout);

    // Wait for master to be elected before starting mongos
    this.configRS.getPrimary();

    this._configDB = this.configRS.getURL();
    this._configServers = this.configRS.nodes;
    for (var i = 0; i < numConfigs; ++i) {
        var conn = this._configServers[i];
        this["config" + i] = conn;
        this["c" + i] = conn;
    }

    printjson("config servers: " + this._configDB);

    var configConnection = _connectWithRetry(this._configDB);

    print("ShardingTest " + this._testName + " :\n" +
          tojson({config: this._configDB, shards: this._connections}));

    this._mongos = [];

    // Start the MongoS servers
    for (var i = 0; i < numMongos; i++) {
        options = {
            useHostname: otherParams.useHostname,
            pathOpts: Object.merge(pathOpts, {mongos: i}),
            configdb: this._configDB,
            verbose: verboseLevel || 0,
            keyFile: keyFile,
        };

        if (otherParams.chunkSize) {
            options.chunkSize = otherParams.chunkSize;
        }

        if (otherParams.mongosOptions && otherParams.mongosOptions.binVersion) {
            otherParams.mongosOptions.binVersion =
                MongoRunner.versionIterator(otherParams.mongosOptions.binVersion);
        }

        options = Object.merge(options, otherParams.mongosOptions);
        options = Object.merge(options, otherParams.extraOptions);
        options = Object.merge(options, otherParams["s" + i]);

        options.port = options.port || allocatePort();

        if (otherParams.useBridge) {
            var bridgeOptions =
                Object.merge(otherParams.bridgeOptions, options.bridgeOptions || {});
            bridgeOptions = Object.merge(
                bridgeOptions,
                {
                  hostName: otherParams.useHostname ? hostName : "localhost",
                  // The mongos processes identify themselves to mongobridge as host:port, where the
                  // host is the actual hostname of the machine and not localhost.
                  dest: hostName + ":" + options.port,
                });

            var bridge = new MongoBridge(bridgeOptions);
        }

        var conn = MongoRunner.runMongos(options);
        if (!conn) {
            throw new Error("Failed to start mongos " + i);
        }

        if (otherParams.useBridge) {
            bridge.connectToBridge();
            this._mongos.push(bridge);
            unbridgedMongos.push(conn);
        } else {
            this._mongos.push(conn);
        }

        if (i === 0) {
            this.s = this._mongos[i];
            this.admin = this._mongos[i].getDB('admin');
            this.config = this._mongos[i].getDB('config');
        }

        this["s" + i] = this._mongos[i];
    }

    // If auth is enabled for the test, login the mongos connections as system in order to
    // configure the instances and then log them out again.
    if (keyFile) {
        authutil.assertAuthenticate(this._mongos,
                                    'admin',
                                    {
                                      user: '__system',
                                      mechanism: 'MONGODB-CR',
                                      pwd: cat(keyFile).replace(/[\011-\015\040]/g, '')
                                    });
    }

    try {
        // Disable the balancer unless it is explicitly turned on
        if (!otherParams.enableBalancer) {
            this.stopBalancer();
        }

        // Lower the mongos replica set monitor's threshold for deeming RS shard hosts as
        // inaccessible in order to speed up tests, which shutdown entire shards and check for
        // errors. This attempt is best-effort and failure should not have effect on the actual
        // test execution, just the execution time.
        this._mongos.forEach(function(mongos) {
            var res = mongos.adminCommand({setParameter: 1, replMonitorMaxFailedChecks: 2});

            // For tests, which use x509 certificate for authentication, the command above will not
            // work due to authorization error.
            if (res.code != ErrorCodes.Unauthorized) {
                assert.commandWorked(res);
            }
        });
    } finally {
        if (keyFile) {
            authutil.logout(this._mongos, 'admin');
        }
    }

    try {
        if (!otherParams.manualAddShard) {
            this._shardNames = [];

            var testName = this._testName;
            var admin = this.admin;
            var shardNames = this._shardNames;

            this._connections.forEach(function(z) {
                var n = z.name || z.host || z;

                print("ShardingTest " + testName + " going to add shard : " + n);

                var result = admin.runCommand({addshard: n});
                assert.commandWorked(result, "Failed to add shard " + n);

                shardNames.push(result.shardAdded);
                z.shardName = result.shardAdded;
            });
        }
    } catch (e) {
        // Clean up the running procceses on failure
        print("Failed to add shards, stopping cluster.");
        this.stop();
        throw e;
    }

    if (jsTestOptions().keyFile) {
        jsTest.authenticate(configConnection);
        jsTest.authenticateNodes(this._configServers);
        jsTest.authenticateNodes(this._mongos);
    }
};



// ---- MODULE: types ---- 
// Date and time types
if (typeof(Timestamp) != "undefined") {
    Timestamp.prototype.tojson = function() {
        return this.toString();
    };

    Timestamp.prototype.getTime = function() {
        return this.t;
    };

    Timestamp.prototype.getInc = function() {
        return this.i;
    };

    Timestamp.prototype.toString = function() {
        return "Timestamp(" + this.t + ", " + this.i + ")";
    };
} else {
    print("warning: no Timestamp class");
}

Date.timeFunc = function(theFunc, numTimes) {
    var start = new Date();
    numTimes = numTimes || 1;
    for (var i = 0; i < numTimes; i++) {
        theFunc.apply(null, Array.from(arguments).slice(2));
    }

    return (new Date()).getTime() - start.getTime();
};

Date.prototype.tojson = function() {
    var UTC = 'UTC';
    var year = this['get' + UTC + 'FullYear']().zeroPad(4);
    var month = (this['get' + UTC + 'Month']() + 1).zeroPad(2);
    var date = this['get' + UTC + 'Date']().zeroPad(2);
    var hour = this['get' + UTC + 'Hours']().zeroPad(2);
    var minute = this['get' + UTC + 'Minutes']().zeroPad(2);
    var sec = this['get' + UTC + 'Seconds']().zeroPad(2);

    if (this['get' + UTC + 'Milliseconds']())
        sec += '.' + this['get' + UTC + 'Milliseconds']().zeroPad(3);

    var ofs = 'Z';
    // // print a non-UTC time
    // var ofsmin = this.getTimezoneOffset();
    // if (ofsmin != 0){
    //     ofs = ofsmin > 0 ? '-' : '+'; // This is correct
    //     ofs += (ofsmin/60).zeroPad(2)
    //     ofs += (ofsmin%60).zeroPad(2)
    // }
    return 'ISODate("' + year + '-' + month + '-' + date + 'T' + hour + ':' + minute + ':' + sec +
        ofs + '")';
};

ISODate = function(isoDateStr) {
    if (!isoDateStr)
        return new Date();

    var isoDateRegex =
        /(\d{4})-?(\d{2})-?(\d{2})([T ](\d{2})(:?(\d{2})(:?(\d{2}(\.\d+)?))?)?(Z|([+-])(\d{2}):?(\d{2})?)?)?/;
    var res = isoDateRegex.exec(isoDateStr);

    if (!res)
        throw Error("invalid ISO date");

    var year = parseInt(res[1], 10) || 1970;  // this should always be present
    var month = (parseInt(res[2], 10) || 1) - 1;
    var date = parseInt(res[3], 10) || 0;
    var hour = parseInt(res[5], 10) || 0;
    var min = parseInt(res[7], 10) || 0;
    var sec = parseInt((res[9] && res[9].substr(0, 2)), 10) || 0;
    var ms = Math.round((parseFloat(res[10]) || 0) * 1000);
    if (ms == 1000) {
        ms = 0;
        ++sec;
    }
    if (sec == 60) {
        sec = 0;
        ++min;
    }
    if (min == 60) {
        min = 0;
        ++hour;
    }
    if (hour == 24) {
        hour = 0;  // the day wrapped, let JavaScript figure out the rest
        var tempTime = Date.UTC(year, month, date, hour, min, sec, ms);
        tempTime += 24 * 60 * 60 * 1000;  // milliseconds in a day
        var tempDate = new Date(tempTime);
        year = tempDate.getUTCFullYear();
        month = tempDate.getUTCMonth();
        date = tempDate.getUTCDate();
    }

    var time = Date.UTC(year, month, date, hour, min, sec, ms);

    if (res[11] && res[11] != 'Z') {
        var ofs = 0;
        ofs += (parseInt(res[13], 10) || 0) * 60 * 60 * 1000;  // hours
        ofs += (parseInt(res[14], 10) || 0) * 60 * 1000;       // mins
        if (res[12] == '+')                                    // if ahead subtract
            ofs *= -1;

        time += ofs;
    }

    return new Date(time);
};

// Regular Expression
RegExp.escape = function(text) {
    return text.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&");
};

RegExp.prototype.tojson = RegExp.prototype.toString;

// Array
Array.contains = function(a, x) {
    if (!Array.isArray(a)) {
        throw new Error("The first argument to Array.contains must be an array");
    }

    for (var i = 0; i < a.length; i++) {
        if (a[i] == x)
            return true;
    }
    return false;
};

Array.unique = function(a) {
    if (!Array.isArray(a)) {
        throw new Error("The first argument to Array.unique must be an array");
    }

    var u = [];
    for (var i = 0; i < a.length; i++) {
        var o = a[i];
        if (!Array.contains(u, o)) {
            u.push(o);
        }
    }
    return u;
};

Array.shuffle = function(arr) {
    if (!Array.isArray(arr)) {
        throw new Error("The first argument to Array.shuffle must be an array");
    }

    for (var i = 0; i < arr.length - 1; i++) {
        var pos = i + Random.randInt(arr.length - i);
        var save = arr[i];
        arr[i] = arr[pos];
        arr[pos] = save;
    }
    return arr;
};

Array.tojson = function(a, indent, nolint) {
    if (!Array.isArray(a)) {
        throw new Error("The first argument to Array.tojson must be an array");
    }

    var elementSeparator = nolint ? " " : "\n";

    if (!indent)
        indent = "";
    if (nolint)
        indent = "";

    if (a.length == 0) {
        return "[ ]";
    }

    var s = "[" + elementSeparator;

    // add to indent if we are pretty
    if (!nolint)
        indent += "\t";

    for (var i = 0; i < a.length; i++) {
        s += indent + tojson(a[i], indent, nolint);
        if (i < a.length - 1) {
            s += "," + elementSeparator;
        }
    }

    // remove from indent if we are pretty
    if (!nolint)
        indent = indent.substring(1);

    s += elementSeparator + indent + "]";
    return s;
};

Array.fetchRefs = function(arr, coll) {
    if (!Array.isArray(arr)) {
        throw new Error("The first argument to Array.fetchRefs must be an array");
    }

    var n = [];
    for (var i = 0; i < arr.length; i++) {
        var z = arr[i];
        if (coll && coll != z.getCollection())
            continue;
        n.push(z.fetch());
    }
    return n;
};

Array.sum = function(arr) {
    if (!Array.isArray(arr)) {
        throw new Error("The first argument to Array.sum must be an array");
    }

    if (arr.length == 0)
        return null;
    var s = arr[0];
    for (var i = 1; i < arr.length; i++)
        s += arr[i];
    return s;
};

Array.avg = function(arr) {
    if (!Array.isArray(arr)) {
        throw new Error("The first argument to Array.avg must be an array");
    }

    if (arr.length == 0)
        return null;
    return Array.sum(arr) / arr.length;
};

Array.stdDev = function(arr) {
    if (!Array.isArray(arr)) {
        throw new Error("The first argument to Array.stdDev must be an array");
    }

    var avg = Array.avg(arr);
    var sum = 0;

    for (var i = 0; i < arr.length; i++) {
        sum += Math.pow(arr[i] - avg, 2);
    }

    return Math.sqrt(sum / arr.length);
};

// Object
Object.extend = function(dst, src, deep) {
    for (var k in src) {
        var v = src[k];
        if (deep && typeof(v) == "object") {
            if (v.constructor === ObjectId) {  // convert ObjectId properly
                eval("v = " + tojson(v));
            } else if ("floatApprox" in v) {  // convert NumberLong properly
                eval("v = " + tojson(v));
            } else {
                v = Object.extend(typeof(v.length) == "number" ? [] : {}, v, true);
            }
        }
        dst[k] = v;
    }
    return dst;
};

Object.merge = function(dst, src, deep) {
    var clone = Object.extend({}, dst, deep);
    return Object.extend(clone, src, deep);
};

Object.keySet = function(o) {
    var ret = new Array();
    for (var i in o) {
        if (!(i in o.__proto__ && o[i] === o.__proto__[i])) {
            ret.push(i);
        }
    }
    return ret;
};

// String
if (String.prototype.trim === undefined) {
    String.prototype.trim = function() {
        return this.replace(/^\s+|\s+$/g, "");
    };
}
if (String.prototype.trimLeft === undefined) {
    String.prototype.trimLeft = function() {
        return this.replace(/^\s+/, "");
    };
}
if (String.prototype.trimRight === undefined) {
    String.prototype.trimRight = function() {
        return this.replace(/\s+$/, "");
    };
}

// always provide ltrim and rtrim for backwards compatibility
String.prototype.ltrim = String.prototype.trimLeft;
String.prototype.rtrim = String.prototype.trimRight;

String.prototype.startsWith = function(str) {
    return this.indexOf(str) == 0;
};

String.prototype.endsWith = function(str) {
    return this.indexOf(str, this.length - str.length) !== -1;
};

// Polyfill taken from
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/includes
if (!String.prototype.includes) {
    String.prototype.includes = function() {
        'use strict';
        return String.prototype.indexOf.apply(this, arguments) !== -1;
    };
}

// Returns a copy padded with the provided character _chr_ so it becomes (at least) _length_
// characters long.
// No truncation is performed if the string is already longer than _length_.
// @param length minimum length of the returned string
// @param right if falsy add leading whitespace, otherwise add trailing whitespace
// @param chr character to be used for padding, defaults to whitespace
// @return the padded string
String.prototype.pad = function(length, right, chr) {
    if (typeof chr == 'undefined')
        chr = ' ';
    var str = this;
    for (var i = length - str.length; i > 0; i--) {
        if (right) {
            str = str + chr;
        } else {
            str = chr + str;
        }
    }
    return str;
};

// Number
Number.prototype.toPercentStr = function() {
    return (this * 100).toFixed(2) + "%";
};

Number.prototype.zeroPad = function(width) {
    return ('' + this).pad(width, false, '0');
};

// NumberLong
if (!NumberLong.prototype) {
    NumberLong.prototype = {};
}

NumberLong.prototype.tojson = function() {
    return this.toString();
};

// NumberInt
if (!NumberInt.prototype) {
    NumberInt.prototype = {};
}

NumberInt.prototype.tojson = function() {
    return this.toString();
};

// NumberDecimal
if (typeof NumberDecimal !== 'undefined') {
    if (!NumberDecimal.prototype) {
        NumberDecimal.prototype = {};
    }

    NumberDecimal.prototype.tojson = function() {
        return this.toString();
    };
}

// ObjectId
if (!ObjectId.prototype)
    ObjectId.prototype = {};

ObjectId.prototype.toString = function() {
    return "ObjectId(" + tojson(this.toHexString()) + ")";
};

ObjectId.prototype.tojson = function() {
    return this.toString();
};

ObjectId.prototype.valueOf = function() {
    return this.str;
};

ObjectId.prototype.isObjectId = true;

ObjectId.prototype.getTimestamp = function() {
    return new Date(parseInt(this.valueOf().slice(0, 8), 16) * 1000);
};

ObjectId.prototype.equals = function(other) {
    return this.str == other.str;
};

// Creates an ObjectId from a Date.
// Based on solution discussed here:
//     http://stackoverflow.com/questions/8749971/can-i-query-mongodb-objectid-by-date
ObjectId.fromDate = function(source) {
    if (!source) {
        throw Error("date missing or undefined");
    }

    var sourceDate;

    // Extract Date from input.
    // If input is a string, assume ISO date string and
    // create a Date from the string.
    if (source instanceof Date) {
        sourceDate = source;
    } else {
        throw Error("Cannot create ObjectId from " + typeof(source) + ": " + tojson(source));
    }

    // Convert date object to seconds since Unix epoch.
    var seconds = Math.floor(sourceDate.getTime() / 1000);

    // Generate hex timestamp with padding.
    var hexTimestamp = seconds.toString(16).pad(8, false, '0') + "0000000000000000";

    // Create an ObjectId with hex timestamp.
    var objectId = ObjectId(hexTimestamp);

    return objectId;
};

// DBPointer
if (typeof(DBPointer) != "undefined") {
    DBPointer.prototype.fetch = function() {
        assert(this.ns, "need a ns");
        assert(this.id, "need an id");
        return db[this.ns].findOne({_id: this.id});
    };

    DBPointer.prototype.tojson = function(indent) {
        return this.toString();
    };

    DBPointer.prototype.getCollection = function() {
        return this.ns;
    };

    DBPointer.prototype.getId = function() {
        return this.id;
    };

    DBPointer.prototype.toString = function() {
        return "DBPointer(" + tojson(this.ns) + ", " + tojson(this.id) + ")";
    };
} else {
    print("warning: no DBPointer");
}

// DBRef
if (typeof(DBRef) != "undefined") {
    DBRef.prototype.fetch = function() {
        assert(this.$ref, "need a ns");
        assert(this.$id, "need an id");
        var coll = this.$db ? db.getSiblingDB(this.$db).getCollection(this.$ref) : db[this.$ref];
        return coll.findOne({_id: this.$id});
    };

    DBRef.prototype.tojson = function(indent) {
        return this.toString();
    };

    DBRef.prototype.getDb = function() {
        return this.$db || undefined;
    };

    DBRef.prototype.getCollection = function() {
        return this.$ref;
    };

    DBRef.prototype.getRef = function() {
        return this.$ref;
    };

    DBRef.prototype.getId = function() {
        return this.$id;
    };

    DBRef.prototype.toString = function() {
        return "DBRef(" + tojson(this.$ref) + ", " + tojson(this.$id) +
            (this.$db ? ", " + tojson(this.$db) : "") + ")";
    };
} else {
    print("warning: no DBRef");
}

// BinData
if (typeof(BinData) != "undefined") {
    BinData.prototype.tojson = function() {
        return this.toString();
    };

    BinData.prototype.subtype = function() {
        return this.type;
    };
    BinData.prototype.length = function() {
        return this.len;
    };
} else {
    print("warning: no BinData class");
}

// Map
if (typeof(Map) == "undefined") {
    Map = function() {
        this._data = {};
    };
}

Map.hash = function(val) {
    if (!val)
        return val;

    switch (typeof(val)) {
        case 'string':
        case 'number':
        case 'date':
            return val.toString();
        case 'object':
        case 'array':
            var s = "";
            for (var k in val) {
                s += k + val[k];
            }
            return s;
    }

    throw Error("can't hash : " + typeof(val));
};

Map.prototype.put = function(key, value) {
    var o = this._get(key);
    var old = o.value;
    o.value = value;
    return old;
};

Map.prototype.get = function(key) {
    return this._get(key).value;
};

Map.prototype._get = function(key) {
    var h = Map.hash(key);
    var a = this._data[h];
    if (!a) {
        a = [];
        this._data[h] = a;
    }
    for (var i = 0; i < a.length; i++) {
        if (friendlyEqual(key, a[i].key)) {
            return a[i];
        }
    }
    var o = {
        key: key,
        value: null
    };
    a.push(o);
    return o;
};

Map.prototype.values = function() {
    var all = [];
    for (var k in this._data) {
        this._data[k].forEach(function(z) {
            all.push(z.value);
        });
    }
    return all;
};

if (typeof(gc) == "undefined") {
    gc = function() {
        print("warning: using noop gc()");
    };
}

// Free Functions
tojsononeline = function(x) {
    return tojson(x, " ", true);
};

tojson = function(x, indent, nolint) {
    if (x === null)
        return "null";

    if (x === undefined)
        return "undefined";

    if (!indent)
        indent = "";

    switch (typeof x) {
        case "string": {
            var out = new Array(x.length + 1);
            out[0] = '"';
            for (var i = 0; i < x.length; i++) {
                switch (x[i]) {
                    case '"':
                        out[out.length] = '\\"';
                        break;
                    case '\\':
                        out[out.length] = '\\\\';
                        break;
                    case '\b':
                        out[out.length] = '\\b';
                        break;
                    case '\f':
                        out[out.length] = '\\f';
                        break;
                    case '\n':
                        out[out.length] = '\\n';
                        break;
                    case '\r':
                        out[out.length] = '\\r';
                        break;
                    case '\t':
                        out[out.length] = '\\t';
                        break;

                    default: {
                        var code = x.charCodeAt(i);
                        if (code < 0x20) {
                            out[out.length] =
                                (code < 0x10 ? '\\u000' : '\\u00') + code.toString(16);
                        } else {
                            out[out.length] = x[i];
                        }
                    }
                }
            }

            return out.join('') + "\"";
        }
        case "number":
        case "boolean":
            return "" + x;
        case "object": {
            var s = tojsonObject(x, indent, nolint);
            if ((nolint == null || nolint == true) && s.length < 80 &&
                (indent == null || indent.length == 0)) {
                s = s.replace(/[\t\r\n]+/gm, " ");
            }
            return s;
        }
        case "function":
            if (x === MinKey || x === MaxKey)
                return x.tojson();
            return x.toString();
        default:
            throw Error("tojson can't handle type " + (typeof x));
    }

};

tojsonObject = function(x, indent, nolint) {
    var lineEnding = nolint ? " " : "\n";
    var tabSpace = nolint ? "" : "\t";
    assert.eq((typeof x), "object", "tojsonObject needs object, not [" + (typeof x) + "]");

    if (!indent)
        indent = "";

    if (typeof(x.tojson) == "function" && x.tojson != tojson) {
        return x.tojson(indent, nolint);
    }

    if (x.constructor && typeof(x.constructor.tojson) == "function" &&
        x.constructor.tojson != tojson) {
        return x.constructor.tojson(x, indent, nolint);
    }

    if (x instanceof Error) {
        return x.toString();
    }

    try {
        x.toString();
    } catch (e) {
        // toString not callable
        return "[object]";
    }

    var s = "{" + lineEnding;

    // push one level of indent
    indent += tabSpace;

    var keys = x;
    if (typeof(x._simpleKeys) == "function")
        keys = x._simpleKeys();
    var fieldStrings = [];
    for (var k in keys) {
        var val = x[k];

        // skip internal DB types to avoid issues with interceptors
        if (typeof DB != 'undefined' && val == DB.prototype)
            continue;
        if (typeof DBCollection != 'undefined' && val == DBCollection.prototype)
            continue;

        fieldStrings.push(indent + "\"" + k + "\" : " + tojson(val, indent, nolint));
    }

    if (fieldStrings.length > 0) {
        s += fieldStrings.join("," + lineEnding);
    } else {
        s += indent;
    }
    s += lineEnding;

    // pop one level of indent
    indent = indent.substring(1);
    return s + indent + "}";
};

printjson = function(x) {
    print(tojson(x));
};

printjsononeline = function(x) {
    print(tojsononeline(x));
};

isString = function(x) {
    return typeof(x) == "string";
};

isNumber = function(x) {
    return typeof(x) == "number";
};

// This function returns true even if the argument is an array.  See SERVER-14220.
isObject = function(x) {
    return typeof(x) == "object";
};



// ---- MODULE: port_types ---- 

Timestamp.prototype.toDateString = function(){
	return new Date(this.low_ * 1000).toString();
}

Timestamp.prototype.toString = function() {
	return "Timestamp(" + this.low_ + ", " + this.high_ + ")"
}

Timestamp.prototype.getTime = function() {
    return this.low_;
};

Timestamp.prototype.getInc = function() {
    return this.high_;
};



// ---- MODULE: upgrade_check ---- 
(function() {
    "use strict";

    /**
     * Check a document
     */
    var documentUpgradeCheck = function(indexes, doc) {
        var goodSoFar = true;
        var invalidForStorage = Object.invalidForStorage(doc);
        if (invalidForStorage) {
            print("Document Error: document is no longer valid in 2.6 because " +
                  invalidForStorage + ": " + tojsononeline(doc));
            goodSoFar = false;
        }
        indexes.forEach(function(idx) {
            if (isKeyTooLarge(idx, doc)) {
                print("Document Error: key for index " + tojsononeline(idx) +
                      " too long for document: " + tojsononeline(doc));
                goodSoFar = false;
            }
        });
        return goodSoFar;
    };

    var indexUpgradeCheck = function(index) {
        var goodSoFar = true;
        var indexValid = validateIndexKey(index.key);
        if (!indexValid.ok) {
            print("Index Error: invalid index spec for index '" + index.name + "': " +
                  tojsononeline(index.key));
            goodSoFar = false;
        }
        return goodSoFar;
    };

    var collUpgradeCheck = function(collObj, checkDocs) {
        var fullName = collObj.getFullName();
        var collName = collObj.getName();
        var dbName = collObj.getDB().getName();
        print("\nChecking collection " + fullName + " (db:" + dbName + " coll:" + collName + ")");
        var dbObj = collObj.getDB();
        var goodSoFar = true;

        // check for _id index if and only if it should be present
        // no $, not oplog, not system, not config db
        var checkIdIdx = true;
        if (dbName == "config") {
            checkIdIdx = false;
        } else if (dbName == "local") {
            if (collName == "oplog.rs" || collName == "oplog.$main" || collName == "startup_log" ||
                collName == "me") {
                checkIdIdx = false;
            }
        }

        if (collName.indexOf('$') !== -1 || collName.indexOf("system.") === 0) {
            checkIdIdx = false;
        }
        var indexes = collObj.getIndexes();
        var foundIdIndex = false;

        // run index level checks on each index on the collection
        indexes.forEach(function(index) {

            if (index.name == "_id_") {
                foundIdIndex = true;
            }

            if (!indexUpgradeCheck(index)) {
                goodSoFar = false;
            } else {
                // add its key to the list of index keys to check documents against
                if (index["v"] !== 1) {
                    print("Warning: upgradeCheck only supports V1 indexes. Skipping index: " +
                          tojsononeline(index));
                } else {
                    indexes.push(index);
                }
            }
        });

        // If we need to validate the _id_ index, see if we found it.
        if (checkIdIdx && !foundIdIndex) {
            print("Collection Error: lack of _id index on collection: " + fullName);
            goodSoFar = false;
        }
        // do not validate the documents in system collections
        if (collName.indexOf("system.") === 0) {
            checkDocs = false;
        }
        // do not validate the documents in config dbs
        if (dbName == "config") {
            checkDocs = false;
        }
        // do not validate docs in local db for some collections
        else if (dbName === "local") {
            if (collName == "oplog.rs" ||  // skip document validation for oplogs
                collName == "oplog.$main" ||
                collName == "replset.minvalid"  // skip document validation for minvalid coll
                ) {
                checkDocs = false;
            }
        }

        if (checkDocs) {
            var lastAlertTime = Date.now();
            var alertInterval = 10 * 1000;  // 10 seconds
            var numDocs = 0;
            // run document level checks on each document in the collection
            var theColl = dbObj.getSiblingDB(dbName).getCollection(collName);
            theColl.find()
                .addOption(DBQuery.Option.noTimeout)
                .sort({$natural: 1})
                .forEach(function(doc) {
                    numDocs++;

                    if (!documentUpgradeCheck(indexes, doc)) {
                        goodSoFar = false;
                        lastAlertTime = Date.now();
                    }
                    var nowTime = Date.now();
                    if (nowTime - lastAlertTime > alertInterval) {
                        print(numDocs + " documents processed");
                        lastAlertTime = nowTime;
                    }
                });
        }

        return goodSoFar;
    };

    var dbUpgradeCheck = function(dbObj, checkDocs) {
        print("\nChecking database " + dbObj.getName());
        var goodSoFar = true;

        // run collection level checks on each collection in the db
        dbObj.getCollectionNames().forEach(function(collName) {
            if (!collUpgradeCheck(dbObj.getCollection(collName), checkDocs)) {
                goodSoFar = false;
            }
        });

        return goodSoFar;
    };

    DB.prototype.upgradeCheck = function(obj, checkDocs) {
        var self = this;
        // parse args if there are any
        if (obj) {
            // check collection if a collection is passed
            if (obj["collection"]) {
                // make sure a string was passed in for the collection
                if (typeof obj["collection"] !== "string") {
                    throw Error("The collection field must contain a string");
                } else {
                    print("Checking collection '" + self.getName() + '.' + obj["collection"] +
                          "' for 2.6 upgrade compatibility");
                    if (collUpgradeCheck(self.getCollection(obj["collection"]))) {
                        print("Everything in '" + self.getName() + '.' + obj["collection"] +
                              "' is ready for the upgrade!");
                        return true;
                    }
                    print("To fix the problems above please consult " +
                          "http://dochub.mongodb.org/core/upgrade_checker_help");
                    return false;
                }
            } else {
                throw Error(
                    "When providing an argument to upgradeCheck, it must be of the form " +
                    "{collection: <collectionNameString>}. Otherwise, it will check every " +
                    "collection in the database. If you would like to check all databases, " +
                    "run db.upgradeCheckAllDBs() from the admin database.");
            }
        }

        print("database '" + self.getName() + "' for 2.6 upgrade compatibility");
        if (dbUpgradeCheck(self, checkDocs)) {
            print("Everything in '" + self.getName() + "' is ready for the upgrade!");
            return true;
        }
        print("To fix the problems above please consult " +
              "http://dochub.mongodb.org/core/upgrade_checker_help");
        return false;
    };

    DB.prototype.upgradeCheckAllDBs = function(checkDocs) {
        var self = this;
        if (self.getName() !== "admin") {
            throw Error("db.upgradeCheckAllDBs() can only be run from the admin database");
        }

        var dbs = self.getMongo().getDBs();
        var goodSoFar = true;

        // run db level checks on each db
        dbs.databases.forEach(function(dbObj) {
            if (!dbUpgradeCheck(self.getSiblingDB(dbObj.name), checkDocs)) {
                goodSoFar = false;
            }
        });

        if (goodSoFar) {
            print("Everything is ready for the upgrade!");
            return true;
        }
        print("To fix the problems above please consult " +
              "http://dochub.mongodb.org/core/upgrade_checker_help");
        return false;
    };

})();



// ---- MODULE: utils_auth ---- 
var authutil;

(function() {
    assert(!authutil);
    authutil = {};

    /**
     * Logs out all connections "conn" from database "dbname".
     */
    authutil.logout = function(conn, dbname) {
        var i;
        if (null == conn.length) {
            conn = [conn];
        }
        for (i = 0; i < conn.length; ++i) {
            conn[i].getDB(dbname).logout();
        }
    };

    /**
     * Authenticates all connections in "conns" using "authParams" on database "dbName".
     *
     * Raises an exception if any authentication fails, and tries to leave all connnections
     * in "conns" in the logged-out-of-dbName state.
     */
    authutil.assertAuthenticate = function(conns, dbName, authParams) {
        var conn, i, ex, ex2;
        if (conns.length == null)
            conns = [conns];

        try {
            for (i = 0; i < conns.length; ++i) {
                conn = conns[i];
                assert(conn.getDB(dbName).auth(authParams),
                       "Failed to authenticate " + conn + " to " + dbName + " using parameters " +
                           tojson(authParams));
            }
        } catch (ex) {
            try {
                authutil.logout(conns, dbName);
            } catch (ex2) {
            }
            throw ex;
        }
    };

    /**
    * Authenticates all connections in "conns" using "authParams" on database "dbName".
    * Raises in exception if any of the authentications succeed.
    */
    authutil.assertAuthenticateFails = function(conns, dbName, authParams) {
        var conn, i;
        if (conns.length == null)
            conns = [conns];

        for (i = 0; i < conns.length; ++i) {
            conn = conns[i];
            assert(!conn.getDB(dbName).auth(authParams),
                   "Unexpectedly authenticated " + conn + " to " + dbName + " using parameters " +
                       tojson(authParams));
        }
    };

    /**
     * Executes action() after authenticating the keyfile user on "conn", then logs out the keyfile
     * user.
     */
    authutil.asCluster = function(conn, keyfile, action) {
        var ex;
        authutil.assertAuthenticate(conn,
                                    'local',
                                    {
                                      user: '__system',
                                      mechanism: 'SCRAM-SHA-1',
                                      pwd: cat(keyfile).replace(/[\011-\015\040]/g, '')
                                    });

        try {
            return action();
        } finally {
            try {
                authutil.logout(conn, 'local');
            } catch (ex) {
            }
        }
    };

}());



// ---- MODULE: utils ---- 
__quiet = false;
__magicNoPrint = {
    __magicNoPrint: 1111
};
__callLastError = false;
_verboseShell = false;

chatty = function(s) {
    if (!__quiet)
        print(s);
};

function reconnect(db) {
    assert.soon(function() {
        try {
            db.runCommand({ping: 1});
            return true;
        } catch (x) {
            return false;
        }
    });
}

function _getErrorWithCode(codeOrObj, message) {
    var e = new Error(message);
    if (codeOrObj != undefined) {
        if (codeOrObj.writeError) {
            e.code = codeOrObj.writeError.code;
        } else if (codeOrObj.code) {
            e.code = codeOrObj.code;
        } else {
            // At this point assume codeOrObj is a number type
            e.code = codeOrObj;
        }
    }

    return e;
}

// Please consider using bsonWoCompare instead of this as much as possible.
friendlyEqual = function(a, b) {
    if (a == b)
        return true;

    a = tojson(a, false, true);
    b = tojson(b, false, true);

    if (a == b)
        return true;

    var clean = function(s) {
        s = s.replace(/NumberInt\((\-?\d+)\)/g, "$1");
        return s;
    };

    a = clean(a);
    b = clean(b);

    if (a == b)
        return true;

    return false;
};

printStackTrace = function() {
    try {
        throw new Error("Printing Stack Trace");
    } catch (e) {
        print(e.stack);
    }
};

/**
 * <p> Set the shell verbosity. If verbose the shell will display more information about command
 * results. </>
 * <p> Default is off. <p>
 * @param {Bool} verbosity on / off
 */
setVerboseShell = function(value) {
    if (value == undefined)
        value = true;
    _verboseShell = value;
};

// Formats a simple stacked horizontal histogram bar in the shell.
// @param data array of the form [[ratio, symbol], ...] where ratio is between 0 and 1 and
//             symbol is a string of length 1
// @param width width of the bar (excluding the left and right delimiters [ ] )
// e.g. _barFormat([[.3, "="], [.5, '-']], 80) returns
//      "[========================----------------------------------------                ]"
_barFormat = function(data, width) {
    var remaining = width;
    var res = "[";
    for (var i = 0; i < data.length; i++) {
        for (var x = 0; x < data[i][0] * width; x++) {
            if (remaining-- > 0) {
                res += data[i][1];
            }
        }
    }
    while (remaining-- > 0) {
        res += " ";
    }
    res += "]";
    return res;
};

// these two are helpers for Array.sort(func)
compare = function(l, r) {
    return (l == r ? 0 : (l < r ? -1 : 1));
};

// arr.sort(compareOn('name'))
compareOn = function(field) {
    return function(l, r) {
        return compare(l[field], r[field]);
    };
};

shellPrint = function(x) {
    it = x;
    if (x != undefined)
        shellPrintHelper(x);

    if (db) {
        var e = db.getPrevError();
        if (e.err) {
            if (e.nPrev <= 1)
                print("error on last call: " + tojson(e.err));
            else
                print("an error " + tojson(e.err) + " occurred " + e.nPrev +
                      " operations back in the command invocation");
        }
        db.resetError();
    }
};

print.captureAllOutput = function(fn, args) {
    var res = {};
    res.output = [];
    var __orig_print = print;
    print = function() {
        Array.prototype.push.apply(res.output,
                                   Array.prototype.slice.call(arguments).join(" ").split("\n"));
    };
    try {
        res.result = fn.apply(undefined, args);
    } finally {
        // Stop capturing print() output
        print = __orig_print;
    }
    return res;
};

if (typeof TestData == "undefined") {
    TestData = undefined;
}

jsTestName = function() {
    if (TestData)
        return TestData.testName;
    return "__unknown_name__";
};

var _jsTestOptions = {
    enableTestCommands: true
};  // Test commands should be enabled by default

jsTestOptions = function() {
    if (TestData) {
        return Object.merge(
            _jsTestOptions,
            {
              setParameters: TestData.setParameters,
              setParametersMongos: TestData.setParametersMongos,
              storageEngine: TestData.storageEngine,
              wiredTigerEngineConfigString: TestData.wiredTigerEngineConfigString,
              wiredTigerCollectionConfigString: TestData.wiredTigerCollectionConfigString,
              wiredTigerIndexConfigString: TestData.wiredTigerIndexConfigString,
              noJournal: TestData.noJournal,
              noJournalPrealloc: TestData.noJournalPrealloc,
              auth: TestData.auth,
              keyFile: TestData.keyFile,
              authUser: "__system",
              authPassword: TestData.keyFileData,
              authMechanism: TestData.authMechanism,
              adminUser: TestData.adminUser || "admin",
              adminPassword: TestData.adminPassword || "password",
              useLegacyConfigServers: TestData.useLegacyConfigServers || false,
              useLegacyReplicationProtocol: TestData.useLegacyReplicationProtocol || false,
              enableMajorityReadConcern: TestData.enableMajorityReadConcern,
              writeConcernMajorityShouldJournal: TestData.writeConcernMajorityShouldJournal,
              enableEncryption: TestData.enableEncryption,
              encryptionKeyFile: TestData.encryptionKeyFile,
              auditDestination: TestData.auditDestination,
              minPort: TestData.minPort,
              maxPort: TestData.maxPort,
              // Note: does not support the array version
              mongosBinVersion: TestData.mongosBinVersion || "",
            });
    }
    return _jsTestOptions;
};

setJsTestOption = function(name, value) {
    _jsTestOptions[name] = value;
};

jsTestLog = function(msg) {
    print("\n\n----\n" + msg + "\n----\n\n");
};

jsTest = {};

jsTest.name = jsTestName;
jsTest.options = jsTestOptions;
jsTest.setOption = setJsTestOption;
jsTest.log = jsTestLog;
jsTest.readOnlyUserRoles = ["read"];
jsTest.basicUserRoles = ["dbOwner"];
jsTest.adminUserRoles = ["root"];

jsTest.authenticate = function(conn) {
    if (!jsTest.options().auth && !jsTest.options().keyFile) {
        conn.authenticated = true;
        return true;
    }

    try {
        assert.soon(function() {
            // Set authenticated to stop an infinite recursion from getDB calling
            // back into authenticate.
            conn.authenticated = true;
            print("Authenticating as internal " + jsTestOptions().authUser +
                  " user with mechanism " + DB.prototype._defaultAuthenticationMechanism +
                  " on connection: " + conn);
            conn.authenticated = conn.getDB('admin').auth({
                user: jsTestOptions().authUser,
                pwd: jsTestOptions().authPassword,
            });
            return conn.authenticated;
        }, "Authenticating connection: " + conn, 5000, 1000);
    } catch (e) {
        print("Caught exception while authenticating connection: " + tojson(e));
        conn.authenticated = false;
    }
    return conn.authenticated;
};

jsTest.authenticateNodes = function(nodes) {
    assert.soon(function() {
        for (var i = 0; i < nodes.length; i++) {
            // Don't try to authenticate to arbiters
            res = nodes[i].getDB("admin").runCommand({replSetGetStatus: 1});
            if (res.myState == 7) {
                continue;
            }
            if (jsTest.authenticate(nodes[i]) != 1) {
                return false;
            }
        }
        return true;
    }, "Authenticate to nodes: " + nodes, 30000);
};

jsTest.isMongos = function(conn) {
    return conn.getDB('admin').isMaster().msg == 'isdbgrid';
};

defaultPrompt = function() {
    var status = db.getMongo().authStatus;
    var prefix = db.getMongo().promptPrefix;

    if (typeof prefix == 'undefined') {
        prefix = "";
        var buildInfo = db.runCommand({buildInfo: 1});
        try {
            if (buildInfo.modules.indexOf("enterprise") > -1) {
                prefix = "MongoDB Enterprise ";
            }
        } catch (e) {
            // Don't do anything here. Just throw the error away.
        }
        db.getMongo().promptPrefix = prefix;
    }

    try {
        // try to use repl set prompt -- no status or auth detected yet
        if (!status || !status.authRequired) {
            try {
                var prompt = replSetMemberStatePrompt();
                // set our status that it was good
                db.getMongo().authStatus = {
                    replSetGetStatus: true,
                    isMaster: true
                };
                return prefix + prompt;
            } catch (e) {
                // don't have permission to run that, or requires auth
                // print(e);
                status = {
                    authRequired: true,
                    replSetGetStatus: false,
                    isMaster: true
                };
            }
        }
        // auth detected

        // try to use replSetGetStatus?
        if (status.replSetGetStatus) {
            try {
                var prompt = replSetMemberStatePrompt();
                // set our status that it was good
                status.replSetGetStatus = true;
                db.getMongo().authStatus = status;
                return prefix + prompt;
            } catch (e) {
                // don't have permission to run that, or requires auth
                // print(e);
                status.authRequired = true;
                status.replSetGetStatus = false;
            }
        }

        // try to use isMaster?
        if (status.isMaster) {
            try {
                var prompt = isMasterStatePrompt();
                status.isMaster = true;
                db.getMongo().authStatus = status;
                return prefix + prompt;
            } catch (e) {
                status.authRequired = true;
                status.isMaster = false;
            }
        }
    } catch (ex) {
        printjson(ex);
        // reset status and let it figure it out next time.
        status = {
            isMaster: true
        };
    }

    db.getMongo().authStatus = status;
    return prefix + "> ";
};

replSetMemberStatePrompt = function() {
    var state = '';
    var stateInfo = db.getSiblingDB('admin').runCommand({replSetGetStatus: 1, forShell: 1});
    if (stateInfo.ok) {
        // Report the self member's stateStr if it's present.
        stateInfo.members.forEach(function(member) {
            if (member.self) {
                state = member.stateStr;
            }
        });
        // Otherwise fall back to reporting the numeric myState field (mongodb 1.6).
        if (!state) {
            state = stateInfo.myState;
        }
        state = '' + stateInfo.set + ':' + state;
    } else {
        var info = stateInfo.info;
        if (info && info.length < 20) {
            state = info;  // "mongos", "configsvr"
        } else {
            throw _getErrorWithCode(stateInfo, "Failed:" + info);
        }
    }
    return state + '> ';
};

isMasterStatePrompt = function() {
    var state = '';
    var isMaster = db.runCommand({isMaster: 1, forShell: 1});
    if (isMaster.ok) {
        var role = "";

        if (isMaster.msg == "isdbgrid") {
            role = "mongos";
        }

        if (isMaster.setName) {
            if (isMaster.ismaster)
                role = "PRIMARY";
            else if (isMaster.secondary)
                role = "SECONDARY";
            else if (isMaster.arbiterOnly)
                role = "ARBITER";
            else {
                role = "OTHER";
            }
            state = isMaster.setName + ':';
        }
        state = state + role;
    } else {
        throw _getErrorWithCode(isMaster, "Failed: " + tojson(isMaster));
    }
    return state + '> ';
};

if (typeof(_useWriteCommandsDefault) == 'undefined') {
    // This is for cases when the v8 engine is used other than the mongo shell, like map reduce.
    _useWriteCommandsDefault = function() {
        return false;
    };
}

if (typeof(_writeMode) == 'undefined') {
    // This is for cases when the v8 engine is used other than the mongo shell, like map reduce.
    _writeMode = function() {
        return "commands";
    };
}

if (typeof(_readMode) == 'undefined') {
    // This is for cases when the v8 engine is used other than the mongo shell, like map reduce.
    _readMode = function() {
        return "legacy";
    };
}

shellPrintHelper = function(x) {
    if (typeof(x) == "undefined") {
        // Make sure that we have a db var before we use it
        // TODO: This implicit calling of GLE can cause subtle, hard to track issues - remove?
        if (__callLastError && typeof(db) != "undefined" && db.getMongo &&
            db.getMongo().writeMode() == "legacy") {
            __callLastError = false;
            // explicit w:1 so that replset getLastErrorDefaults aren't used here which would be bad
            var err = db.getLastError(1);
            if (err != null) {
                print(err);
            }
        }
        return;
    }

    if (x == __magicNoPrint)
        return;

    if (x == null) {
        print("null");
        return;
    }

    if (x === MinKey || x === MaxKey)
        return x.tojson();

    if (typeof x != "object")
        return print(x);

    var p = x.shellPrint;
    if (typeof p == "function")
        return x.shellPrint();

    var p = x.tojson;
    if (typeof p == "function")
        print(x.tojson());
    else
        print(tojson(x));
};

shellAutocomplete = function(
    /*prefix*/) {  // outer scope function called on init. Actual function at end

    var universalMethods =
        "constructor prototype toString valueOf toLocaleString hasOwnProperty propertyIsEnumerable"
            .split(' ');

    var builtinMethods = {};  // uses constructor objects as keys
    builtinMethods[Array] =
        "length concat join pop push reverse shift slice sort splice unshift indexOf lastIndexOf every filter forEach map some isArray reduce reduceRight"
            .split(' ');
    builtinMethods[Boolean] = "".split(' ');  // nothing more than universal methods
    builtinMethods[Date] =
        "getDate getDay getFullYear getHours getMilliseconds getMinutes getMonth getSeconds getTime getTimezoneOffset getUTCDate getUTCDay getUTCFullYear getUTCHours getUTCMilliseconds getUTCMinutes getUTCMonth getUTCSeconds getYear parse setDate setFullYear setHours setMilliseconds setMinutes setMonth setSeconds setTime setUTCDate setUTCFullYear setUTCHours setUTCMilliseconds setUTCMinutes setUTCMonth setUTCSeconds setYear toDateString toGMTString toISOString toLocaleDateString toLocaleTimeString toTimeString toUTCString UTC now"
            .split(' ');
    if (typeof JSON != "undefined") {  // JSON is new in V8
        builtinMethods["[object JSON]"] = "parse stringify".split(' ');
    }
    builtinMethods[Math] =
        "E LN2 LN10 LOG2E LOG10E PI SQRT1_2 SQRT2 abs acos asin atan atan2 ceil cos exp floor log max min pow random round sin sqrt tan"
            .split(' ');
    builtinMethods[Number] =
        "MAX_VALUE MIN_VALUE NEGATIVE_INFINITY POSITIVE_INFINITY toExponential toFixed toPrecision"
            .split(' ');
    builtinMethods[RegExp] =
        "global ignoreCase lastIndex multiline source compile exec test".split(' ');
    builtinMethods[String] =
        "length charAt charCodeAt concat fromCharCode indexOf lastIndexOf match replace search slice split substr substring toLowerCase toUpperCase trim trimLeft trimRight"
            .split(' ');
    builtinMethods[Function] = "call apply bind".split(' ');
    builtinMethods[Object] =
        "bsonsize create defineProperty defineProperties getPrototypeOf keys seal freeze preventExtensions isSealed isFrozen isExtensible getOwnPropertyDescriptor getOwnPropertyNames"
            .split(' ');

    builtinMethods[Mongo] = "find update insert remove".split(' ');
    builtinMethods[BinData] = "hex base64 length subtype".split(' ');

    var extraGlobals =
        "Infinity NaN undefined null true false decodeURI decodeURIComponent encodeURI encodeURIComponent escape eval isFinite isNaN parseFloat parseInt unescape Array Boolean Date Math Number RegExp String print load gc MinKey MaxKey Mongo NumberInt NumberLong ObjectId DBPointer UUID BinData HexData MD5 Map Timestamp JSON"
            .split(' ');
    if (typeof NumberDecimal !== 'undefined') {
        extraGlobals[extraGlobals.length] = "NumberDecimal";
    }

    var isPrivate = function(name) {
        if (shellAutocomplete.showPrivate)
            return false;
        if (name == '_id')
            return false;
        if (name[0] == '_')
            return true;
        if (name[name.length - 1] == '_')
            return true;  // some native functions have an extra name_ method
        return false;
    };

    var customComplete = function(obj) {
        try {
            if (obj.__proto__.constructor.autocomplete) {
                var ret = obj.constructor.autocomplete(obj);
                if (ret.constructor != Array) {
                    print("\nautocompleters must return real Arrays");
                    return [];
                }
                return ret;
            } else {
                return [];
            }
        } catch (e) {
            // print( e ); // uncomment if debugging custom completers
            return [];
        }
    };

    var worker = function(prefix) {
        var global = (function() {
            return this;
        }).call();  // trick to get global object

        var curObj = global;
        var parts = prefix.split('.');
        for (var p = 0; p < parts.length - 1; p++) {  // doesn't include last part
            curObj = curObj[parts[p]];
            if (curObj == null)
                return [];
        }

        var lastPrefix = parts[parts.length - 1] || '';
        var lastPrefixLowercase = lastPrefix.toLowerCase();
        var beginning = parts.slice(0, parts.length - 1).join('.');
        if (beginning.length)
            beginning += '.';

        var possibilities =
            new Array().concat(universalMethods,
                               Object.keySet(curObj),
                               Object.keySet(curObj.__proto__),
                               builtinMethods[curObj] || [],  // curObj is a builtin constructor
                               builtinMethods[curObj.__proto__.constructor] ||
                                   [],  // curObj is made from a builtin constructor
                               curObj == global ? extraGlobals : [],
                               customComplete(curObj));

        var noDuplicates =
            {};  // see http://dreaminginjavascript.wordpress.com/2008/08/22/eliminating-duplicates/
        for (var i = 0; i < possibilities.length; i++) {
            var p = possibilities[i];
            if (typeof(curObj[p]) == "undefined" && curObj != global)
                continue;  // extraGlobals aren't in the global object
            if (p.length == 0 || p.length < lastPrefix.length)
                continue;
            if (lastPrefix[0] != '_' && isPrivate(p))
                continue;
            if (p.match(/^[0-9]+$/))
                continue;  // don't array number indexes
            if (p.substr(0, lastPrefix.length).toLowerCase() != lastPrefixLowercase)
                continue;

            var completion = beginning + p;
            if (curObj[p] && curObj[p].constructor == Function && p != 'constructor')
                completion += '(';

            noDuplicates[completion] = 0;
        }

        var ret = [];
        for (var i in noDuplicates)
            ret.push(i);

        return ret;
    };

    // this is the actual function that gets assigned to shellAutocomplete
    return function(prefix) {
        try {
            __autocomplete__ = worker(prefix).sort();
        } catch (e) {
            print("exception during autocomplete: " + tojson(e.message));
            __autocomplete__ = [];
        }
    };
}();

shellAutocomplete.showPrivate = false;  // toggle to show (useful when working on internals)

shellHelper = function(command, rest, shouldPrint) {
    command = command.trim();
    var args = rest.trim().replace(/\s*;$/, "").split("\s+");

    if (!shellHelper[command])
        throw Error("no command [" + command + "]");

    var res = shellHelper[command].apply(null, args);
    if (shouldPrint) {
        shellPrintHelper(res);
    }
    return res;
};

shellHelper.use = function(dbname) {
    var s = "" + dbname;
    if (s == "") {
        print("bad use parameter");
        return;
    }
    db = db.getMongo().getDB(dbname);
    print("switched to db " + db.getName());
};

shellHelper.set = function(str) {
    if (str == "") {
        print("bad use parameter");
        return;
    }
    tokens = str.split(" ");
    param = tokens[0];
    value = tokens[1];

    if (value == undefined)
        value = true;
    // value comes in as a string..
    if (value == "true")
        value = true;
    if (value == "false")
        value = false;

    if (param == "verbose") {
        _verboseShell = value;
    }
    print("set " + param + " to " + value);
};

shellHelper.it = function() {
    if (typeof(___it___) == "undefined" || ___it___ == null) {
        print("no cursor");
        return;
    }
    shellPrintHelper(___it___);
};

shellHelper.show = function(what) {
    assert(typeof what == "string");

    var args = what.split(/\s+/);
    what = args[0];
    args = args.splice(1);

    if (what == "profile") {
        if (db.system.profile.count() == 0) {
            print("db.system.profile is empty");
            print("Use db.setProfilingLevel(2) will enable profiling");
            print("Use db.system.profile.find() to show raw profile entries");
        } else {
            print();
            db.system.profile.find({millis: {$gt: 0}})
                .sort({$natural: -1})
                .limit(5)
                .forEach(function(x) {
                    print("" + x.op + "\t" + x.ns + " " + x.millis + "ms " +
                          String(x.ts).substring(0, 24));
                    var l = "";
                    for (var z in x) {
                        if (z == "op" || z == "ns" || z == "millis" || z == "ts")
                            continue;

                        var val = x[z];
                        var mytype = typeof(val);

                        if (mytype == "string" || mytype == "number")
                            l += z + ":" + val + " ";
                        else if (mytype == "object")
                            l += z + ":" + tojson(val) + " ";
                        else if (mytype == "boolean")
                            l += z + " ";
                        else
                            l += z + ":" + val + " ";
                    }
                    print(l);
                    print("\n");
                });
        }
        return "";
    }

    if (what == "users") {
        db.getUsers().forEach(printjson);
        return "";
    }

    if (what == "roles") {
        db.getRoles({showBuiltinRoles: true}).forEach(printjson);
        return "";
    }

    if (what == "collections" || what == "tables") {
        db.getCollectionNames().forEach(function(x) {
            print(x);
        });
        return "";
    }

    if (what == "dbs" || what == "databases") {
        var dbs = db.getMongo().getDBs();
        var dbinfo = [];
        var maxNameLength = 0;
        var maxGbDigits = 0;

        dbs.databases.forEach(function(x) {
            var sizeStr = (x.sizeOnDisk / 1024 / 1024 / 1024).toFixed(3);
            var nameLength = x.name.length;
            var gbDigits = sizeStr.indexOf(".");

            if (nameLength > maxNameLength)
                maxNameLength = nameLength;
            if (gbDigits > maxGbDigits)
                maxGbDigits = gbDigits;

            dbinfo.push({
                name: x.name,
                size: x.sizeOnDisk,
                size_str: sizeStr,
                name_size: nameLength,
                gb_digits: gbDigits
            });
        });

        dbinfo.sort(compareOn('name'));
        dbinfo.forEach(function(db) {
            var namePadding = maxNameLength - db.name_size;
            var sizePadding = maxGbDigits - db.gb_digits;
            var padding = Array(namePadding + sizePadding + 3).join(" ");
            if (db.size > 1) {
                print(db.name + padding + db.size_str + "GB");
            } else {
                print(db.name + padding + "(empty)");
            }
        });

        return "";
    }

    if (what == "log") {
        var n = "global";
        if (args.length > 0)
            n = args[0];

        var res = db.adminCommand({getLog: n});
        if (!res.ok) {
            print("Error while trying to show " + n + " log: " + res.errmsg);
            return "";
        }
        for (var i = 0; i < res.log.length; i++) {
            print(res.log[i]);
        }
        return "";
    }

    if (what == "logs") {
        var res = db.adminCommand({getLog: "*"});
        if (!res.ok) {
            print("Error while trying to show logs: " + res.errmsg);
            return "";
        }
        for (var i = 0; i < res.names.length; i++) {
            print(res.names[i]);
        }
        return "";
    }

    if (what == "startupWarnings") {
        var dbDeclared, ex;
        try {
            // !!db essentially casts db to a boolean
            // Will throw a reference exception if db hasn't been declared.
            dbDeclared = !!db;
        } catch (ex) {
            dbDeclared = false;
        }
        if (dbDeclared) {
            var res = db.adminCommand({getLog: "startupWarnings"});
            if (res.ok) {
                if (res.log.length == 0) {
                    return "";
                }
                print("Server has startup warnings: ");
                for (var i = 0; i < res.log.length; i++) {
                    print(res.log[i]);
                }
                return "";
            } else if (res.errmsg == "no such cmd: getLog") {
                // Don't print if the command is not available
                return "";
            } else if (res.code == 13 /*unauthorized*/ || res.errmsg == "unauthorized" ||
                       res.errmsg == "need to login") {
                // Don't print if startupWarnings command failed due to auth
                return "";
            } else {
                print("Error while trying to show server startup warnings: " + res.errmsg);
                return "";
            }
        } else {
            print("Cannot show startupWarnings, \"db\" is not set");
            return "";
        }
    }

    throw Error("don't know how to show [" + what + "]");

};

Math.sigFig = function(x, N) {
    if (!N) {
        N = 3;
    }
    var p = Math.pow(10, N - Math.ceil(Math.log(Math.abs(x)) / Math.log(10)));
    return Math.round(x * p) / p;
};

var Random = (function() {
    var initialized = false;
    var errorMsg =
        "The random number generator hasn't been seeded yet; " + "call Random.setRandomSeed()";

    // Set the random generator seed.
    function srand(s) {
        initialized = true;
        return _srand(s);
    }

    // Set the random generator seed & print the result.
    function setRandomSeed(s) {
        var seed = srand(s);
        print("setting random seed: " + seed);
    }

    // Generate a random number 0 <= r < 1.
    function rand() {
        if (!initialized) {
            throw new Error(errorMsg);
        }
        return _rand();
    }

    // Generate a random integer 0 <= r < n.
    function randInt(n) {
        if (!initialized) {
            throw new Error(errorMsg);
        }
        return Math.floor(rand() * n);
    }

    // Generate a random value from the exponential distribution with the specified mean.
    function genExp(mean) {
        if (!initialized) {
            throw new Error(errorMsg);
        }
        var r = rand();
        if (r == 0) {
            r = rand();
            if (r == 0) {
                r = 0.000001;
            }
        }
        return -Math.log(r) * mean;
    }

    /**
     * Generate a random value from the normal distribution with specified 'mean' and
     * 'standardDeviation'.
     */
    function genNormal(mean, standardDeviation) {
        if (!initialized) {
            throw new Error(errorMsg);
        }
        // See http://en.wikipedia.org/wiki/Marsaglia_polar_method
        while (true) {
            var x = (2 * rand()) - 1;
            var y = (2 * rand()) - 1;
            var s = (x * x) + (y * y);

            if (s > 0 && s < 1) {
                var standardNormal = x * Math.sqrt(-2 * Math.log(s) / s);
                return mean + (standardDeviation * standardNormal);
            }
        }
    }

    return {
        genExp: genExp,
        genNormal: genNormal,
        rand: rand,
        randInt: randInt,
        setRandomSeed: setRandomSeed,
        srand: srand,
    };

})();

Geo = {};
Geo.distance = function(a, b) {
    var ax = null;
    var ay = null;
    var bx = null;
    var by = null;

    for (var key in a) {
        if (ax == null)
            ax = a[key];
        else if (ay == null)
            ay = a[key];
    }

    for (var key in b) {
        if (bx == null)
            bx = b[key];
        else if (by == null)
            by = b[key];
    }

    return Math.sqrt(Math.pow(by - ay, 2) + Math.pow(bx - ax, 2));
};

Geo.sphereDistance = function(a, b) {
    var ax = null;
    var ay = null;
    var bx = null;
    var by = null;

    // TODO swap order of x and y when done on server
    for (var key in a) {
        if (ax == null)
            ax = a[key] * (Math.PI / 180);
        else if (ay == null)
            ay = a[key] * (Math.PI / 180);
    }

    for (var key in b) {
        if (bx == null)
            bx = b[key] * (Math.PI / 180);
        else if (by == null)
            by = b[key] * (Math.PI / 180);
    }

    var sin_x1 = Math.sin(ax), cos_x1 = Math.cos(ax);
    var sin_y1 = Math.sin(ay), cos_y1 = Math.cos(ay);
    var sin_x2 = Math.sin(bx), cos_x2 = Math.cos(bx);
    var sin_y2 = Math.sin(by), cos_y2 = Math.cos(by);

    var cross_prod = (cos_y1 * cos_x1 * cos_y2 * cos_x2) + (cos_y1 * sin_x1 * cos_y2 * sin_x2) +
        (sin_y1 * sin_y2);

    if (cross_prod >= 1 || cross_prod <= -1) {
        // fun with floats
        assert(Math.abs(cross_prod) - 1 < 1e-6);
        return cross_prod > 0 ? 0 : Math.PI;
    }

    return Math.acos(cross_prod);
};

rs = function() {
    return "try rs.help()";
};

/**
 * This method is intended to aid in the writing of tests. It takes a host's address, desired state,
 * and replicaset and waits either timeout milliseconds or until that reaches the desired state.
 *
 * It should be used instead of awaitRSClientHost when there is no MongoS with a connection to the
 * replica set.
 */
_awaitRSHostViaRSMonitor = function(hostAddr, desiredState, rsName, timeout) {
    timeout = timeout || 60 * 1000;

    if (desiredState == undefined) {
        desiredState = {
            ok: true
        };
    }

    print("Awaiting " + hostAddr + " to be " + tojson(desiredState) + " in " + " rs " + rsName);

    var tests = 0;
    assert.soon(
        function() {
            var stats = _replMonitorStats(rsName);
            if (tests++ % 10 == 0) {
                printjson(stats);
            }

            for (var i = 0; i < stats.length; i++) {
                var node = stats[i];
                printjson(node);
                if (node["addr"] !== hostAddr)
                    continue;

                // Check that *all* hostAddr properties match desiredState properties
                var stateReached = true;
                for (var prop in desiredState) {
                    if (isObject(desiredState[prop])) {
                        if (!friendlyEqual(sortDoc(desiredState[prop]), sortDoc(node[prop]))) {
                            stateReached = false;
                            break;
                        }
                    } else if (node[prop] !== desiredState[prop]) {
                        stateReached = false;
                        break;
                    }
                }
                if (stateReached) {
                    printjson(stats);
                    return true;
                }
            }
            return false;
        },
        "timed out waiting for replica set member: " + hostAddr + " to reach state: " +
            tojson(desiredState),
        timeout);
};

rs.help = function() {
    print(
        "\trs.status()                                { replSetGetStatus : 1 } checks repl set status");
    print(
        "\trs.initiate()                              { replSetInitiate : null } initiates set with default settings");
    print(
        "\trs.initiate(cfg)                           { replSetInitiate : cfg } initiates set with configuration cfg");
    print(
        "\trs.conf()                                  get the current configuration object from local.system.replset");
    print(
        "\trs.reconfig(cfg)                           updates the configuration of a running replica set with cfg (disconnects)");
    print(
        "\trs.add(hostportstr)                        add a new member to the set with default attributes (disconnects)");
    print(
        "\trs.add(membercfgobj)                       add a new member to the set with extra attributes (disconnects)");
    print(
        "\trs.addArb(hostportstr)                     add a new member which is arbiterOnly:true (disconnects)");
    print("\trs.stepDown([stepdownSecs, catchUpSecs])   step down as primary (disconnects)");
    print(
        "\trs.syncFrom(hostportstr)                   make a secondary sync from the given member");
    print(
        "\trs.freeze(secs)                            make a node ineligible to become primary for the time specified");
    print(
        "\trs.remove(hostportstr)                     remove a host from the replica set (disconnects)");
    print("\trs.slaveOk()                               allow queries on secondary nodes");
    print();
    print("\trs.printReplicationInfo()                  check oplog size and time range");
    print(
        "\trs.printSlaveReplicationInfo()             check replica set members and replication lag");
    print("\tdb.isMaster()                              check who is primary");
    print();
    print("\treconfiguration helpers disconnect from the database so the shell will display");
    print("\tan error, even if the command succeeds.");
};
rs.slaveOk = function(value) {
    return db.getMongo().setSlaveOk(value);
};
rs.status = function() {
    return db._adminCommand("replSetGetStatus");
};
rs.isMaster = function() {
    return db.isMaster();
};
rs.initiate = function(c) {
    return db._adminCommand({replSetInitiate: c});
};
rs.printSlaveReplicationInfo = function() {
    return db.printSlaveReplicationInfo();
};
rs.printReplicationInfo = function() {
    return db.printReplicationInfo();
};
rs._runCmd = function(c) {
    // after the command, catch the disconnect and reconnect if necessary
    var res = null;
    try {
        res = db.adminCommand(c);
    } catch (e) {
        if (("" + e).indexOf("error doing query") >= 0) {
            // closed connection.  reconnect.
            db.getLastErrorObj();
            var o = db.getLastErrorObj();
            if (o.ok) {
                print("reconnected to server after rs command (which is normal)");
            } else {
                printjson(o);
            }
        } else {
            print("shell got exception during repl set operation: " + e);
            print(
                "in some circumstances, the primary steps down and closes connections on a reconfig");
        }
        return "";
    }
    return res;
};
rs.reconfig = function(cfg, options) {
    cfg.version = rs.conf().version + 1;
    cmd = {
        replSetReconfig: cfg
    };
    for (var i in options) {
        cmd[i] = options[i];
    }
    return this._runCmd(cmd);
};
rs.add = function(hostport, arb) {
    var cfg = hostport;

    var local = db.getSisterDB("local");
    assert(local.system.replset.count() <= 1,
           "error: local.system.replset has unexpected contents");
    var c = local.system.replset.findOne();
    assert(c, "no config object retrievable from local.system.replset");

    c.version++;

    var max = 0;
    for (var i in c.members)
        if (c.members[i]._id > max)
            max = c.members[i]._id;
    if (isString(hostport)) {
        cfg = {
            _id: max + 1,
            host: hostport
        };
        if (arb)
            cfg.arbiterOnly = true;
    } else if (arb == true) {
        throw Error("Expected first parameter to be a host-and-port string of arbiter, but got " +
                    tojson(hostport));
    }

    if (cfg._id == null) {
        cfg._id = max + 1;
    }
    c.members.push(cfg);
    return this._runCmd({replSetReconfig: c});
};
rs.syncFrom = function(host) {
    return db._adminCommand({replSetSyncFrom: host});
};
rs.stepDown = function(stepdownSecs, catchUpSecs) {
    var cmdObj = {
        replSetStepDown: stepdownSecs === undefined ? 60 : stepdownSecs
    };
    if (catchUpSecs !== undefined) {
        cmdObj['secondaryCatchUpPeriodSecs'] = catchUpSecs;
    }
    return db._adminCommand(cmdObj);
};
rs.freeze = function(secs) {
    return db._adminCommand({replSetFreeze: secs});
};
rs.addArb = function(hn) {
    return this.add(hn, true);
};

rs.conf = function() {
    var resp = db._adminCommand({replSetGetConfig: 1});
    if (resp.ok && !(resp.errmsg) && resp.config)
        return resp.config;
    else if (resp.errmsg && resp.errmsg.startsWith("no such cmd"))
        return db.getSisterDB("local").system.replset.findOne();
    throw new Error("Could not retrieve replica set config: " + tojson(resp));
};
rs.config = rs.conf;

rs.remove = function(hn) {
    var local = db.getSisterDB("local");
    assert(local.system.replset.count() <= 1,
           "error: local.system.replset has unexpected contents");
    var c = local.system.replset.findOne();
    assert(c, "no config object retrievable from local.system.replset");
    c.version++;

    for (var i in c.members) {
        if (c.members[i].host == hn) {
            c.members.splice(i, 1);
            return db._adminCommand({replSetReconfig: c});
        }
    }

    return "error: couldn't find " + hn + " in " + tojson(c.members);
};

rs.debug = {};

rs.debug.nullLastOpWritten = function(primary, secondary) {
    var p = connect(primary + "/local");
    var s = connect(secondary + "/local");
    s.getMongo().setSlaveOk();

    var secondToLast = s.oplog.rs.find().sort({$natural: -1}).limit(1).next();
    var last = p.runCommand({
        findAndModify: "oplog.rs",
        query: {ts: {$gt: secondToLast.ts}},
        sort: {$natural: 1},
        update: {$set: {op: "n"}}
    });

    if (!last.value.o || !last.value.o._id) {
        print("couldn't find an _id?");
    } else {
        last.value.o = {
            _id: last.value.o._id
        };
    }

    print("nulling out this op:");
    printjson(last);
};

rs.debug.getLastOpWritten = function(server) {
    var s = db.getSisterDB("local");
    if (server) {
        s = connect(server + "/local");
    }
    s.getMongo().setSlaveOk();

    return s.oplog.rs.find().sort({$natural: -1}).limit(1).next();
};

help = shellHelper.help = function(x) {
    if (x == "mr") {
        print("\nSee also http://dochub.mongodb.org/core/mapreduce");
        print("\nfunction mapf() {");
        print("  // 'this' holds current document to inspect");
        print("  emit(key, value);");
        print("}");
        print("\nfunction reducef(key,value_array) {");
        print("  return reduced_value;");
        print("}");
        print("\ndb.mycollection.mapReduce(mapf, reducef[, options])");
        print("\noptions");
        print("{[query : <query filter object>]");
        print(" [, sort : <sort the query.  useful for optimization>]");
        print(" [, limit : <number of objects to return from collection>]");
        print(" [, out : <output-collection name>]");
        print(" [, keeptemp: <true|false>]");
        print(" [, finalize : <finalizefunction>]");
        print(" [, scope : <object where fields go into javascript global scope >]");
        print(" [, verbose : true]}\n");
        return;
    } else if (x == "connect") {
        print(
            "\nNormally one specifies the server on the mongo shell command line.  Run mongo --help to see those options.");
        print("Additional connections may be opened:\n");
        print("    var x = new Mongo('host[:port]');");
        print("    var mydb = x.getDB('mydb');");
        print("  or");
        print("    var mydb = connect('host[:port]/mydb');");
        print(
            "\nNote: the REPL prompt only auto-reports getLastError() for the shell command line connection.\n");
        return;
    } else if (x == "keys") {
        print("Tab completion and command history is available at the command prompt.\n");
        print("Some emacs keystrokes are available too:");
        print("  Ctrl-A start of line");
        print("  Ctrl-E end of line");
        print("  Ctrl-K del to end of line");
        print("\nMulti-line commands");
        print(
            "You can enter a multi line javascript expression.  If parens, braces, etc. are not closed, you will see a new line ");
        print(
            "beginning with '...' characters.  Type the rest of your expression.  Press Ctrl-C to abort the data entry if you");
        print("get stuck.\n");
    } else if (x == "misc") {
        print("\tb = new BinData(subtype,base64str)  create a BSON BinData value");
        print("\tb.subtype()                         the BinData subtype (0..255)");
        print("\tb.length()                          length of the BinData data in bytes");
        print("\tb.hex()                             the data as a hex encoded string");
        print("\tb.base64()                          the data as a base 64 encoded string");
        print("\tb.toString()");
        print();
        print(
            "\tb = HexData(subtype,hexstr)         create a BSON BinData value from a hex string");
        print("\tb = UUID(hexstr)                    create a BSON BinData value of UUID subtype");
        print("\tb = MD5(hexstr)                     create a BSON BinData value of MD5 subtype");
        print(
            "\t\"hexstr\"                            string, sequence of hex characters (no 0x prefix)");
        print();
        print("\to = new ObjectId()                  create a new ObjectId");
        print(
            "\to.getTimestamp()                    return timestamp derived from first 32 bits of the OID");
        print("\to.isObjectId");
        print("\to.toString()");
        print("\to.equals(otherid)");
        print();
        print(
            "\td = ISODate()                       like Date() but behaves more intuitively when used");
        print(
            "\td = ISODate('YYYY-MM-DD hh:mm:ss')    without an explicit \"new \" prefix on construction");
        return;
    } else if (x == "admin") {
        print("\tls([path])                      list files");
        print("\tpwd()                           returns current directory");
        print("\tlistFiles([path])               returns file list");
        print("\thostname()                      returns name of this host");
        print("\tcat(fname)                      returns contents of text file as a string");
        print("\tremoveFile(f)                   delete a file or directory");
        print("\tload(jsfilename)                load and execute a .js file");
        print("\trun(program[, args...])         spawn a program and wait for its completion");
        print("\trunProgram(program[, args...])  same as run(), above");
        print("\tsleep(m)                        sleep m milliseconds");
        print("\tgetMemInfo()                    diagnostic");
        return;
    } else if (x == "test") {
        print("\tMongoRunner.runMongod(args)   DELETES DATA DIR and then starts mongod");
        print("\t                              returns a connection to the new server");
        return;
    } else if (x == "") {
        print("\t" + "db.help()                    help on db methods");
        print("\t" + "db.mycoll.help()             help on collection methods");
        print("\t" + "sh.help()                    sharding helpers");
        print("\t" + "rs.help()                    replica set helpers");
        print("\t" + "help admin                   administrative help");
        print("\t" + "help connect                 connecting to a db help");
        print("\t" + "help keys                    key shortcuts");
        print("\t" + "help misc                    misc things to know");
        print("\t" + "help mr                      mapreduce");
        print();
        print("\t" + "show dbs                     show database names");
        print("\t" + "show collections             show collections in current database");
        print("\t" + "show users                   show users in current database");
        print(
            "\t" +
            "show profile                 show most recent system.profile entries with time >= 1ms");
        print("\t" + "show logs                    show the accessible logger names");
        print(
            "\t" +
            "show log [name]              prints out the last segment of log in memory, 'global' is default");
        print("\t" + "use <db_name>                set current database");
        print("\t" + "db.foo.find()                list objects in collection foo");
        print("\t" + "db.foo.find( { a : 1 } )     list objects in foo where a == 1");
        print(
            "\t" +
            "it                           result of the last line evaluated; use to further iterate");
        print("\t" +
              "DBQuery.shellBatchSize = x   set default number of items to display on shell");
        print("\t" + "exit                         quit the mongo shell");
    } else
        print("unknown help option");
};



// ---- MODULE: utils_sh ---- 
sh = function() {
    return "try sh.help();";
};

sh._checkMongos = function() {
    var x = db.runCommand("ismaster");
    if (x.msg != "isdbgrid")
        throw Error("not connected to a mongos");
};

sh._checkFullName = function(fullName) {
    assert(fullName, "need a full name");
    assert(fullName.indexOf(".") > 0, "name needs to be fully qualified <db>.<collection>'");
};

sh._adminCommand = function(cmd, skipCheck) {
    if (!skipCheck)
        sh._checkMongos();
    return db.getSisterDB("admin").runCommand(cmd);
};

sh._getConfigDB = function() {
    sh._checkMongos();
    return db.getSiblingDB("config");
};

sh._dataFormat = function(bytes) {
    if (bytes < 1024)
        return Math.floor(bytes) + "B";
    if (bytes < 1024 * 1024)
        return Math.floor(bytes / 1024) + "KiB";
    if (bytes < 1024 * 1024 * 1024)
        return Math.floor((Math.floor(bytes / 1024) / 1024) * 100) / 100 + "MiB";
    return Math.floor((Math.floor(bytes / (1024 * 1024)) / 1024) * 100) / 100 + "GiB";
};

sh._collRE = function(coll) {
    return RegExp("^" + RegExp.escape(coll + "") + "-.*");
};

sh._pchunk = function(chunk) {
    return "[" + tojson(chunk.min) + " -> " + tojson(chunk.max) + "]";
};

sh.help = function() {
    print("\tsh.addShard( host )                       server:port OR setname/server:port");
    print("\tsh.enableSharding(dbname)                 enables sharding on the database dbname");
    print("\tsh.shardCollection(fullName,key,unique)   shards the collection");

    print(
        "\tsh.splitFind(fullName,find)               splits the chunk that find is in at the median");
    print(
        "\tsh.splitAt(fullName,middle)               splits the chunk that middle is in at middle");
    print(
        "\tsh.moveChunk(fullName,find,to)            move the chunk where 'find' is to 'to' (name of shard)");

    print(
        "\tsh.setBalancerState( <bool on or not> )   turns the balancer on or off true=on, false=off");
    print("\tsh.getBalancerState()                     return true if enabled");
    print(
        "\tsh.isBalancerRunning()                    return true if the balancer has work in progress on any mongos");

    print("\tsh.disableBalancing(coll)                 disable balancing on one collection");
    print("\tsh.enableBalancing(coll)                  re-enable balancing on one collection");

    print("\tsh.addShardTag(shard,tag)                 adds the tag to the shard");
    print("\tsh.removeShardTag(shard,tag)              removes the tag from the shard");
    print(
        "\tsh.addTagRange(fullName,min,max,tag)      tags the specified range of the given collection");
    print(
        "\tsh.removeTagRange(fullName,min,max,tag)   removes the tagged range of the given collection");

    print("\tsh.status()                               prints a general overview of the cluster");
};

sh.status = function(verbose, configDB) {
    // TODO: move the actual command here
    printShardingStatus(configDB, verbose);
};

sh.addShard = function(url) {
    return sh._adminCommand({addShard: url}, true);
};

sh.enableSharding = function(dbname) {
    assert(dbname, "need a valid dbname");
    return sh._adminCommand({enableSharding: dbname});
};

sh.shardCollection = function(fullName, key, unique) {
    sh._checkFullName(fullName);
    assert(key, "need a key");
    assert(typeof(key) == "object", "key needs to be an object");

    var cmd = {
        shardCollection: fullName,
        key: key
    };
    if (unique)
        cmd.unique = true;

    return sh._adminCommand(cmd);
};

sh.splitFind = function(fullName, find) {
    sh._checkFullName(fullName);
    return sh._adminCommand({split: fullName, find: find});
};

sh.splitAt = function(fullName, middle) {
    sh._checkFullName(fullName);
    return sh._adminCommand({split: fullName, middle: middle});
};

sh.moveChunk = function(fullName, find, to) {
    sh._checkFullName(fullName);
    return sh._adminCommand({moveChunk: fullName, find: find, to: to});
};

sh.setBalancerState = function(onOrNot) {
    return sh._getConfigDB().settings.update(
        {_id: 'balancer'},
        {$set: {stopped: onOrNot ? false : true}},
        {upsert: true, writeConcern: {w: 'majority', timeout: 30}});
};

sh.getBalancerState = function(configDB) {
    if (configDB === undefined)
        configDB = sh._getConfigDB();
    var x = configDB.settings.findOne({_id: "balancer"});
    if (x == null)
        return true;
    return !x.stopped;
};

sh.isBalancerRunning = function(configDB) {
    if (configDB === undefined)
        configDB = sh._getConfigDB();
    var x = configDB.locks.findOne({_id: "balancer"});
    if (x == null) {
        print("config.locks collection empty or missing. be sure you are connected to a mongos");
        return false;
    }
    return x.state > 0;
};

sh.getBalancerHost = function(configDB) {
    if (configDB === undefined)
        configDB = sh._getConfigDB();
    var x = configDB.locks.findOne({_id: "balancer"});
    if (x == null) {
        print(
            "config.locks collection does not contain balancer lock. be sure you are connected to a mongos");
        return "";
    }
    return x.process.match(/[^:]+:[^:]+/)[0];
};

sh.stopBalancer = function(timeout, interval) {
    sh.setBalancerState(false);
    sh.waitForBalancer(false, timeout, interval);
};

sh.startBalancer = function(timeout, interval) {
    sh.setBalancerState(true);
    sh.waitForBalancer(true, timeout, interval);
};

sh.waitForDLock = function(lockId, onOrNot, timeout, interval) {
    // Wait for balancer to be on or off
    // Can also wait for particular balancer state
    var state = onOrNot;
    var configDB = sh._getConfigDB();

    var beginTS = undefined;
    if (state == undefined) {
        var currLock = configDB.locks.findOne({_id: lockId});
        if (currLock != null)
            beginTS = currLock.ts;
    }

    var lockStateOk = function() {
        var lock = configDB.locks.findOne({_id: lockId});

        if (state == false)
            return !lock || lock.state == 0;
        if (state == true)
            return lock && lock.state == 2;
        if (state == undefined)
            return (beginTS == undefined && lock) ||
                (beginTS != undefined && (!lock || lock.ts + "" != beginTS + ""));
        else
            return lock && lock.state == state;
    };

    assert.soon(
        lockStateOk,
        "Waited too long for lock " + lockId + " to " +
            (state == true ? "lock" : (state == false ? "unlock" : "change to state " + state)),
        timeout,
        interval);
};

sh.waitForPingChange = function(activePings, timeout, interval) {

    var isPingChanged = function(activePing) {
        var newPing = sh._getConfigDB().mongos.findOne({_id: activePing._id});
        return !newPing || newPing.ping + "" != activePing.ping + "";
    };

    // First wait for all active pings to change, so we're sure a settings reload
    // happened

    // Timeout all pings on the same clock
    var start = new Date();

    var remainingPings = [];
    for (var i = 0; i < activePings.length; i++) {
        var activePing = activePings[i];
        print("Waiting for active host " + activePing._id +
              " to recognize new settings... (ping : " + activePing.ping + ")");

        // Do a manual timeout here, avoid scary assert.soon errors
        var timeout = timeout || 30000;
        var interval = interval || 200;
        while (isPingChanged(activePing) != true) {
            if ((new Date()).getTime() - start.getTime() > timeout) {
                print("Waited for active ping to change for host " + activePing._id +
                      ", a migration may be in progress or the host may be down.");
                remainingPings.push(activePing);
                break;
            }
            sleep(interval);
        }
    }

    return remainingPings;
};

sh.waitForBalancerOff = function(timeout, interval) {
    var pings = sh._getConfigDB().mongos.find().toArray();
    var activePings = [];
    for (var i = 0; i < pings.length; i++) {
        if (!pings[i].waiting)
            activePings.push(pings[i]);
    }

    print("Waiting for active hosts...");

    activePings = sh.waitForPingChange(activePings, 60 * 1000);

    // After 1min, we assume that all hosts with unchanged pings are either
    // offline (this is enough time for a full errored balance round, if a network
    // issue, which would reload settings) or balancing, which we wait for next
    // Legacy hosts we always have to wait for

    print("Waiting for the balancer lock...");

    // Wait for the balancer lock to become inactive
    // We can guess this is stale after 15 mins, but need to double-check manually
    try {
        sh.waitForDLock("balancer", false, 15 * 60 * 1000);
    } catch (e) {
        print(
            "Balancer still may be active, you must manually verify this is not the case using the config.changelog collection.");
        throw Error(e);
    }

    print("Waiting again for active hosts after balancer is off...");

    // Wait a short time afterwards, to catch the host which was balancing earlier
    activePings = sh.waitForPingChange(activePings, 5 * 1000);

    // Warn about all the stale host pings remaining
    for (var i = 0; i < activePings.length; i++) {
        print("Warning : host " + activePings[i]._id + " seems to have been offline since " +
              activePings[i].ping);
    }

};

sh.waitForBalancer = function(onOrNot, timeout, interval) {

    // If we're waiting for the balancer to turn on or switch state or
    // go to a particular state
    if (onOrNot) {
        // Just wait for the balancer lock to change, can't ensure we'll ever see it
        // actually locked
        sh.waitForDLock("balancer", undefined, timeout, interval);
    } else {
        // Otherwise we need to wait until we're sure balancing stops
        sh.waitForBalancerOff(timeout, interval);
    }

};

sh.disableBalancing = function(coll) {
    if (coll === undefined) {
        throw Error("Must specify collection");
    }
    var dbase = db;
    if (coll instanceof DBCollection) {
        dbase = coll.getDB();
    } else {
        sh._checkMongos();
    }

    dbase.getSisterDB("config").collections.update({_id: coll + ""}, {$set: {"noBalance": true}});
};

sh.enableBalancing = function(coll) {
    if (coll === undefined) {
        throw Error("Must specify collection");
    }
    var dbase = db;
    if (coll instanceof DBCollection) {
        dbase = coll.getDB();
    } else {
        sh._checkMongos();
    }

    dbase.getSisterDB("config").collections.update({_id: coll + ""}, {$set: {"noBalance": false}});
};

/*
 * Can call _lastMigration( coll ), _lastMigration( db ), _lastMigration( st ), _lastMigration(
 * mongos )
 */
sh._lastMigration = function(ns) {

    var coll = null;
    var dbase = null;
    var config = null;

    if (!ns) {
        config = db.getSisterDB("config");
    } else if (ns instanceof DBCollection) {
        coll = ns;
        config = coll.getDB().getSisterDB("config");
    } else if (ns instanceof DB) {
        dbase = ns;
        config = dbase.getSisterDB("config");
    } else if (ns instanceof ShardingTest) {
        config = ns.s.getDB("config");
    } else if (ns instanceof Mongo) {
        config = ns.getDB("config");
    } else {
        // String namespace
        ns = ns + "";
        if (ns.indexOf(".") > 0) {
            config = db.getSisterDB("config");
            coll = db.getMongo().getCollection(ns);
        } else {
            config = db.getSisterDB("config");
            dbase = db.getSisterDB(ns);
        }
    }

    var searchDoc = {
        what: /^moveChunk/
    };
    if (coll)
        searchDoc.ns = coll + "";
    if (dbase)
        searchDoc.ns = new RegExp("^" + dbase + "\\.");

    var cursor = config.changelog.find(searchDoc).sort({time: -1}).limit(1);
    if (cursor.hasNext())
        return cursor.next();
    else
        return null;
};

sh._checkLastError = function(mydb) {
    var errObj = mydb.getLastErrorObj();
    if (errObj.err)
        throw _getErrorWithCode(errObj, "error: " + errObj.err);
};

sh.addShardTag = function(shard, tag) {
    var config = sh._getConfigDB();
    if (config.shards.findOne({_id: shard}) == null) {
        throw Error("can't find a shard with name: " + shard);
    }
    config.shards.update({_id: shard}, {$addToSet: {tags: tag}});
    sh._checkLastError(config);
};

sh.removeShardTag = function(shard, tag) {
    var config = sh._getConfigDB();
    if (config.shards.findOne({_id: shard}) == null) {
        throw Error("can't find a shard with name: " + shard);
    }
    config.shards.update({_id: shard}, {$pull: {tags: tag}});
    sh._checkLastError(config);
};

sh.addTagRange = function(ns, min, max, tag) {
    if (bsonWoCompare(min, max) == 0) {
        throw new Error("min and max cannot be the same");
    }

    var config = sh._getConfigDB();
    config.tags.update({_id: {ns: ns, min: min}},
                       {_id: {ns: ns, min: min}, ns: ns, min: min, max: max, tag: tag},
                       true);
    sh._checkLastError(config);
};

sh.removeTagRange = function(ns, min, max, tag) {
    var config = sh._getConfigDB();
    // warn if the namespace does not exist, even dropped
    if (config.collections.findOne({_id: ns}) == null) {
        print("Warning: can't find the namespace: " + ns + " - collection likely never sharded");
    }
    // warn if the tag being removed is still in use
    if (config.shards.findOne({tags: tag})) {
        print("Warning: tag still in use by at least one shard");
    }
    // max and tag criteria not really needed, but including them avoids potentially unexpected
    // behavior.
    config.tags.remove({_id: {ns: ns, min: min}, max: max, tag: tag});
    sh._checkLastError(config);
};

sh.getBalancerLockDetails = function(configDB) {
    if (configDB === undefined)
        configDB = db.getSiblingDB('config');
    var lock = configDB.locks.findOne({_id: 'balancer'});
    if (lock == null) {
        return null;
    }
    if (lock.state == 0) {
        return null;
    }
    return lock;
};

sh.getBalancerWindow = function(configDB) {
    if (configDB === undefined)
        configDB = db.getSiblingDB('config');
    var settings = configDB.settings.findOne({_id: 'balancer'});
    if (settings == null) {
        return null;
    }
    if (settings.hasOwnProperty("activeWindow")) {
        return settings.activeWindow;
    }
    return null;
};

sh.getActiveMigrations = function(configDB) {
    if (configDB === undefined)
        configDB = db.getSiblingDB('config');
    var activeLocks = configDB.locks.find({_id: {$ne: "balancer"}, state: {$eq: 2}});
    var result = [];
    if (activeLocks != null) {
        activeLocks.forEach(function(lock) {
            result.push({_id: lock._id, when: lock.when});
        });
    }
    return result;
};

sh.getRecentFailedRounds = function(configDB) {
    if (configDB === undefined)
        configDB = db.getSiblingDB('config');
    var balErrs = configDB.actionlog.find({what: "balancer.round"}).sort({time: -1}).limit(5);
    var result = {
        count: 0,
        lastErr: "",
        lastTime: " "
    };
    if (balErrs != null) {
        balErrs.forEach(function(r) {
            if (r.details.errorOccured) {
                result.count += 1;
                result.lastErr = r.details.errmsg;
                result.lastTime = r.time;
            }
        });
    }
    return result;
};

/**
 * Returns a summary of chunk migrations that was completed either successfully or not
 * since yesterday. The format is an array of 2 arrays, where the first array contains
 * the successful cases, and the second array contains the failure cases.
 */
sh.getRecentMigrations = function(configDB) {
    if (configDB === undefined)
        configDB = sh._getConfigDB();
    var yesterday = new Date(new Date() - 24 * 60 * 60 * 1000);

    // Successful migrations.
    var result = configDB.changelog.aggregate([
        {
          $match: {
              time: {$gt: yesterday},
              what: "moveChunk.from", 'details.errmsg': {$exists: false}, 'details.note': 'success'
          }
        },
        {$group: {_id: {msg: "$details.errmsg"}, count: {$sum: 1}}},
        {$project: {_id: {$ifNull: ["$_id.msg", "Success"]}, count: "$count"}}
    ]).toArray();

    // Failed migrations.
    result = result.concat(configDB.changelog.aggregate([
        {
          $match: {
              time: {$gt: yesterday},
              what: "moveChunk.from",
              $or: [{'details.errmsg': {$exists: true}}, {'details.note': {$ne: 'success'}}]
          }
        },
        {
          $group: {
              _id: {msg: "$details.errmsg", from: "$details.from", to: "$details.to"},
              count: {$sum: 1}
          }
        },
        {
          $project: {
              _id: {$ifNull: ['$_id.msg', 'aborted']},
              from: "$_id.from",
              to: "$_id.to",
              count: "$count"
          }
        }
    ]).toArray());

    return result;
};

function printShardingStatus(configDB, verbose) {
    // configDB is a DB object that contains the sharding metadata of interest.
    // Defaults to the db named "config" on the current connection.
    if (configDB === undefined)
        configDB = db.getSisterDB('config');

    var version = configDB.getCollection("version").findOne();
    if (version == null) {
        print(
            "printShardingStatus: this db does not have sharding enabled. be sure you are connecting to a mongos from the shell and not to a mongod.");
        return;
    }

    var raw = "";
    var output = function(s) {
        raw += s + "\n";
    };
    output("--- Sharding Status --- ");
    output("  sharding version: " + tojson(configDB.getCollection("version").findOne()));

    output("  shards:");
    configDB.shards.find().sort({_id: 1}).forEach(function(z) {
        output("\t" + tojsononeline(z));
    });

    // (most recently) active mongoses
    var mongosActiveThresholdMs = 60000;
    var mostRecentMongos = configDB.mongos.find().sort({ping: -1}).limit(1);
    var mostRecentMongosTime = null;
    var mongosAdjective = "most recently active";
    if (mostRecentMongos.hasNext()) {
        mostRecentMongosTime = mostRecentMongos.next().ping;
        // Mongoses older than the threshold are the most recent, but cannot be
        // considered "active" mongoses. (This is more likely to be an old(er)
        // configdb dump, or all the mongoses have been stopped.)
        if (mostRecentMongosTime.getTime() >= Date.now() - mongosActiveThresholdMs) {
            mongosAdjective = "active";
        }
    }

    output("  " + mongosAdjective + " mongoses:");
    if (mostRecentMongosTime === null) {
        output("\tnone");
    } else {
        var recentMongosQuery = {
            ping: {
                $gt: (function() {
                    var d = mostRecentMongosTime;
                    d.setTime(d.getTime() - mongosActiveThresholdMs);
                    return d;
                })()
            }
        };

        if (verbose) {
            configDB.mongos.find(recentMongosQuery)
                .sort({ping: -1})
                .forEach(function(z) {
                    output("\t" + tojsononeline(z));
                });
        } else {
            configDB.mongos.aggregate([
                {$match: recentMongosQuery},
                {$group: {_id: "$mongoVersion", num: {$sum: 1}}},
                {$sort: {num: -1}}
            ])
                .forEach(function(z) {
                    output("\t" + tojson(z._id) + " : " + z.num);
                });
        }
    }

    output("  balancer:");

    // Is the balancer currently enabled
    output("\tCurrently enabled:  " + (sh.getBalancerState(configDB) ? "yes" : "no"));

    // Is the balancer currently active
    output("\tCurrently running:  " + (sh.isBalancerRunning(configDB) ? "yes" : "no"));

    // Output details of the current balancer round
    var balLock = sh.getBalancerLockDetails(configDB);
    if (balLock) {
        output("\t\tBalancer lock taken at " + balLock.when + " by " + balLock.who);
    }

    // Output the balancer window
    var balSettings = sh.getBalancerWindow(configDB);
    if (balSettings) {
        output("\t\tBalancer active window is set between " + balSettings.start + " and " +
               balSettings.stop + " server local time");
    }

    // Output the list of active migrations
    var activeMigrations = sh.getActiveMigrations(configDB);
    if (activeMigrations.length > 0) {
        output("\tCollections with active migrations: ");
        activeMigrations.forEach(function(migration) {
            output("\t\t" + migration._id + " started at " + migration.when);
        });
    }

    // Actionlog and version checking only works on 2.7 and greater
    var versionHasActionlog = false;
    var metaDataVersion = configDB.getCollection("version").findOne().currentVersion;
    if (metaDataVersion > 5) {
        versionHasActionlog = true;
    }
    if (metaDataVersion == 5) {
        var verArray = db.serverBuildInfo().versionArray;
        if (verArray[0] == 2 && verArray[1] > 6) {
            versionHasActionlog = true;
        }
    }

    if (versionHasActionlog) {
        // Review config.actionlog for errors
        var actionReport = sh.getRecentFailedRounds(configDB);
        // Always print the number of failed rounds
        output("\tFailed balancer rounds in last 5 attempts:  " + actionReport.count);

        // Only print the errors if there are any
        if (actionReport.count > 0) {
            output("\tLast reported error:  " + actionReport.lastErr);
            output("\tTime of Reported error:  " + actionReport.lastTime);
        }

        output("\tMigration Results for the last 24 hours: ");
        var migrations = sh.getRecentMigrations(configDB);
        if (migrations.length > 0) {
            migrations.forEach(function(x) {
                if (x._id === "Success") {
                    output("\t\t" + x.count + " : " + x._id);
                } else {
                    output("\t\t" + x.count + " : Failed with error '" + x._id + "', from " +
                           x.from + " to " + x.to);
                }
            });
        } else {
            output("\t\tNo recent migrations");
        }
    }

    output("  databases:");
    configDB.databases.find().sort({name: 1}).forEach(function(db) {
        var truthy = function(value) {
            return !!value;
        };
        var nonBooleanNote = function(name, value) {
            // If the given value is not a boolean, return a string of the
            // form " (<name>: <value>)", where <value> is converted to JSON.
            var t = typeof(value);
            var s = "";
            if (t != "boolean" && t != "undefined") {
                s = " (" + name + ": " + tojson(value) + ")";
            }
            return s;
        };

        output("\t" + tojsononeline(db, "", true));

        if (db.partitioned) {
            configDB.collections.find({_id: new RegExp("^" + RegExp.escape(db._id) + "\\.")})
                .sort({_id: 1})
                .forEach(function(coll) {
                    if (!coll.dropped) {
                        output("\t\t" + coll._id);
                        output("\t\t\tshard key: " + tojson(coll.key));
                        output("\t\t\tunique: " + truthy(coll.unique) +
                               nonBooleanNote("unique", coll.unique));
                        output("\t\t\tbalancing: " + !truthy(coll.noBalance) +
                               nonBooleanNote("noBalance", coll.noBalance));
                        output("\t\t\tchunks:");

                        res = configDB.chunks
                                  .aggregate({$match: {ns: coll._id}},
                                             {$group: {_id: "$shard", cnt: {$sum: 1}}},
                                             {$project: {_id: 0, shard: "$_id", nChunks: "$cnt"}},
                                             {$sort: {shard: 1}})
                                  .toArray();
                        var totalChunks = 0;
                        res.forEach(function(z) {
                            totalChunks += z.nChunks;
                            output("\t\t\t\t" + z.shard + "\t" + z.nChunks);
                        });

                        if (totalChunks < 20 || verbose) {
                            configDB.chunks.find({"ns": coll._id})
                                .sort({min: 1})
                                .forEach(function(chunk) {
                                    output("\t\t\t" + tojson(chunk.min) + " -->> " +
                                           tojson(chunk.max) + " on : " + chunk.shard + " " +
                                           tojson(chunk.lastmod) + " " +
                                           (chunk.jumbo ? "jumbo " : ""));
                                });
                        } else {
                            output(
                                "\t\t\ttoo many chunks to print, use verbose if you want to force print");
                        }

                        configDB.tags.find({ns: coll._id})
                            .sort({min: 1})
                            .forEach(function(tag) {
                                output("\t\t\t tag: " + tag.tag + "  " + tojson(tag.min) +
                                       " -->> " + tojson(tag.max));
                            });
                    }
                });
        }
    });

    print(raw);
}

function printShardingSizes(configDB) {
    // configDB is a DB object that contains the sharding metadata of interest.
    // Defaults to the db named "config" on the current connection.
    if (configDB === undefined)
        configDB = db.getSisterDB('config');

    var version = configDB.getCollection("version").findOne();
    if (version == null) {
        print("printShardingSizes : not a shard db!");
        return;
    }

    var raw = "";
    var output = function(s) {
        raw += s + "\n";
    };
    output("--- Sharding Status --- ");
    output("  sharding version: " + tojson(configDB.getCollection("version").findOne()));

    output("  shards:");
    var shards = {};
    configDB.shards.find().forEach(function(z) {
        shards[z._id] = new Mongo(z.host);
        output("      " + tojson(z));
    });

    var saveDB = db;
    output("  databases:");
    configDB.databases.find().sort({name: 1}).forEach(function(db) {
        output("\t" + tojson(db, "", true));

        if (db.partitioned) {
            configDB.collections.find({_id: new RegExp("^" + RegExp.escape(db._id) + "\.")})
                .sort({_id: 1})
                .forEach(function(coll) {
                    output("\t\t" + coll._id + " chunks:");
                    configDB.chunks.find({"ns": coll._id})
                        .sort({min: 1})
                        .forEach(function(chunk) {
                            var mydb = shards[chunk.shard].getDB(db._id);
                            var out = mydb.runCommand({
                                dataSize: coll._id,
                                keyPattern: coll.key,
                                min: chunk.min,
                                max: chunk.max
                            });
                            delete out.millis;
                            delete out.ok;

                            output("\t\t\t" + tojson(chunk.min) + " -->> " + tojson(chunk.max) +
                                   " on : " + chunk.shard + " " + tojson(out));

                        });
                });
        }
    });

    print(raw);
}



// ---- MODULE: port_connection ---- 
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


// ---- MODULE: codemirror_hinter ---- 
function mongoDBHintAdapter(cm, callback, options){
	var curDoc = cm.getDoc();
	var cursor = curDoc.getCursor();
	var db = options.connectedTab.state.db;
	var curLine = curDoc.getLine(cursor.line).substr(0, cursor.ch);
	var list = modifiedShellAutocomplete(curLine, db);
	callback({list:list, from: {line:cursor.line, ch:0}, to: cursor});

	/**
	 * Modified shellAutocomplete because theirs is not compatible with strict mode
	 */
	function modifiedShellAutocomplete(prefix, db){
		var shellAutocomplete = (function(
		    /*prefix*/) {  // outer scope function called on init. Actual function at end

		    var universalMethods =
		        "constructor prototype toString valueOf toLocaleString hasOwnProperty propertyIsEnumerable"
		            .split(' ');

		    var builtinMethods = {};  // uses constructor objects as keys
		    builtinMethods[Array] =
		        "length concat join pop push reverse shift slice sort splice unshift indexOf lastIndexOf every filter forEach map some isArray reduce reduceRight"
		            .split(' ');
		    builtinMethods[Boolean] = "".split(' ');  // nothing more than universal methods
		    builtinMethods[Date] =
		        "getDate getDay getFullYear getHours getMilliseconds getMinutes getMonth getSeconds getTime getTimezoneOffset getUTCDate getUTCDay getUTCFullYear getUTCHours getUTCMilliseconds getUTCMinutes getUTCMonth getUTCSeconds getYear parse setDate setFullYear setHours setMilliseconds setMinutes setMonth setSeconds setTime setUTCDate setUTCFullYear setUTCHours setUTCMilliseconds setUTCMinutes setUTCMonth setUTCSeconds setYear toDateString toGMTString toISOString toLocaleDateString toLocaleTimeString toTimeString toUTCString UTC now"
		            .split(' ');
		    if (typeof JSON != "undefined") {  // JSON is new in V8
		        builtinMethods["[object JSON]"] = "parse stringify".split(' ');
		    }
		    builtinMethods[Math] =
		        "E LN2 LN10 LOG2E LOG10E PI SQRT1_2 SQRT2 abs acos asin atan atan2 ceil cos exp floor log max min pow random round sin sqrt tan"
		            .split(' ');
		    builtinMethods[Number] =
		        "MAX_VALUE MIN_VALUE NEGATIVE_INFINITY POSITIVE_INFINITY toExponential toFixed toPrecision"
		            .split(' ');
		    builtinMethods[RegExp] =
		        "global ignoreCase lastIndex multiline source compile exec test".split(' ');
		    builtinMethods[String] =
		        "length charAt charCodeAt concat fromCharCode indexOf lastIndexOf match replace search slice split substr substring toLowerCase toUpperCase trim trimLeft trimRight"
		            .split(' ');
		    builtinMethods[Function] = "call apply bind".split(' ');
		    builtinMethods[Object] =
		        "bsonsize create defineProperty defineProperties getPrototypeOf keys seal freeze preventExtensions isSealed isFrozen isExtensible getOwnPropertyDescriptor getOwnPropertyNames"
		            .split(' ');

		    builtinMethods[Mongo] = "find update insert remove".split(' ');
		    builtinMethods[BinData] = "hex base64 length subtype".split(' ');

		    var extraGlobals =
		        "Infinity NaN undefined null true false decodeURI decodeURIComponent encodeURI encodeURIComponent escape eval isFinite isNaN parseFloat parseInt unescape Array Boolean Date Math Number RegExp String print load gc MinKey MaxKey Mongo NumberInt NumberLong ObjectId DBPointer UUID BinData HexData MD5 Map Timestamp JSON"
		            .split(' ');
		    if (typeof NumberDecimal !== 'undefined') {
		        extraGlobals[extraGlobals.length] = "NumberDecimal";
		    }

		    var isPrivate = function(name) {
		        if (shellAutocomplete.showPrivate)
		            return false;
		        if (name == '_id')
		            return false;
		        if (name[0] == '_')
		            return true;
		        if (name[name.length - 1] == '_')
		            return true;  // some native functions have an extra name_ method
		        return false;
		    };

		    var customComplete = function(obj) {
		        try {
		            if (obj.__proto__.constructor.autocomplete) {
		                var ret = obj.constructor.autocomplete(obj);
		                if (ret.constructor != Array) {
		                    print("\nautocompleters must return real Arrays");
		                    return [];
		                }
		                return ret;
		            } else {
		                return [];
		            }
		        } catch (e) {
		            // print( e ); // uncomment if debugging custom completers
		            return [];
		        }
		    };

		    var worker = function(prefix) {
		        var global = {db:db}; //this is the first place where we patched around

		        var curObj = global;
		        var parts = prefix.split('.');
		        for (var p = 0; p < parts.length - 1; p++) {  // doesn't include last part
		            curObj = curObj[parts[p]];
		            if (curObj == null)
		                return [];
		        }

		        var lastPrefix = parts[parts.length - 1] || '';
		        var lastPrefixLowercase = lastPrefix.toLowerCase();
		        var beginning = parts.slice(0, parts.length - 1).join('.');
		        if (beginning.length)
		            beginning += '.';

		        var possibilities =
		            new Array().concat(universalMethods,
		                               Object.keySet(curObj),
		                               Object.keySet(curObj.__proto__),
		                               builtinMethods[curObj] || [],  // curObj is a builtin constructor
		                               builtinMethods[curObj.__proto__.constructor] ||
		                                   [],  // curObj is made from a builtin constructor
		                               curObj == global ? extraGlobals : [],
		                               customComplete(curObj));

		        var noDuplicates =
		            {};  // see http://dreaminginjavascript.wordpress.com/2008/08/22/eliminating-duplicates/
		        for (var i = 0; i < possibilities.length; i++) {
		            var p = possibilities[i];
		            if (typeof(curObj[p]) == "undefined" && curObj != global)
		                continue;  // extraGlobals aren't in the global object
		            if (p.length == 0 || p.length < lastPrefix.length)
		                continue;
		            if (lastPrefix[0] != '_' && isPrivate(p))
		                continue;
		            if (p.match(/^[0-9]+$/))
		                continue;  // don't array number indexes
		            if (p.substr(0, lastPrefix.length).toLowerCase() != lastPrefixLowercase)
		                continue;

		            var completion = beginning + p;
		            if (curObj[p] && curObj[p].constructor == Function && p != 'constructor')
		                completion += '(';

		            noDuplicates[completion] = 0;
		        }

		        var ret = [];
		        for (var i in noDuplicates)
		            ret.push(i);

		        return ret;
		    };

		    // this is the actual function that gets assigned to shellAutocomplete
		    return function(prefix) {
		        try {
		            return worker(prefix).sort(); //this is the second place where we patched around
		        } catch (e) {
		            print("exception during autocomplete: " + tojson(e.message));
		            return [];
		        }
		    };
		})();

		return shellAutocomplete(prefix);
	}

	shellAutocomplete.showPrivate = false;  // toggle to show (useful when working on internals)

}

mongoDBHintAdapter.async = true;



	var toBeAccessible = {
		simple_connect : simple_connect,
		Mongo : Mongo,
		DB : DB,
		DBQuery : DBQuery,
		DBCommandCursor : DBCommandCursor,
		Cursor: Cursor,
		sh: sh,
		execute: execute,
		ObjectId: ObjectId,
		NumberLong: NumberLong,
		Timestamp: Timestamp,
		tojson: tojson,
		WriteResult: WriteResult,
		mongoDBHintAdapter: mongoDBHintAdapter,
		initServerConnection: Connection.initServerConnection
	};

	return toBeAccessible;
})();
