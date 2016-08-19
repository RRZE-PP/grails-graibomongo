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