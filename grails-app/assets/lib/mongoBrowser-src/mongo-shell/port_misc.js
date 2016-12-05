
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