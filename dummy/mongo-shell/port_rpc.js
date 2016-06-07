"use strict";

window.rpc = (function(){

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

			//TODO: is this correct? see CPP code below:
			appendElements(metaDataBob, readPrefEl);
		    // metadataBob->append(readPrefEl);
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
		        // TODO: Move this check elsewhere when we handle upconverting/downconverting maxTimeMS.
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

	return {"upconvertRequestMetadata": upconvertRequestMetadata, makeReply: makeReply};
})();

