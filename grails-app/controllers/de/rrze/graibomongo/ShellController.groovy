package de.rrze.graibomongo

import com.mongodb.MongoClient
import com.mongodb.client.MongoDatabase
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.bson.BsonDocument
import org.jongo.Jongo
import org.jongo.RawResultHandler
import org.jongo.query.BsonQueryFactory
import org.jongo.marshall.jackson.JacksonEngine
import org.jongo.marshall.jackson.configuration.Mapping

import grails.converters.JSON

class ResultFlagType {
    /* returned, with zero results, when getMore is called but the cursor id
       is not valid at the server. */
    static ResultFlag_CursorNotFound = 1

    /* { $err : ... } is being returned */
    static ResultFlag_ErrSet = 2

    /* Have to update config from the server, usually $err is also set */
    static ResultFlag_ShardConfigStale = 4

    /* for backward compatibility: this let's us know the server supports
       the QueryOption_AwaitData option. if it doesn't, a repl slave client should sleep
    a little between getMore's.
    */
    static ResultFlag_AwaitCapable = 8
};

class AuthData  implements grails.validation.Validateable {
	String user;
	String password;

	String authDatabase;
	String authMechanism;

	static constraints = {
		authDatabase(nullable: true)
		authMechanism(nullable: true)
	}
}

class ConnectionData implements grails.validation.Validateable {
	String hostname;
	Integer port;

	AuthData auth;

    def beforeValidate() {
        hostname = hostname ?: '127.0.0.1'
        port = port ?: 27017
    }

	String toString(){
		return auth?.user + ":" + auth?.password + "@" + hostname + ":" + port;
	}

	static constraints = {
		auth(nullable:true)
	}
}

class CommandRequest implements grails.validation.Validateable {
	ConnectionData connection;

	String database;
	String command;

	String toString(){
		return "DB[" + database + "].runCommand(" + command + ")";
	}
}

class CursorInitRequest implements grails.validation.Validateable {
	ConnectionData connection;

	String query;
	String ns;
	Long nToReturn;
	Integer nToSkip;

	String toString(){
		return ns + ".find(" + query + ").get(" + nToReturn + ")";
	}
}

class RequestMoreRequest implements grails.validation.Validateable {
	ConnectionData connection;

	Long cursorId;
	Long nToReturn;

	String toString(){
		return "Cursor(" + cursorId + ").get(" + nToReturn + ")";
	}
}

class ShellController {

	static cursors = [:];

    def index(){
    	render "some text"
    }

	def runCommand(CommandRequest request){
		println "=== RunCommand ==="
		if(request.hasErrors()){
			println "   Error"
			print "    "; println request
			print "    "; println request?.connection
			response.status = 422
			render([error: 'Invalid command sent'] as JSON)
			return
		}

		def conn = request.connection

		//TODO: auth
		//TODO: Verbindungsfehler abfangen
		MongoClient mc = new MongoClient(conn.hostname, conn.port);
    	Jongo jong = new Jongo(mc.getDB(request.database))
    	//TODO: Javascript Integer gehen nur bis 2^53-1 => eigentlich muessen wir ueberall BSON Longs verwenden

    	//TODO: JSON-parse-fehler in request.command abfangen
		def result = jong.runCommand(request.command).map(new RawResultHandler());

		mc.close()

		render result as JSON
	}

	def initCursor(CursorInitRequest request){
		println "=== InitCursor ==="
		if(request.hasErrors()){
			println "   Error"
			print "    "; println request
			print "    "; println request?.connection
			response.status = 422
			render([error: 'Invalid command sent'] as JSON)
			return
		}

		def conn = request.connection

		MongoClient mc = new MongoClient(conn.hostname, conn.port);

		def database = request.ns.substring(0, request.ns.indexOf("."));
		def collection = request.ns.substring(request.ns.indexOf(".")+1);

		def query = BsonDocument.parse(request.query);
		def iterable = mc.getDatabase(database).getCollection(collection).find(query).skip(request.nToSkip) //TODO: Handle other options
		def cursor = iterable.iterator()

		def nToReturn = request.nToReturn;
		if(nToReturn == 0)
			nToReturn = 20;

		def data = []
		for(int i=0; i<nToReturn; i++){
			def item = cursor.tryNext()
			if(item != null){
				data.push(item.toJson(new JsonWriterSettings(JsonMode.STRICT)))
			}else{
				break
			}
		}

		//TODO: Handle all iterated
		def scursor = cursor.getServerCursor()
		def cursorId = 0;
		if(scursor != null){
			cursorId = scursor?.getId();
			cursors[conn.hostname + conn.port + cursorId] = [cursor, mc]
		}else{
			mc.close()
		}

		render([nReturned: data.size(),
				data: data,
				resultFlags: 0,
				cursorId: cursorId]  as JSON)

	}

	def requestMore(RequestMoreRequest request){
		println "=== RequestMore ==="
		if(request.hasErrors()){
			println "   Error"
			print "    "; println request
			print "    "; println request?.connection
			response.status = 422
			render([error: 'Invalid command sent'] as JSON)
			return
		}

		def conn = request.connection

		def cursorId = request.cursorId
		def cursorKey = conn.hostname + conn.port + cursorId

		def nToReturn = request.nToReturn
		if(nToReturn == 0)
			nToReturn = 20;

		if(cursorKey in cursors){
			def cursor = cursors[cursorKey][0];

			def data = []
			for(int i=0; i<nToReturn; i++){
				def item = cursor.tryNext()
				if(item != null){
					data.push(item.toJson(new JsonWriterSettings(JsonMode.STRICT)))
				}else{
					break
				}
			}

			if(cursor.getServerCursor() == null){
				cursors[cursorKey][1].close()
				cursors.remove(cursorKey);
				cursorId = 0;
			}

			render([nReturned: data.size(),
					data: data,
					resultFlags: 0,
					cursorId: cursorId] as JSON)
		}else{
			render([resultFlags: ResultFlagType.ResultFlag_CursorNotFound] as JSON)
		}
	}
}

