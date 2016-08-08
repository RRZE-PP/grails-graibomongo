package de.rrze.graibomongo

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.MongoCredential
import com.mongodb.MongoClientOptions
import com.mongodb.MongoTimeoutException
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

	Boolean performAuth;
	AuthData auth;

	List<MongoCredential> getAuthList(){
		if(performAuth != true)
			return new ArrayList<MongoCredential>();

		ArrayList<MongoCredential> credentials = new ArrayList<MongoCredential>();
		//TODO: check for encoding issues in password with toCharArray!
		switch(auth.authMechanism){
			case "scram-sha-1":
				credentials.push(MongoCredential.createScramSha1Credential(auth.user, auth.authDatabase, auth.password.toCharArray()));
				break;
			case "mongodb-cr":
				credentials.push(MongoCredential.createMongoCRCredential(auth.user, auth.authDatabase, auth.password.toCharArray()));
				break;
			default:
				throw new IllegalArgumentException("Unknown auth mechanism");
		}

		return credentials;
	}

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
	String fieldsToReturn;

	def beforeValidate(){
		fieldsToReturn = fieldsToReturn ?: "{}"
	}

	String toString(){
		return ns + ".find(" + query + ", " + fieldsToReturn +").get(" + nToReturn + ")";
	}
}

class RequestMoreRequest implements grails.validation.Validateable {
	ConnectionData connection;

	String cursorId;
	Long nToReturn;

	def getCursorId(){
		if(this.cursorId && this.cursorId instanceof String && this.cursorId.startsWith("NumberLong(\"") && this.cursorId.endsWith("\")")){
			this.cursorId = Long.valueOf(this.cursorId[12 .. -3]);
		}

		return this.cursorId
	}

	String toString(){
		return "Cursor(" + cursorId + ").get(" + nToReturn + ")";
	}
}

class ShellController {

	final int SERVER_SELECT_TIMEOUT_MS = 1000;
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
		println(request)

		def conn = request.connection

		try{
			MongoClient mc = new MongoClient(new ServerAddress(conn.hostname, conn.port),
			                                  conn.getAuthList(),
			                                  MongoClientOptions.builder().serverSelectionTimeout(SERVER_SELECT_TIMEOUT_MS).build());

	    	Jongo jong = new Jongo(mc.getDB(request.database))

			def result = jong.runCommand(request.command).map(new RawResultHandler());

			mc.close()

			render result as JSON

		}catch(IllegalArgumentException e){
			render([error: 'Invalid command sent. Exception was: ' + e.getMessage()])
		}catch(MongoTimeoutException e){
			render([error: 'Connection to the database timed out. Exception was: ' + e.getMessage()])
		}
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
		println(request)

		def conn = request.connection

		try{
			MongoClient mc = new MongoClient(new ServerAddress(conn.hostname, conn.port),
			                                  conn.getAuthList(),
			                                  MongoClientOptions.builder().serverSelectionTimeout(SERVER_SELECT_TIMEOUT_MS).build());

			def database = request.ns.substring(0, request.ns.indexOf("."));
			def collection = request.ns.substring(request.ns.indexOf(".")+1);

			def query = BsonDocument.parse(request.query);
			def iterable = mc.getDatabase(database).getCollection(collection)
									.find(query)
									.projection(BsonDocument.parse(request.fieldsToReturn))
									.skip(request.nToSkip) //TODO: Handle other options
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
					cursorId: "NumberLong(\"" + cursorId + "\")"]  as JSON)

		}catch(IllegalArgumentException e){
			render([error: 'Invalid command sent. Exception was: ' + e.getMessage()])
		}catch(MongoTimeoutException e){
			render([error: 'Connection to the database timed out. Exception was: ' + e.getMessage()])
		}
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
		println(request)

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
					cursorId: "NumberLong(\"" + cursorId + "\")"] as JSON)
		}else{
			render([resultFlags: ResultFlagType.ResultFlag_CursorNotFound] as JSON)
		}
	}
}

