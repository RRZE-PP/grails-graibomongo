package de.rrze.graibomongo

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.MongoCredential
import com.mongodb.MongoClientOptions
import com.mongodb.MongoTimeoutException
import com.mongodb.MongoCommandException
import com.mongodb.MongoCursorNotFoundException
import com.mongodb.MongoQueryException
import com.mongodb.client.MongoDatabase
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.bson.BsonDocument

import grails.converters.JSON

import java.util.TreeMap

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

	Map cursorId; //This will be converted to a Long in the getter! Ugly, but I see no other way :(
	Long cursorId_;
	Long nToReturn;

	def getCursorId(){
		if(this.cursorId_ == null){
			if(this.cursorId && this.cursorId instanceof String && this.cursorId.startsWith("NumberLong(\"") && this.cursorId.endsWith("\")")){
				this.cursorId_ = Long.valueOf(this.cursorId[12 .. -3]); //keeping this here, but should never be invoced
			}else if(this.cursorId && this.cursorId instanceof Map &&  '$numberLong' in this.cursorId){
				this.cursorId_ = Long.valueOf(this.cursorId['$numberLong']);
			}
		}

		return this.cursorId_
	}

	String toString(){
		return "Cursor(" + getCursorId() + ").get(" + nToReturn + ")";
	}
}

class ShellController {

	static final int SERVER_SELECT_TIMEOUT_MS = 1000;
	static final int CLIENT_EXPIRATION_S = 3600
	static final int MAX_CACHED_CLIENTS = 1000
	static final float PRUNE_AT_PERCENTAGE = 80 //when N percent of MAX_CACHED_CLIENTS have been cached, clear old clients

	static cursors = [:];
	static SortedMap<Long, String> clientAge = new TreeMap<Long, String>()
	static lastClientUsage = [:]
	static Object ageLock = new Object()
	static final int PRUNE_AT_COUNT = MAX_CACHED_CLIENTS * PRUNE_AT_PERCENTAGE / 100

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
			//TODO: check if a mongoclient could be reused (address + auth) and reuse it if possible
			MongoClient mc = new MongoClient(new ServerAddress(conn.hostname, conn.port),
			                                  conn.getAuthList(),
			                                  MongoClientOptions.builder().serverSelectionTimeout(SERVER_SELECT_TIMEOUT_MS).build());

			def result = mc.getDatabase(request.database).runCommand(BsonDocument.parse(request.command))

			mc.close()

			response.setContentType("application/json")
			render result.toJson(new JsonWriterSettings(JsonMode.STRICT))

		}catch(IllegalArgumentException | MongoCommandException e){
			response.status = 422
			render([error: 'Invalid command sent. Exception was: ' + e.getMessage()] as JSON)
		}catch(MongoTimeoutException e){
			response.status = 422
			render([error: 'Connection to the database timed out. Exception was: ' + e.getMessage()] as JSON)
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

		//prune expired clients
		if(cursors.size() > PRUNE_AT_COUNT){
			println "Pruning threshold reached. Starting a prune run."
			def expiredBefore = System.currentTimeMillis() - CLIENT_EXPIRATION_S * 1000
			synchronized(ageLock){
				def stillInUse = [:]
				for(def cursorKey : clientAge.headMap(expiredBefore).values()){
					print cursorKey
					if(lastClientUsage.containsKey(cursorKey) && lastClientUsage[cursorKey] > expiredBefore){
						//this client is old, but still in use
						stillInUse[lastClientUsage[cursorKey]] = cursorKey
						continue
					}

					if(!cursors.containsKey(cursorKey))
						continue


					cursors[cursorKey][0].close()
					cursors[cursorKey][1].close()
					cursors.remove(cursorKey)
				}

				clientAge.headMap(expiredBefore).clear()
				clientAge.putAll(stillInUse)
			}
		}

		if(cursors.size() > MAX_CACHED_CLIENTS){
			response.status = 500
			render([error: "Maximum client number on server has been reached."] as JSON)
			return
		}

		def conn = request.connection

		try{
			//TODO: check if a mongoclient could be reused (address + auth) and reuse it if possible
			MongoClient mc = new MongoClient(new ServerAddress(conn.hostname, conn.port),
			                                  conn.getAuthList(),
			                                  MongoClientOptions.builder().serverSelectionTimeout(SERVER_SELECT_TIMEOUT_MS).build());

			def database = request.ns.substring(0, request.ns.indexOf("."));
			def collection = request.ns.substring(request.ns.indexOf(".")+1);

			def isFindOne = false;

			def query = BsonDocument.parse(request.query);
			def iterable = mc.getDatabase(database).getCollection(collection)
									.find(query)
									.projection(BsonDocument.parse(request.fieldsToReturn))
									.skip(request.nToSkip)
			def cursor = iterable.iterator()

			def nToReturn = request.nToReturn;
			if(nToReturn == 0)
				nToReturn = 20;
			if(nToReturn == -1){
				isFindOne = true;
				nToReturn = 1;
			}

			def data = []
			for(int i=0; i<nToReturn; i++){
				def item = cursor.tryNext()
				if(item != null){
					data.push(item.toJson(new JsonWriterSettings(JsonMode.STRICT)))
				}else{
					break
				}
			}

			def scursor = cursor.getServerCursor()
			def cursorId = 0;
			if(!isFindOne && scursor != null){
				cursorId = scursor?.getId()
				def cursorKey = conn.hostname + conn.port + cursorId
				synchronized(ageLock){
					cursors[cursorKey] = [cursor, mc]
					def curTime = System.currentTimeMillis()
					while(clientAge.containsKey(curTime))//TODO: now there's at most 1000 connections/second :(
						curTime = System.currentTimeMillis()
					clientAge.put(curTime, cursorKey)
				}
			}else{
				mc.close()
			}

			render([nReturned: data.size(),
					data: data,
					resultFlags: 0,
					cursorId: "NumberLong(\"" + cursorId + "\")"]  as JSON)

		}catch(IllegalArgumentException | MongoQueryException e){
			response.status = 422
			render([error: 'Invalid command sent. Exception was: ' + e.getMessage()] as JSON)
		}catch(MongoTimeoutException e){
			response.status = 422
			render([error: 'Connection to the database timed out. Exception was: ' + e.getMessage()] as JSON)
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
		try{

			def conn = request.connection

			def cursorId = request.cursorId
			def cursorKey = conn.hostname + conn.port + cursorId

			def nToReturn = request.nToReturn
			if(nToReturn == 0)
				nToReturn = 20;

			def cursor = null
			synchronized(ageLock){
				if(cursorKey in cursors){
					cursor = cursors[cursorKey][0]
					lastClientUsage[cursorKey] = System.currentTimeMillis()
				}else{
					render([resultFlags: ResultFlagType.ResultFlag_CursorNotFound] as JSON)
					return
				}
			}

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
				synchronized(ageLock){
					cursors[cursorKey][1].close()
					cursors.remove(cursorKey);
					cursorId = 0;
					lastClientUsage.remove(cursorKey)
				}
			}

			render([nReturned: data.size(),
					data: data,
					resultFlags: 0,
							cursorId: "NumberLong(\"" + cursorId + "\")"] as JSON)
		}catch(MongoCursorNotFoundException e){
			render([resultFlags: ResultFlagType.ResultFlag_CursorNotFound] as JSON)
		}
	}
}

