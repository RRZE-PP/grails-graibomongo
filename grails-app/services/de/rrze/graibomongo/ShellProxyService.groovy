package de.rrze.graibomongo

import com.mongodb.MongoClient
import com.mongodb.client.MongoCursor
import com.mongodb.ServerAddress
import com.mongodb.MongoClientOptions
import com.mongodb.MongoTimeoutException
import com.mongodb.MongoCommandException
import com.mongodb.MongoCursorNotFoundException
import com.mongodb.MongoQueryException
import com.mongodb.client.MongoDatabase
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.bson.BsonDocument

import java.util.TreeMap
import java.util.concurrent.atomic.AtomicInteger

class ShellProxyService {
	private static final int CLIENT_EXPIRATION_S = 3600
	private static final int SERVER_SELECT_TIMEOUT_MS = 1000
	private static final int MAX_CACHED_CLIENTS = 1000
	private static final float PRUNE_AT_PERCENTAGE = 80 //when N percent of MAX_CACHED_CLIENTS have been cached, clear old clients

	private static cursors = [:]
	private static clients = [:]
	private static SortedMap<Long, ArrayList<String>> cursorsCreatedPerSecond =
	                    new TreeMap<Long, ArrayList<String>>() //allows easy search for possibly too old cursors
	private static lastUsageOfCursor = [:]
	private static clientOfCursor = [:]
	private static openCursorsPerClient = [:]

	private static Object ageLock = new Object()
	private static final int PRUNE_AT_COUNT = MAX_CACHED_CLIENTS * PRUNE_AT_PERCENTAGE / 100

	/**
	 * Executes a command on the database.
	 *
	 * @param request - the information about the connection and the requested command
	 * @return a {@code Tuple2<int, org.bson.Document>} in case of success and a {@code Tuple2<int, LinkedHashMap>}
	 * in case of an error with the first value a suggested HTTP status code and the second the result or error message
	 * which can be sent as JSON to a mongobrowser instance
	 */
	def executeCommand(CommandRequest request){
		def conn = request.connection

		try{
			def mongoClient = getOrCreateClient(new ServerAddress(conn.hostname, conn.port),
			                           conn.authList,
			                           MongoClientOptions.builder().serverSelectionTimeout(SERVER_SELECT_TIMEOUT_MS).build())

			def result = mongoClient.getDatabase(request.database).runCommand(BsonDocument.parse(request.command))

			doneWithClient(mongoClient)

			return new Tuple2(200, result)

		}catch(IllegalArgumentException | MongoCommandException e){
			return new Tuple2(422, [error: 'Invalid command sent. Exception was: ' + e.getMessage()])
		}catch(MongoTimeoutException e){
			return new Tuple2(422, [error: 'Connection to the database timed out. Exception was: ' + e.getMessage()])
		}
	}

	/**
	 * Initiates a new cursor by executing a query on the database
	 *
	 * @param request - the information about the connection and the requested query
	 * @return a {@code Tuple2<int, LinkedHashMap>} with the first value a suggested HTTP status code
	 * and the second the result or error message as understandable by a mongobrowser instance
	 */
	def initiateNewCursor(CursorInitRequest request){
		pruneClientCache()

		if(clients.size() > MAX_CACHED_CLIENTS){
			return new Tuple2(500, [error: "Maximum client number on server has been reached."])
		}

		def conn = request.connection

		def isFindOne = false
		def nToReturn = request.nToReturn
		if(nToReturn == 0){
			nToReturn = 20
		}
		if(nToReturn == -1){
			isFindOne = true
			nToReturn = 1
		}

		try{
			def database = request.ns.substring(0, request.ns.indexOf("."))
			def collection = request.ns.substring(request.ns.indexOf(".")+1)

			def mongoClient = getOrCreateClient(new ServerAddress(conn.hostname, conn.port),
			                           conn.authList,
			                           MongoClientOptions.builder().serverSelectionTimeout(SERVER_SELECT_TIMEOUT_MS).build())

			def query = BsonDocument.parse(request.query)
			def cursor = mongoClient.getDatabase(database).getCollection(collection)
									.find(query)
									.projection(BsonDocument.parse(request.fieldsToReturn))
									.skip(request.nToSkip)
									.iterator()

			def data = []
			for(int i = 0; i < nToReturn; i++){
				def item = cursor.tryNext()
				if(item != null){
					data.push(item.toJson(new JsonWriterSettings(JsonMode.STRICT)))
				}else{
					break
				}
			}

			def scursor = cursor.getServerCursor()
			def cursorId = 0
			if(!isFindOne && scursor != null){
				cursorId = scursor.getId()
				storeCursor(conn, mongoClient, cursor)
			}else{
				cursor.close()
				doneWithClient(mongoClient)
			}

			return new Tuple2(200, [nReturned: data.size(),
								data: data,
								resultFlags: 0,
								cursorId: "NumberLong(\"" + cursorId + "\")"])

		}catch(IndexOutOfBoundsException | IllegalArgumentException | MongoQueryException e){
			return new Tuple2(422, [error: 'Invalid command sent. Exception was: ' + e.getMessage()])
		}catch(MongoTimeoutException e){
			return new Tuple2(422, [error: 'Connection to the database timed out. Exception was: ' + e.getMessage()])
		}
	}


	/**
	 * Initiates a new cursor by executing a query on the database
	 *
	 * @param request - the information about the connection and the requested query
	 * @return a {@code Tuple2<int, LinkedHashMap>} with the first value a suggested HTTP status code
	 * and the second the result or error message as understandable by a mongobrowser instance
	 */
	def getMoreFromCursor(RequestMoreRequest request){
		def conn = request.connection
		def cursorId = request.cursorId

		def nToReturn = request.nToReturn
		if(nToReturn == 0)
			nToReturn = 20

		def cursor = loadCursor(conn, cursorId)
		if(cursor == null){
			return new Tuple2(200, [resultFlags: ResultFlagType.ResultFlag_CursorNotFound])
		}

		try{
			def data = []
			for(int i = 0; i < nToReturn; i++){
				def item = cursor.tryNext()
				if(item != null){
					data.push(item.toJson(new JsonWriterSettings(JsonMode.STRICT)))
				}else{
					break
				}
			}

			if(cursor.getServerCursor() == null){
				doneWithLoadedCursor(conn, cursor)
				cursor.close()
				cursorId = 0
			}

			return new Tuple2(200, [nReturned: data.size(),
								data: data,
								resultFlags: 0,
								cursorId: "NumberLong(\"" + cursorId + "\")"])
		}catch(MongoCursorNotFoundException e){
			return new Tuple2(200, [resultFlags: ResultFlagType.ResultFlag_CursorNotFound])
		}
	}

	/**
	 * Removes expired clients and cursors from the cache if a certain threshold has been reached
	 */
	private def pruneClientCache(){
		if(clients.size() > PRUNE_AT_COUNT){
			println "Pruning threshold reached. Starting a prune run."
			def expiredBefore = System.currentTimeMillis()/1000 - CLIENT_EXPIRATION_S

			synchronized(ageLock){

				def stillInUse = [:]
				for(def cursorsCreatedInThisSecond : cursorsCreatedPerSecond.headMap(expiredBefore).values()){
					for(def cursorKey : cursorsCreatedInThisSecond){
						def cursor = cursors[cursorKey]

						if(lastUsageOfCursor[cursor] > expiredBefore){
							//this cursor is old, but still in use
							def lastUsage = lastClientUsage[cursor]

							if(stillInUse[lastUsage] == null){
								stillInUse[lastUsage] = new ArrayList<MongoCursor>()
							}

							stillInUse[lastUsage].add(cursorKey)
							println " is still in use"
							continue
						}

						if(!cursors.containsKey(cursorKey))
							continue

						println " can be removed"

						cursor.close()

						def client = clientOfCursor[cursor]
						lastUsageOfCursor.remove(cursor)
						clientOfCursor.remove(cursor)
						openCursorsPerClient[client].decrementAndGet()
						if(openCursorsPerClient[client].get() == 0){
							client.close()
							clients.remove[client]
							openCursorsPerClient.remove(client)
						}

						cursors.remove(cursorKey)
					}
				}

				clientAge.headMap(expiredBefore).clear()
				clientAge.putAll(stillInUse)
			}
		}
	}

	/**
	 * Creates a new client or gets an applicable one from the cache.
	 * A client is applicable if it is connected to the same address, uses the same authentication and
	 * the same client options.
	 *
	 * @param serverAdress the address to connect to
	 * @param authenticationList the (possibly empty) list of authentication credentials
	 * @param mongoClientOptions the options to set on the client
	 * @return the {@code MongoClient} instance created or loaded from cache
	 */
	private def getOrCreateClient(ServerAddress serverAddress, List<MongoCredential> authenticationList, MongoClientOptions mongoClientOptions){
		def clientID = new Tuple(serverAddress, authenticationList, mongoClientOptions)

		MongoClient mongoClient = null
		synchronized(ageLock){
			if(clients.containsKey(clientID)){
				mongoClient = clients[clientID]
				openCursorsPerClient[mongoClient].incrementAndGet()
			}else{
				mongoClient = new MongoClient(serverAddress,
	                                  authenticationList,
	                                  mongoClientOptions)
				clients[clientID] = mongoClient
				openCursorsPerClient[mongoClient] = new AtomicInteger(1)
			}
		}

		return mongoClient
	}

	/**
	 * Indicates a connection no longer intents to use its cursor on the client
	 *
	 * @param mongoClient the client whose cursor is no longer in use
	 */
	private def doneWithClient(MongoClient mongoClient){
		openCursorsPerClient[mongoClient].decrementAndGet()
	}

	/**
	 * Store a cursor (and the client it's opened from) in the cache
	 *
	 * @param connection the connection to which this client and cursor belong
	 * @param mongoClient the client to store
	 * @param cursor the cursor to store
	 */
	private def storeCursor(ConnectionData connection, MongoClient mongoClient, MongoCursor cursor){
		def cursorKey = connection.hostname + connection.port + cursor.getServerCursor().getId()
		def curTime = System.currentTimeMillis()/1000

		synchronized(ageLock){
			cursors[cursorKey] = cursor
			clientOfCursor[cursor] = mongoClient
			lastUsageOfCursor[cursor] = curTime

			if(!cursorsCreatedPerSecond.containsKey(curTime)){
				//TODO: Maybe decrease the time resolution to 10s?
				ArrayList<String> newList = new ArrayList<String>()
				newList.add(cursorKey)
				cursorsCreatedPerSecond.put(curTime, newList)
			}else{
				cursorsCreatedPerSecond.get(curTime).put(cursorKey)
			}
		}
	}

	/**
	 * Load a cursor from the cache. If the cursor is depleted after loading it, this should be indicated by
	 * calling {@code doneWithLoadedCursor}.
	 *
	 * @param connection the connection to which the cursor belongs
	 * @param cursorId the Id of the cursor to load
	 * @return the {@code MongoCursor} loaded or null if there was no applicable cursor for this connection and id
	 */
	private def loadCursor(ConnectionData connection, long cursorId){
		def cursorKey = connection.hostname + connection.port + cursorId

		synchronized(ageLock){
			if(cursorKey in cursors){
				def cursor = cursors[cursorKey]
				lastUsageOfCursor[cursor] = System.currentTimeMillis()/1000
				return cursor
			}else{
				return null
			}
		}
	}

	/**
	 * Indicates a connection no longer needs a cursor it has previously loaded from cache. It is removed
	 * immediately from the cache (it's client is kept until it expires).
	 *
	 * @param connection the connection to which the cursor belongs
	 * @param cursor the cursor which is obsolete
	 */
	private def doneWithLoadedCursor(ConnectionData connection, MongoCursor cursor){
		def cursorKey = connection.hostname + connection.port + cursor.getServerCursor().getId()

		synchronized(ageLock){
			cursors.remove(cursorKey)
			lastUsageOfCursor.remove(cursor)
			clientOfCursor[cursor].decrementAndGet()
			clientOfCursor.remove(cursor)
		}

	}
}