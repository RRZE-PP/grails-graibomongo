package de.rrze.graibomongo

import com.mongodb.MongoClient
import com.mongodb.client.MongoCursor
import com.mongodb.ServerAddress
import com.mongodb.MongoClientOptions
import com.mongodb.MongoCredential
import com.mongodb.MongoException
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
import java.util.concurrent.atomic.AtomicLong

class CacheFullException extends Exception {}

class ShellProxyService {
	private static def configHolder = grails.util.Holders.config
	private static final int CLIENT_EXPIRATION_S = configHolder?.graibomongo?.clientExpiration ?: 8*60*60
	private static final int CONNECT_TIMEOUT = configHolder?.graibomongo?.connectTimeout ?: 3000
	private static final int SOCKET_TIMEOUT = configHolder?.graibomongo?.socketTimeout ?: 60000
	private static final int SERVER_SELECT_TIMEOUT = configHolder?.graibomongo?.serverSelectionTimeout ?: CONNECT_TIMEOUT + 1000
	private static final int MAX_CACHED_CLIENTS = configHolder?.graibomongo?.maxCachedClients ?: 1000
	private static final int MAX_CACHED_CURSORS = configHolder?.graibomongo?.maxCachedCursors ?: MAX_CACHED_CLIENTS * 500
	private static final float PRUNE_AT_PERCENTAGE = configHolder?.graibomongo?.pruneAtPercentage ?: 80 //when N percent of MAX_CACHED_CLIENTS have been cached, clear old clients


	//TODO: The following structures could mostly be replaced by a wrapper class around cursor and client which reference
	//their respective counterpart
	private static cursors = [:]                //(host, port, cursorId) => cursor
	private static clients = [:]                //(serverAddress, authenticationList, mongoClientOptions) => client
	private static SortedMap<Long, ArrayList<String>> cursorsCreatedPerSecond =
	                    new TreeMap<Long, ArrayList<String>>() //allows easy search for possibly too old cursors
	private static lastUsageOfCursor = [:]      //cursor => long (seconds)
	private static clientOfCursor = [:]         //cursor => client
	private static openCursorsPerClient = [:]   //client => cursor[]

	/* sometimes no server cursor is created with the java driver and we need to fill in our own cursorId
	 * cursorIds are always positive judgin from CursorId CursorManager::_allocateCursorId_inlock() in the mongodb sources
	 * therefore we can safely give out negative IDs hoping that this has no currently unknown side effects
	 */
	private static AtomicLong currentCursorId = new AtomicLong(-1)

	private static Object ageLock = new Object()
	private static final int PRUNE_AT_CLIENT_COUNT = MAX_CACHED_CLIENTS * PRUNE_AT_PERCENTAGE / 100
	private static final int PRUNE_AT_CURSOR_COUNT = MAX_CACHED_CURSORS * PRUNE_AT_PERCENTAGE / 100

	/**
	 * Executes a command on the database.
	 *
	 * @param request - the information about the connection and the requested command
	 * @return a {@code Tuple2<int, org.bson.Document>} in case of success and a {@code Tuple2<int, LinkedHashMap>}
	 * in case of an error with the first value a suggested HTTP status code and the second the result or error message
	 * which can be sent as JSON to a mongobrowser instance
	 */
	def executeCommand(CommandRequest request){
		pruneClientCache()

		def conn = request.connection

		try{
			def mongoClient = getOrCreateClient(new ServerAddress(conn.hostname, conn.port),
			                           conn.createAuthList(),
			                           MongoClientOptions.builder().connectTimeout(CONNECT_TIMEOUT)
			                                                       .socketTimeout(SOCKET_TIMEOUT)
			                                                       .serverSelectionTimeout(SERVER_SELECT_TIMEOUT)
			                                                       .build())

			def result = mongoClient.getDatabase(request.database).runCommand(BsonDocument.parse(request.command))

			doneWithClient(mongoClient)

			return new Tuple2(200, result)
		}catch(CacheFullException e){
			return new Tuple2(500, [error: 'Maximum client number on server has been reached.'])
		}catch(IllegalArgumentException | MongoCommandException e){
			return new Tuple2(422, [error: 'Invalid command sent. Exception was: ' + e.getMessage()])
		}catch(MongoTimeoutException e){
			return new Tuple2(422, [error: 'Connection to the database timed out. Exception was: ' + e.getMessage()])
		}catch(MongoException e){
			return new Tuple2(422, [error: 'An unexpected error occured: ' + e.getMessage()])
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

		if(cursors.size() >= MAX_CACHED_CURSORS){
			return new Tuple2(500, [error: 'Maximum cursor number on server has been reached.'])
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
			                           conn.createAuthList(),
			                           MongoClientOptions.builder().connectTimeout(CONNECT_TIMEOUT)
			                                                       .socketTimeout(SOCKET_TIMEOUT)
			                                                       .serverSelectionTimeout(SERVER_SELECT_TIMEOUT)
			                                                       .build())

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
			}else if(!isFindOne && cursor.hasNext() && scursor == null){
				//for whatever reason the java driver has no server cursor but there are still results left
				//=> we set our own cursorId (see above!)
				cursorId = currentCursorId.decrementAndGet()
				storeCursor(conn, mongoClient, cursor, cursorId)
			}else{
				cursor.close()
				doneWithClient(mongoClient)
			}

			return new Tuple2(200, [nReturned: data.size(),
								data: data,
								resultFlags: 0,
								cursorId: "NumberLong(\"" + cursorId + "\")"])

		}catch(CacheFullException e){
			return new Tuple2(500, [error: 'Maximum client number on server has been reached.'])
		}catch(IndexOutOfBoundsException | IllegalArgumentException | MongoQueryException e){
			return new Tuple2(422, [error: 'Invalid command sent. Exception was: ' + e.getMessage()])
		}catch(MongoTimeoutException e){
			return new Tuple2(422, [error: 'Connection to the database timed out. Exception was: ' + e.getMessage()])
		}catch(MongoException e){
			return new Tuple2(422, [error: 'An unexpected error occured: ' + e.getMessage()])
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

			if(!cursor.hasNext()){
				def cursorKey = conn.hostname + conn.port + cursorId
				doneWithLoadedCursor(conn, cursor, cursorKey)
				cursor.close()
				cursorId = 0
			}

			return new Tuple2(200, [nReturned: data.size(),
								data: data,
								resultFlags: 0,
								cursorId: "NumberLong(\"" + cursorId + "\")"])
		}catch(MongoCursorNotFoundException e){
			return new Tuple2(200, [resultFlags: ResultFlagType.ResultFlag_CursorNotFound])
		}catch(MongoException e){
			return new Tuple2(422, [error: 'An unexpected error occured: ' + e.getMessage()])
		}
	}

	/**
	 * Removes expired clients and cursors from the cache if a certain threshold has been reached
	 */
	private def pruneClientCache(){
		if(clients.size() > PRUNE_AT_CLIENT_COUNT || cursors.size() > PRUNE_AT_CURSOR_COUNT){
			log.info("Pruning threshold reached (" + cursors.size() + " > " + PRUNE_AT_CURSOR_COUNT +
			         " of max " + MAX_CACHED_CURSORS + " cursors or " + clients.size() + " > " + PRUNE_AT_CLIENT_COUNT +
			         " of max " + MAX_CACHED_CLIENTS + " clients).")
			def oldCursorCount = cursors.size()
			def oldClientCount = clients.size()
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
							log.debug("Cursor " + cursorKey + " is still in use")
							continue
						}

						if(!cursors.containsKey(cursorKey))
							continue

						log.debug("Cursor " + cursorKey + " can be removed")

						cursor.close()

						def client = clientOfCursor[cursor]
						lastUsageOfCursor.remove(cursor)
						clientOfCursor.remove(cursor)
						openCursorsPerClient[client].decrementAndGet()
						cursors.remove(cursorKey)
					}
				}

				cursorsCreatedPerSecond.headMap(expiredBefore).clear()
				cursorsCreatedPerSecond.putAll(stillInUse)

				//Now remove all old clients who have never had a cursor created or whose cursors we have removed
				def removeClients = []
				for(client in clients.values()){
					if(openCursorsPerClient[client].get() == 0){
						log.debug("No cursor uses this client.")
						client.close()
						removeClients.add(client)
						openCursorsPerClient.remove(client)
					}
				}
				for(client in removeClients){
					clients.values().remove(client) //This could be done more efficiently
				}

			}// synchronized

			log.info("Removed " + (oldCursorCount - cursors.size()) +
				     " cursors and " + (oldClientCount - clients.size()) + " clients.")
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
	private def getOrCreateClient(ServerAddress serverAddress,
	                              List<MongoCredential> authenticationList,
	                              MongoClientOptions mongoClientOptions) throws CacheFullException {
		def clientID = new Tuple(serverAddress, authenticationList, mongoClientOptions)

		MongoClient mongoClient = null
		synchronized(ageLock){
			if(clients.containsKey(clientID)){
				mongoClient = clients[clientID]
				openCursorsPerClient[mongoClient].incrementAndGet()
			}else{
				if(clients.size() >= MAX_CACHED_CLIENTS)
					throw new CacheFullException()

				mongoClient = new MongoClient(serverAddress,
	                                  authenticationList,
	                                  mongoClientOptions)
				clients[clientID] = mongoClient
				openCursorsPerClient[mongoClient] = new AtomicInteger(1)
				log.debug("New client created, caching " + clients.size() + " clients")
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
	 * @param cursorId the cursor id to use or null when to infer from the cursor's servercursor
	 */
	private def storeCursor(ConnectionData connection, MongoClient mongoClient, MongoCursor cursor, Long cursorId = null){
		def cursorKey = connection.hostname + connection.port + (cursorId ?: cursor.getServerCursor().getId())
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
		log.debug("New cursor stored, caching " + cursors.size() + " cursors")
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
	 * @param cursorKey this cursor's key
	 */
	private def doneWithLoadedCursor(ConnectionData connection, MongoCursor cursor, String cursorKey){
		synchronized(ageLock){
			cursors.remove(cursorKey)
			lastUsageOfCursor.remove(cursor)
			openCursorsPerClient[clientOfCursor[cursor]].decrementAndGet()
			clientOfCursor.remove(cursor)
		}

	}
}