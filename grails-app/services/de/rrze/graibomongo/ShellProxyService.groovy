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

	private def getOrCreateClient(serverAddress, authenticationList, mongoClientOptions){
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

	private def doneWithClient(mongoClient){
		openCursorsPerClient[mongoClient].decrementAndGet()
	}

	private def storeCursor(connection, mongoClient, cursor){
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

	private def loadCursor(connection, cursorId){
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

	private def doneWithLoadedCursor(connection, cursor){
		def cursorKey = connection.hostname + connection.port + cursor.getServerCursor().getId()

		synchronized(ageLock){
			cursors.remove(cursorKey)
			lastUsageOfCursor.remove(cursor)
			clientOfCursor[cursor].decrementAndGet()
			clientOfCursor.remove(cursor)
		}

	}
}