package de.rrze.graibomongo

import com.mongodb.MongoClient
import com.mongodb.client.MongoDatabase
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


class ShellController {

	static cursors = [:];

    def index(){
    	render "some text"
    }

	def runCommand(){

		println params as JSON
		MongoClient mc = new MongoClient()
    	Jongo jong = new Jongo(mc.getDB("test"))
    	//TODO: Javascript Integer gehen nur bis 2^53-1 => eigentlich muessen wir ueberall BSON Longs verwenden

		def result = jong.runCommand(params.command).map(new RawResultHandler());

		println "===== COMMAND ===="
		println params as JSON
		println params.boolean("multi")
		println result as JSON
		println "====="

		render ""
	}

	def initCursor(){
		def query = new BsonQueryFactory(new JacksonEngine(Mapping.defaultMapping())).createQuery(params.query).toDBObject()

		MongoClient mc = new MongoClient()
		def iterable = mc.getDatabase("test").getCollection("foobar").find(query)//TODO: Handle other options
		def cursor = iterable.iterator()

		def data = []
		for(int i=0; i<params.int('nToReturn'); i++){
			def item = cursor.tryNext()
			if(item != null){
				data.push(item)
			}else{
				break
			}
		}

		//TODO: Handle all iterated
		def scursor = cursor.getServerCursor()
		if(scursor != null)
			cursors[scursor.getId()] = cursor

		render([nReturned: data.size(),
				data: data,
				resultFlags: 0,
				cursorId: scursor.getId()] as JSON)
	}

	def requestMore(){
		println "===== requestMore ===="
		println params
		println cursors
		def cursorId = params.long('cursorId');
		println cursorId
		println cursorId in cursors
		println "====="

		def nToReturn = params.int('nToReturn');
		if(nToReturn == 0)
			nToReturn = 20;

		if(cursorId in cursors){
			def cursor = cursors[cursorId];

			def data = []
			for(int i=0; i<nToReturn; i++){
				def item = cursor.tryNext()
				if(item != null){
					data.push(item)
				}else{
					break
				}
			}

			if(cursor.getServerCursor() == null){
				cursors.remove(cursorId);
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

