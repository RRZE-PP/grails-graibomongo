package de.rrze.graibomongo

import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings

import grails.converters.JSON


class ShellController {

	def shellProxyService

    def index(){
    	render([error: "This controller offers only JSON access for mongobrowser"] as JSON)
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

		def result = shellProxyService.executeCommand(request)

		//Can't use JSON converter on correct results here :(
		if(result.first == 200){
			response.setContentType("application/json")
			render result.second.toJson(new JsonWriterSettings(JsonMode.STRICT))
		}else{
			response.status = result.first
			render(result.second as JSON)
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

		def result = shellProxyService.initiateNewCursor(request)

		response.status = result.first
		render(result.second as JSON)
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

		def result = shellProxyService.getMoreFromCursor(request)

		response.status = result.first
		render(result.second as JSON)
	}
}

