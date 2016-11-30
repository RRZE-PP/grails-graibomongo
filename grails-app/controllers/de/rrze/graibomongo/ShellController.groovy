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
		log.info("Command request received.")
		if(request.hasErrors()){
			log.warn("Request was irregular.")
			log.debug("Request: " + request)
			log.debug("Connection: " + request?.connection)

			response.status = 422
			render([error: 'Invalid command sent'] as JSON)
			return
		}

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
		log.info("Cursor initiation request received.")
		if(request.hasErrors()){
			log.warn("Request was irregular.")
			log.debug("Request: " + request)
			log.debug("Connection: " + request?.connection)

			response.status = 422
			render([error: 'Invalid command sent'] as JSON)
			return
		}

		def result = shellProxyService.initiateNewCursor(request)

		response.status = result.first
		render(result.second as JSON)
	}

	def requestMore(RequestMoreRequest request){
		log.info("Cursor request more request received.")
		if(request.hasErrors()){
			log.warn("Request was irregular.")
			log.debug("Request: " + request)
			log.debug("Connection: " + request?.connection)

			response.status = 422
			render([error: 'Invalid command sent'] as JSON)
			return
		}

		def result = shellProxyService.getMoreFromCursor(request)

		response.status = result.first
		render(result.second as JSON)
	}
}

