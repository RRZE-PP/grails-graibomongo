package de.rrze.graibomongo

class CmeditorTagLib {

	static namespace = "mongoBrowser"

	def block = { attrs, body ->
		out << render(template: "mongoBrowser", model: [windowMode: "resizable", presets: ([attrs.preset] + attrs.presets + defaultPresets)])
	}

	def window = { attrs, body ->
		out << render(template: "mongoBrowser", model: [windowMode: "moveable", presets: ([attrs.preset] + attrs.presets)])
	}
}
