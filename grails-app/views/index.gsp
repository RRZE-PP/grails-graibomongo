<!doctype html>
<html>
<head>
    <title>Standalone Mongobrowser</title>
</head>
<body>

<%@ page import="de.rrze.graibomongo.ConnectionPreset" %>
<mongoBrowser:block presets="${[
		new ConnectionPreset("dev-gsp-hiddenPW", "localhost").auth("root", "root").hidePassword(),
		new ConnectionPreset("dev-gsp-withAuth", "localhost").auth("root", "root", "admin", ConnectionPreset.Method.MONGODB_CR),
		new ConnectionPreset("dev-gsp-withouth", "localhost")]}"/>

</body>
</html>
