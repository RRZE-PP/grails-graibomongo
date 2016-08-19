<!doctype html>
<html>
<head>
    <title>Standalone Mongobrowser</title>
</head>
<body>

<mongoBrowser:window preset="${new ConnectionPreset("test", "test").auth("foo", "bar", "admin2", ConnectionPreset.Method.MONGODB_CR)}" presets="${[new ConnectionPreset("test", "test2"), new ConnectionPreset("test", "test3")]}"/>
</body>
</html>
