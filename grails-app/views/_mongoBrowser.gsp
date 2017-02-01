<asset:javascript src="mongobrowser.js" />
<asset:javascript src="mongoBrowser-src/mongo-shell/mongo-shell.js" />
<asset:stylesheet src="mongobrowser.css" />

<script>
    MongoNS.initServerConnection("<g:createLink controller="shell" action="initCursor" />",
            "<g:createLink controller="shell" action="requestMore" />",
            "<g:createLink controller="shell" action="runCommand" />")

    var m = MongoBrowser($("body")[0],
        {assetPrefix: "${raw(assetPath(src: "mongoBrowser-src/"))}",
        connectionPresets: [
            <g:each in="${presets}" var="p">
                ${raw(p.toJSON())},
            </g:each>],
        window:"${windowMode}"})
</script>
