<asset:javascript src="mongobrowser.js" />
<%-- mongo-shell.js cannot be minified, therefore... --%>
<g:if env="development">
	<%-- in development where no minifaction occurs and no .unminified.js exists, use original --%>
    <asset:javascript src="mongoBrowser-src/mongo-shell/mongo-shell.js" />
</g:if>
<g:elseif env="test">
	<%-- in test where no minifaction occurs (I guess) and no .unminified.js exists, use original --%>
    <asset:javascript src="mongoBrowser-src/mongo-shell/mongo-shell.js" />
    <!-- if the line above does not include the mongo shell, something went wrong in _mongoBrowser.gsp-->
</g:elseif>
<g:else>
	<%-- in production where minifaction occurs, use unminified explicitly --%>
	<asset:javascript src="mongoBrowser-src/mongo-shell/mongo-shell.unminified.js" />
</g:else>
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
