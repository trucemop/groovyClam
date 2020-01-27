# groovyClam
A Groovy script intended for use as an InvokeScriptedProcessor instance in Apache NiFi and the template showing how it works.

The necessary dependencies listed in the Module Directory property can be downloaded from here:

https://search.maven.org/artifact/xyz.capybara/clamav-client/2.0.2/jar

https://search.maven.org/artifact/org.jetbrains.kotlin/kotlin-stdlib/1.3.61/jar

https://search.maven.org/artifact/io.github.microutils/kotlin-logging/1.5.3/jar


A testable clamav instance can be started with docker via:

docker run -d -p 3310:3310 mk0x/docker-clamav

Generate EICAR creates a flowfile with the EICAR test string as its content.  The ClamAvProcessor routes to scanfailure and populates "clamav.results" with clamav's result list (just the one result for this example).

