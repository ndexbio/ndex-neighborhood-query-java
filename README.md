[ndex]: https://www.ndexbio.org
[ndex-rest]: https://github.com/ndexbio/ndex-rest
[nexus]: https://nrnb-nexus.ucsd.edu
[jetty]: http://eclipse.org/jetty/
[maven]: http://maven.apache.org/
[java]: https://www.oracle.com/java/index.html
[git]: https://git-scm.com/
[make]: https://www.gnu.org/software/make

# NDEx Neighborhood query REST Service

Provides a micro REST service to perform **Direct,
1-Step Neighborhood, 1-step adjacent, Interconnect,
2-step neighborhood, and 2-step adjacent** node queries
on networks in [NDEx][ndex]. This service leverages
an embedded [Jetty][jetty] server and is used
by the [NDEx REST service][ndex-rest] and is intended to 
be stood up internally since it offers **NO** authentication
or invocation checks.

## Requirements

* [Solr](https://solr.apache.org) 8+ (to run)
* [Java][java] 17+ (jdk to build)
* [Maven][maven] 3.6+ (to build)
* [Make][make] **(optional to build)**

Dependency deployed on [NRNB Nexus][nexus]

* [ndex-object-model](https://github.com/ndexbio/ndex-object-model)

## Building NDEx Neighborhood query REST Service

Commands below build NDEx Neighborhood query REST Service 
assuming machine has Git command line tools installed and 
above Java modules have been installed:

```Bash
# In lieu of git one can just download repo and unzip it
git clone https://github.com/ndexbio/ndex-neighborhood-query-java.git

cd ndex-neighborhood-query-java
mvn clean test install
```

The above commands will create a jar file under **target/** named
**NDexQuery-<VERSION>.jar** that is a command line application

## Running NDEx Neighborhood query REST Service 

```Bash
mkdir logs
java -jar NDExQuery-<VERSION>.jar 
```
Above will start the service and utilize [Solr][solr] running
at ``http://localhost:8284``. The port can be overridden by
passing `-Dndex.port=XXXX` on command line

To check service is running call: 

```Bash
curl http://localhost:8284/query/v1/status
```

## COPYRIGHT AND LICENSE

TODO

## Acknowledgements

TODO

