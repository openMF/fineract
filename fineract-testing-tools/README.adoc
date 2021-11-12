= Gatling plugin for Gradle demo project

A simple showcase of a Gradle project using
https://github.com/gatling/gatling-gradle-plugin/[Gatling plugin for Gradle].

It includes:

* minimal `build.gradle` leveraging Gradle wrapper
* latest version of `io.gatling.gradle plugin` applied
* sample
  https://javadoc.io/doc/io.gatling/gatling-core/latest/io/gatling/core/scenario/Simulation.html[Simulation]
  class, demonstrating sufficient Gatling functionality
* proper source file layout

== Create sample project

For this quickstart: `git` must be installed and available in $PATH,
and a JDK must be installed and $JAVA_HOME configured.

[source, bash]
----
$ git clone https://github.com/gatling/gatling-gradle-plugin-demo.git
$ cd ./gatling-gradle-plugin-demo
----

== Run simulations

To test it out, use following commands:

* Run all Gatling simulations at once

[source, bash]
----
$ ./gradlew gatlingRun
----

* Run single simulation by its FQN (fully qualified class name)

[source, bash]
----
$ ./gradlew gatlingRun-computerdatabase.BasicSimulation
----
