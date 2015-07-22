# Arctic Scholar

For instructions on deployment, see the [confluence wiki article](https://sensorweb.atlassian.net/wiki/display/AR/Arctic+Scholar+Setting+Up+Tutorial).

## Build Requirements

* Java 1.7.0 or newer
* Maven 3.2.5 or newer

To build from source, use Apache Maven to automatically install the dependent libraries and compile to a jar file.

    $ mvn package

This will create a JAR file in the `target` directory. Use that JAR file to deploy the application.

## Deployment Requirements

* Ubuntu 14.04
* OpenJDK 1.7.0_55
* MySQL 5.5.38
* Elasticsearch 1.3.1

To run the Data Setup step using the JAR file:

    $ java -cp ArcticScholar-1.0.0.jar main/DataSetup

Or to run the Feeder program:

    $ java -cp ArcticScholar-1.0.0.jar main/Feeder

The feeder is designed to be run continuously in the background and not trigger remotely via cron or other system process. The Java app will run the feeder process at midnight MDT and retrieve 24 hours of data.

## License

See LICENSE

## Authors

* Rose Lin <yjlin@ucalgary.ca>
* James Badger <jpbadger@ucalgary.ca>
