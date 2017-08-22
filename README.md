Kafka KNMI Connector
====================

Kafka Connect Source for KNMI Weatherstation data (NL)

Source Connector
----------------
The Kafka Connect source class is provided as ``com.knmi.climatology.connect.KnmiClimatologySourceConnector``

The following properties can be set using a properties file:
* ``topic.name``: Name of the Kafka topic this source connector writes to.
* ``connect.knmi.climatology.weatherstations``: List of comma-separated ID's of KNMI weatherstations. These are listed on the website of the Koninklijke Nederlandse Meteorologisch Instituut: http://projects.knmi.nl/klimatologie/metadata/index.html. The ID's are listed as 'nummer'. F.e.; *210 Valkenburg* has ID: 210
* ``connect.knmi.climatology.start.epochsecond``: When the Connector is run for the first time, and no previous offset are knwown, start collecting from this timestamp onwards.
* ``connect.knmi.climatology.maxpollingintervalseconds``: The maximum polling interval.
* ``connect.knmi.climatology.maxdataintervalseconds``: The maximum range of data to be gathered in one polling.

