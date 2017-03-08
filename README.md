# escalar
A Scala REST Client for Elasticsearch.
<p align="center">
  <img src="img/escalar_logo.png" width="64">
</p>

Overview
---

Escalar is a Scala project you can use to make requests over HTTP to your Elasticsearch cluster using 
Elasticsearch's REST API. 
This client supports a number of features of the 
[Elasticsearch REST API](https://www.elastic.co/guide/en/elasticsearch/reference/1.7/index.html).
Create indices, index documents, create and restore from snapshots, manage your cluster health, and [much more](https://github.com/Workday/escalar#documentation)
 using Escalar. 

 We currently support v1.7 of the REST API, but we are currently porting our client to v5.x in the near future.
 
[![Build Status](https://travis-ci.org/Workday/escalar.svg?branch=master)](https://travis-ci.org/Workday/escalar)
[![Code Coverage](https://codecov.io/gh/Workday/escalar/branch/master/graph/badge.svg)](https://codecov.io/gh/Workday/escalar)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.workday/escalar_2.10/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.workday/escalar_2.10)
 
How-to
---

Build a client like this:
````scala
import com.workday.esclient._
val esUrl = "http://localhost:9200"
val client = EsClient.createEsClient(esUrl)
 ````
And shutdown with:
````scala
client.shutdownClient()
````
Create an index, index a document, and retrieve that same document:
````scala
val indexName = "presidents" //index name
val typeName = "president" //type for documents
val id = "1" //document ID
val doc1 = "{'first_name':'George', 'last_name':'Washington', 'home_state':'Virginia'}" //actual document to index
client.createIndex(indexName) //creates index in ES
client.index(indexName, typeName, id, doc) //indexes doc to that index
val getDoc = client.get(indexName, id)
````
Get more sophisticated by adding a few more presidents to our "presidents" index and query by home state:
````scala
val doc2 = "{'first_name':'Thomas', 'last_name':'Jefferson', 'home_state':'Virginia'}"
val doc3 = "{'first_name':'Abraham', 'last_name':'Lincoln', 'home_state':'Ohio'}"
val doc4 = "{'first_name':'Theodore', 'last_name':'Roosevelt', 'home_state':'New York'}"
val bulkActions = Seq(UpdateDocAction(indexName, typeName, "2", doc2), 
                      UpdateDocAction(indexName, typeName, "3", doc3), 
                      UpdateDocAction(indexName, typeName, "4", doc4))
client.bulkWithRetry(bulkActions) //bulk add documents
client.search(indexName, "Virginia") //search the "presidents" index for documents with the text "Virginia"
````
Finally remove all the docs we just created:
````scala
client.deleteDocsByQuery(indexName) //defaults to "match_all" query
````

Building, Testing, and Contributing
---
This is an SBT-based project, so building and testing locally is done simply by using:
````scala
sbt clean coverage test
````
Generate the code coverage report with:
````scala
sbt coverageReport
````
This project aims for 100% test coverage, so any new code should be covered by test code.

To contribute, first read our [contributing](../master/CONTRIBUTING) documentation. Create a separate branch for your
patch and get a passing CI build before submitting a pull request. 

Documentation
---
The full documentation can be found on [Sonatype](https://oss.sonatype.org/service/local/repositories/releases/archive/com/workday/escalar_2.10/1.7.0/escalar_2.10-1.7.0-javadoc.jar/!/index.html#com.workday.esclient.package).


Dependencies
---

We use the following dependencies: 

[FasterXML jackson](https://github.com/FasterXML/jackson)
for JSON parsing.
We're planning changing this dependency to [Circe](https://github.com/circe/circe) in the future,
but Jackson will remain as our primary JSON parser in the meantime.

[Jest](https://github.com/searchbox-io/Jest) is our core Java HTTP REST client for ES.
 
[Google Gson](https://github.com/google/gson) is used to serialize/deserialize Java objects to/from JSON.

Authors
---
Note that the commit history doesn't accurately reflect authorship, because much of the code was ported from an internal repository. Please view the [Contributors List](../master/CONTRIBUTORS) for a full list of everyone who's contributed to the project.

License
---
Copyright 2017 Workday, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

<p align="center">
  <img src="img/escalar_logo.png" width="64">
</p>
