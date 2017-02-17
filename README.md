# scala-elasticsearch-client

A Scala REST Client for Elasticsearch.
---

Overview
---

Scala-elaticsearch-client is a Scala project you can use to make requests over HTTP to your Elasticsearch cluster using 
Elasticsearch's REST API. 
This client supports a number of features of the 
[Elasticsearch REST API](https://www.elastic.co/guide/en/elasticsearch/reference/1.7/index.html).
Create indices, index documents, create and restore from snapshots, manage your cluster health, and [much more](https://github.com/Workday/scala-elasticsearch-client#documentation)
 using 
scala-elasticsearch-client. 

 We currently support v1.7 of the REST API, but we are currently porting our client to v5.X in the near future.
 
 
 
How-to
---

Build a client like this:
````
import com.workday.esclient._
val esUrl = "http://localhost:9200"
val client = EsClient.createEsClient(esUrl)
 ````
And shutdown with:
````
client.shutdownClient()
````
Create an index, index a document, and retrieve that same document:
````
val indexName = "employees" //index name
val typeName = "employee" //type for documents
val id = "1" //document ID
val doc = "{"first_name":"George", "last_name":"Washington", "role":"President"}" //actual document to index
esClient.createIndex(indexName) //creates index in ES
esClient.index(indexName, typeName, id, doc) //indexes doc to that index
val getDoc = esClient.get(indexName, id)
````

Documentation
---
...

Dependencies
---

We use the following dependencies: 

[FasterXML jackson](https://github.com/FasterXML/jackson)
for JSON parsing.
We're planning changing this dependency to [Circe] (https://github.com/circe/circe)in the future,
but Jackson will remain as our primary JSON parser in the meantime.

[Jest](https://github.com/searchbox-io/Jest) is our core Java HTTP REST client for ES.
 
[Google Gson](https://github.com/google/gson) is used to serialize/deserialize Java objects to/from JSON.

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