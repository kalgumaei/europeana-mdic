# Measurement Information Content in Europeana Metadata Instances

This framework is designed to measure the Information Content (IC) in the metadata instances in general. However, a part of this code is devoted for measuring the IC in Europeana metadata.
The framework is developed on top of Hadoop. It uses Hadoop HDFS as a distributed storage, Apache Spark as Lightning-Fast cluster computing system, and Elasticsearch as a distributed analysis and data store engine.

## Prerequisites
1) Install Hadoop, prepare Hadoop_data folder on all nodes, and start the HDFS daemon on all nodes. (You can follow the instructions given [here](http://pingax.com/install-apache-hadoop-ubuntu-cluster-setup/) to install and prepare the Hadoop cluster).  
To start HDFS, use the following command on the NameNode:
```
$ start-dfs.sh
```

2) Install Apache Spark by choosing a Pre-built for Hadoop (2.6.4
 or later) package,  so that you do not have to build Spark first. (you can follow the instructions given on the following link: [install Spark on a cluster](http://blog.insightdatalabs.com/spark-cluster-step-by-step/).  
 Then use the following command on the master node to start both the master and slave nodes:
```
$ sbin/start-all.sh
```
 
3) Install Elasticsearch. (you can follow the instructions given [here](https://www.digitalocean.com/community/tutorials/how-to-set-up-a-production-elasticsearch-cluster-on-ubuntu-14-04).  
You can configure Elasticsearch to start automatically with system booting. However, you can also start the service manually by running the following code on each elasticseearch node:
```
$ sudo service elasticsearch start
```
 or restart it using:
```
$ sudo service elasticsearch start
```
* To easily explore & visualize Elasticsearch indices, you can install Kibana by following the instructions given [here]( https://www.elastic.co/guide/en/kibana/current/setup.html).
* You can also install Sense console for interacting with the REST API of Elasticsearch. It's a Kibana app and can be installed by running the following command from the Kibana folder:
```
$ ./bin/kibana plugin --install elastic/sense
```
* In order to use Kibana UI and Sense console, you need to start Kibana using the following command:
```
$ ./bin/kibana
```
3) Download Elasticsearch for Hadoop connector (elasticsearch-hadoop-2.3.2.jar or later) from [here](http://central.maven.org/maven2/org/elasticsearch/elasticsearch-hadoop/) to the jars folder in your Spark home.

## Installation
1) This framework is written in Python 3, so Python 3.x needs to be installed on each node. (For both Ubuntu and Debian, Python 3 will be installed by default)  
2) Download the application code to your home or src directory on your Spark master node.  
3) Upload the Europeana metadata files from the 'europeana' local folder to your HDFS by running the following HDFS command on your Hadoop NameNode:
```
$ hdfs dfs -copyFromLocal europeana/*.gz  /spark/europeana/
```
Note: We have downloaded the full Europeana metadata collection as zipped JSON files from the [Metadata Quality Assurance Framework website](http://141.5.103.129/europeana-qa/download.html).

4) Edit the configuration file /src/main/conf.py to assign the appropriate parameter values based on your previous installation. For example:
```
data_path = "hdfs://es-spark1:9000/spark/europeana/*.gz"
target_fields = ["title", "description"]
es_hosts = "10.10.1.5,10.10.1.4,10.10.1.2"
spark_master = "es-spark2:7077"
```
## Run application
The application consists of two main job that should be submitted to the Spark cluster:  
1) _data transformation_ job (transforms metadata recoreds to elasticsearch analyzable documents)  
2) _IC scoring_ job (calculates Tf-IDF for subfields, parent fields, and instance)  

There are two ways to submit our jobs to Spark master:

1) **Manually:** by running the following command on the Spark master node command line:
```
$ ./usr/local/spark/bin/spark-submit\
                              --master spark://es-spark2:7077\
                              --name 'Transform data job'\
                              --jars /usr/local/spark/jars/elasticsearch-hadoop-2.3.2.jar\
                              --executor-memory 2g\
                               data_transform.py
```    
2) **Through main menu options:** to run the application frequently and interact with indices options, we have written the code in the file `submit_menu.py` as an end point entry for submitting the jobs easily and dealing with indices (overwrite, append, add replicas, and so on). This can be done easily by running the command on the master node command line:
```
$ python3 submit_menu.py
```
The following main menu options will appear:
```
                      1- Index metadata instances
                      2- Calculate IC scores
                      3- Add replicas (orginal index)
                      4- Add replicas (results index)
                      5- Exit application

```
Note: The first two options are dependant, so you should run the _data transformation_ job (option 1) and then the _IC scoring_ job (option2) in order.

Here is an example of _data transformation_ output:
```
{
        "_index": "europeana-55",
        "_type": "resources",
        "_id": "AVb6zFWjVxZflKb-IQl8",
        "_score": 1,
        "_source": {
          "doc_source": "es-spark1",
          "doc_id": "09218/EUS_00A11C0279CF4EBA9D68ABEBE2885CB5",
          "dc:description": {
            "dc:description_hungarian": "A beszélgetés témája:
            az elektronikus kereskedelem helye a magyar gazdaságban.
            Magyarországon no a bizalom az interneten keresztül történo vásárlás irányába.
            Három év alatt a kétszeresére növekedett az online vásárolt árucikkek értéke.
            Magyar szakértok beszélnek az e-kereskedelem térnyerésének okairól, elonyeirol,
            hátrányairól és a biztonság kérdéseirol.",
            "dc:description_english": "The topic of today's discussion:
            the role of electronic commerce in the Hungarian economy.
            Trust towards Internet shopping is growing in Hungary.
            In three years, the value of the goods purchased online has doubled.
            Hungarian experts of the online market discuss the expansion of e-commerce,
            the reasons behind it, the advantages and disadvantages of buying online,
            and the issue of shopping online safely."
          },
          "dcterms:alternative": {
            "dcterms:alternative_hungarian": "Elektronikus kereskedelem Magyarországon"
          },
          "dc:title": {
            "dc:title_english": "E-commerce in Hungary"
          }
        }
      }
```
where the related fields are inside the section ``_source`` as follows:  
``doc_id``: instance ID  
``doc_source``: source where the instnace came from (e.g. file, server, provider, or whatever)  
``dc:description``: an example of a parent field, the same like dc:title and dcterms:alternative  
``dc:description_hungarian``: an example of a subfield (field in specific language)  

Here is an example of _IC scoring_ result:
```
{
        "_index": "europeana-55-result",
        "_type": "resources",
        "_id": "AVb6zSWsVxZflKb-IQ8l",
        "_score": 5.816241,
        "_source": {
          "doc_info_density_score": 3.0582557380262267,
          "doc_ic_score": 5.8915167250152445,
          "doc_source": "es-spark1",
          "doc_id": "09218/EUS_00A11C0279CF4EBA9D68ABEBE2885CB5",
          "dc:description": {
            "field_ic_score": 328.7249932276163,
            "field_info_density_score": 9.547435577358105,
            "sub_tfidf": {
              "dc:description_hungarian": {
                "terms_count": 36,
                "tfidf_score": 163.9154300975866,
                "info_density": 4.553206391599628
              },
              "dc:description_english": {
                "terms_count": 33,
                "tfidf_score": 164.80956313002974,
                "info_density": 4.994229185758477
              }
            }
          },
          "dc:title": {
            "field_ic_score": 16.682130982531095,
            "field_info_density_score": 5.560710327510365,
            "sub_tfidf": {
              "dc:title_english": {
                "terms_count": 3,
                "tfidf_score": 16.682130982531095,
                "info_density": 5.560710327510365
              }
            }
          },
          "dcterms:alternative": {
            "field_ic_score": 15.5467285060896,
            "field_info_density_score": 5.1822428353632,
            "sub_tfidf": {
              "dcterms:alternative_hungarian": {
                "terms_count": 3,
                "tfidf_score": 15.5467285060896,
                "info_density": 5.1822428353632
              }
            }
          }
        }
      }
```
where:  
``doc_id``: instance ID given by Europeana, not the system identifier of Elasticsearch  
``doc_ic_score``: IC score of a given metadata instance. The value represents the logarithm of the fields' IC scores summation.  
``doc_info_density_score``: information density score of a given metadata instance, which represent the logarithm of the fields' information density scores summation.

For each target field, there are several values that describe the information content results on the field level as well as on the subfield level such as:  
`field_ic_score`: information content score of the field  
`field_info_density_score`: information density score of the field  
`sub_tfidf`: result details of the subfields (e.g dc:title_english , dc:title_german, and so on)  

For each subfield, there are three values:  
`terms_count`: length of the subfield after the analysis (e.g. after removing the stop words)  
`tfidf_score`: IC score of the subfield  
`info_density`: information density score of the subfield calculated by dividing the tfidf_score by terms_count    

## Run tests

The unittest modules are located in the directory `test`, so to run the tests follow these steps:
    
1) Upload the Europeana file 2021108_Ag_CZ_CroatianCulturalHeritage_Zvucni11.json.gz to the HDFS storage using the following command:
```
$ hdfs dfs -copyFromLocal europeana/2021108_Ag_CZ_CroatianCulturalHeritage_Zvucni11.json.gz\
                                                     /spark/europeana/test/test_json_data.gz
```
Note: We used this file for the test purpose because it contains only 3 records.

2) Assign the test file path to the parameter `test_data_path` in the conf.py file, for example:
```
test_data_path = "hdfs://NameNode:9000/spark/europeana/test/test_json_data.gz"
```

3) Run the following command on Spark master command to discover all unittest files and run them:

```
$ python3 -m unittest discover -s  path_to/src/test/ -p 'test_*.py'
```
You should see the following text as a result of the unit testing:
```
Ran 29 tests in xx.xxxs

OK
```
### Built With
* Python 3
* PySpark

### Author

* **Khaled Al-Gumaei**  - *Initial work* - 

### License

This project is licensed under the MDIC License - see the [LICENSE.md](LICENSE.md) file for details
