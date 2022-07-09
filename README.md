# **Pipeline to MongoDB**
![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Apache Beam](https://img.shields.io/badge/Apache-Beam-yellowgreen?style=for-the-badge&logo=apache)
## **Prerequisites**
- Docker (version 20.10.7+)
- docker-compose (version 1.29.2+)

## The pipeline

A pipeline written in Apache Beam SDK for Python to bring data from file to MongoDB with the following step:
  - Reading and parsing from file;
  - Stripping string value;
  - Renamed the _id_ key to __id_ to use this as index in Mongo.

Another feature is related to a left join performed by CoGroupByKey and a custom DoFn to recollect data in properly way to ensure bulk writing to MongoDB. For further details [visit Apache Beam docs](https://beam.apache.org/documentation/programming-guide/#cogroupbykey)


## **Deployment**

To test application, after cloning this repo, open your terminal and write
```bash
    $ docker-compose up -d mongo-express
```
Sometimes, at start time, mongo-express has some problems to start. To fix you need just to restart the container:
```bash
    $ docker-compose restart mongo-express
```
Finally, you can launch the pipeline after building the container:
```bash
    $ docker-compose build app
    $ docker-compose up app
```
To ensure pipeline work properly, please be sure that mongo is reachable. 
