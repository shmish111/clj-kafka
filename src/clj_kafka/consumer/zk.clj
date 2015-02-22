(ns clj-kafka.consumer.zk
  (:import [kafka.consumer ConsumerConfig Consumer]
           [kafka.javaapi.consumer ConsumerConnector])
  (:require [clj-kafka.core :refer [as-properties]]))

(defn consumer
  "Uses information in Zookeeper to connect to Kafka. More info on settings
   is available here: https://kafka.apache.org/08/configuration.html

  Recommended for using with with-resource:
  (with-resource [c (consumer m)]
  shutdown
  (take 5 (messages c \"test\")))

  Keys:
  zookeeper.connect             : host:port for Zookeeper. e.g: 127.0.0.1:2181
  group.id                      : consumer group. e.g. group1
  auto.offset.reset             : what to do if an offset is out of range, e.g. smallest, largest
  auto.commit.interval.ms       : the frequency that the consumed offsets are committed to zookeeper.
  auto.commit.enable            : if set to true, the consumer periodically commits to zookeeper the latest consumed offset of each partition"
  [m]
  (let [config (ConsumerConfig. (as-properties m))]
    (Consumer/createJavaConsumerConnector config)))

(defn shutdown
  "Closes the connection to Zookeeper and stops consuming messages."
  [^ConsumerConnector consumer]
  (.shutdown consumer))

(defn stream
  "takes a Zookeeper consumer, a topic and the number of threads to use and returns a KafkaStream"
  [^ConsumerConnector consumer topic threads]
  (let [[_ [kafka-stream & _]] (first (.createMessageStreams consumer {topic (int threads)}))]
    kafka-stream))