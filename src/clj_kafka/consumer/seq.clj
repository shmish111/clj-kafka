(ns clj-kafka.consumer.seq
  (:import (kafka.consumer KafkaStream))
  (:require [clj-kafka.core :refer [to-clojure]]))

(defn- lazy-iterate
  [it]
  (lazy-seq
    (when (.hasNext it)
      (cons (.next it) (lazy-iterate it)))))

(defn kafka-seq
  [^KafkaStream stream]
  (map to-clojure (lazy-iterate (.iterator stream))))
