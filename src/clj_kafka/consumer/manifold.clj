(ns clj-kafka.consumer.manifold
  (:require [clj-kafka.consumer.zk :as zk]
            [clj-kafka.core :refer [to-clojure]]
            [manifold.stream :as s]))

(defn input-stream
  [consumer-config topic & {:keys [threads]
                            :or   {threads 1}}]
  (let [stream (s/stream)
        consumer (zk/consumer consumer-config)
        kafka-stream (zk/stream consumer topic threads)]
    (s/on-closed stream (fn [] (zk/shutdown consumer)))
    (future
      (doseq [msg kafka-stream]
        (s/put! stream (to-clojure msg))))
    stream))
