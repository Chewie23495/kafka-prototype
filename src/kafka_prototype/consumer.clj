(ns kafka-prototype.consumer
  (:gen-class)
  (:require
    [clojure.java.io :as jio])
  (:import
    (java.util Properties)
    (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
    (java.time Duration)))

;;Consumer funktionieren momentan nicht wie geplant. Teilweise werden Events gepollt, teilweise nicht.

(defn- build-properties [config-fname]
  (with-open [config (jio/reader config-fname)]
    (doto (Properties.)
      (.putAll {ConsumerConfig/GROUP_ID_CONFIG                 "kafka_prototype"
                ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringDeserializer"
                ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"})
      (.load config))))


(defn message-loop
  [config-fname topic]
  (with-open [consumer (KafkaConsumer. (build-properties config-fname))]
    (.subscribe consumer [topic])
    (while true
      (let [records (.poll consumer (Duration/ofMillis 1000))]
        #p(.isEmpty records)
        (doseq [record records]
          #p record
          (println (str "Temperature Data: " (.value record)))))
      (.commitAsync consumer))))

(defn -main [& args]
  (apply message-loop args))