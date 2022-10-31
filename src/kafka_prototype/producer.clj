(ns kafka-prototype.producer
  (:gen-class)
  (:require [clojure.java.io :as cio]
            [cheshire.core :as cc-json])
  (:import
    (java.util Properties)
    (org.apache.kafka.clients.admin AdminClient NewTopic)
    (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
    (org.apache.kafka.common.errors TopicExistsException)))


(defn- set-properties [config-file]
  (with-open [config (cio/reader config-file)]
    (doto (Properties.)
      (.putAll
        {ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringSerializer"
         ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"})
      (.load config))))

(defn- create-topic! [topic partitions replication machine-config]
  (println "called")
  (let [admin-client (AdminClient/create machine-config)]
    (try
      (.createTopics admin-client [(NewTopic. ^String topic (int partitions) (short replication))])
      (println "Topic created")
      (catch TopicExistsException e
        nil)
      (finally
        (.close admin-client)))))

(defn producer! [config-file input-file]
  (let [props (set-properties config-file)
        print-ex       (comp println (partial str "Failed to deliver message: "))
        print-metadata #(printf "Produced record to topic %s partition [%d] @ offest %d\n"
                                (.topic %)
                                (.partition %)
                                (.offset %))
        messages (mapv (fn [data]
                         (let [td (cc-json/parse-string data)]
                           td)) (line-seq
                                  (let [r (cio/reader input-file)]
                                    r)))]
    (create-topic! "Sensor_1" 1 3 props)
    (create-topic! "Sensor_2" 1 3 props)
    (with-open [producer (KafkaProducer. props)]
      (let [callback (reify Callback
                         (onCompletion [this metadata exception]
                           (if exception
                             (print-ex exception)
                             (print-metadata metadata))))]
        (map (fn [td]
               (let [k  (first (keys td))
                     v  (get td k)]
                 (.send producer (ProducerRecord. k (cc-json/generate-string k) (cc-json/generate-string v)) callback))) messages)
        (.flush producer)))))

(defn -main [& args]
  (apply producer! args))
        
        

