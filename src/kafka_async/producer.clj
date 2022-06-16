(ns kafka-async.producer
  "Original idea from https://github.com/JoaoVasques/kafka-async"
  (:require
    [kafka-async.kafka-utils :as k-u]
    [clojure.core.async :refer [>! <! >!! close! chan timeout go-loop]]
    [kafka-async.utils :as u]
    [kafka-async.configuration :as config])
  (:import
    (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)))

(defn producer
  [config]
  "Instantiate org.apache.kafka.clients.producer.KafkaProducer"
  (KafkaProducer. (k-u/map->props config)))

(def producers
  "Clojure `atom` containing all the registered producers, one singleton for each schema/entity"
  (atom {}))

(def buffer-size
  "Producer core.async input channel default buffer size"
  (or (u/to-int->nil (config/get-configuration :kafka-producer-buffer-size)) 100))

(defn to-producer-record
  "Convert a map into a Kafka Producer Record. The map contains the following structure
  {:topic \"some-topic\" :hash-key \"some-hash\" :event {...}}
  "
  [{topic :topic hash-key :key event :event}]
  (ProducerRecord. topic hash-key event))

(defn drain!
  "Sleep waiting for all pending messages on producer's channel to be processed."
  [entity-name]
  (u/with-timeout
    (max (* buffer-size 300) 60000)
    (while (not (:chan-empty? (entity-name @producers)))
      (Thread/sleep 100))))

(defn run-producer
  "A core.async process is created that reads from the input channel and produces
  the message. If a nil event is passed the process ends."
  [producer in-chan entity-name]
  (go-loop []
           (let [event (<! in-chan)]
             (if event
               (do (try
                     (.send producer event)
                     (catch Exception e
                       (k-u/log (str "Internal error while sending message on producer " entity-name ": " event) e "ERROR")))
                   (recur))
               (swap! producers assoc-in [entity-name :chan-empty?] true)
               ))))

(defn create!
  "Create a KafkaProducer with a core.async interface. The created producer is
  saved in the `producers` atom with the following format:
  {:entity-name {:chan core-async-input-channel
                 :producer java-kafka-producer
                 :chan-empty? control boolean for flushing}}
  "
  [config schema-name]
  (if-not (:producer (schema-name @producers))
    (let [producer (producer config)
          in-chan (chan buffer-size (map to-producer-record))]

      (swap! producers assoc schema-name {:chan in-chan :producer producer :chan-empty? false})

      (run-producer producer in-chan schema-name)
      (k-u/log (str "Producer " schema-name " created.") nil "INFO")
      [in-chan producer])
    [(-> @producers schema-name :chan) (-> @producers schema-name :producer)]))

(defn close
  "Close a KafkaProducer given its id (entity-name) or all of the registered producers otherwise.
  '.close' is called out of producer's thread since its thread safe.
  If successful, remove the closed producer from the atom.
  Return nil."
  [& entity-name]
  (doseq [entity-name (or entity-name (keys @producers))]
    (let [{chan :chan producer :producer} (entity-name @producers)]
      (if (and chan producer)
        (try
          (do (close! chan)
              (drain! entity-name)
              (.flush producer)
              (.close producer)
              (swap! producers dissoc entity-name)
              (k-u/log (str "Producer:  " entity-name " correctly shut down") nil "INFO"))
          (catch Exception e
            (do (k-u/log (str "Internal error while closing producer " entity-name) e "ERROR")
                (throw e))))
        (do (k-u/log (str "Unable to close producer " entity-name " since it doesn't exist. " @producers) nil "INFO")
            (swap! producers dissoc entity-name))))))
