(ns kafka-async.consumer
  "Original idea from https://github.com/JoaoVasques/kafka-async"
  (:require
    [kafka-async.kafka-utils :as k-u]
    [clojure.core.async :refer [>! <! close! chan timeout go-loop alt! alts!! >!!]]
    [kafka-async.json-serdes :as json-serdes])
  (:import
    (org.apache.kafka.clients.consumer KafkaConsumer)
    (java.util UUID)
    (java.time Duration)))

(def consumers
  "atom containing all the registered consumers in the following format:
  {:uuid {:chan core-async-input-output-channel
          :commit-chan core-async-commit-channel
          :consumer java-kafka-consumer
          :is-closing? boolean
          :topics [string-topic-list]
          :config config-map}}
  "
  (atom {}))

(defn records-by-topic
  "Higher order function that receives a set of consumer records and returns a function that expects a topic.
  The purpose of this returned function is to map each record in the set to a topic passed as argument.
  "
  [records]
  (fn [topic]
    (let [consumer-records (.records records topic)]
      {(keyword topic)
       (map (fn [consumer-record]
              {:key       (.key consumer-record)
               :timestamp (.timestamp consumer-record)
               :message   (.value consumer-record)})
            consumer-records)})))

(defn- poll!
  "Poll the selected consumer for records and return them if any as ConsumerRecords.
  Returns nil if no records are available in 5 seconds."
  [consumer-id]
  (let [consumer (consumer-id @consumers)]
    (try (.poll (:consumer consumer) ^Duration (Duration/ofMillis 5000)) ;todo
         (catch Exception e
           (do (k-u/log (str "Internal error while polling record on consumer: " consumer-id) e "ERROR")
               (throw e))))))

(defn- close-consumer
  "Gracefully shut down consumer instance."
  [consumer-id]
  (let [consumer (consumer-id @consumers)]
    (try (do (close! (:commit-chan consumer))
             (close! (:chan consumer))
             (.close (:consumer consumer))
             (swap! consumers dissoc consumer-id)
             (k-u/log (str "Consumer:  " consumer-id " correctly shut down") nil "INFO"))
         (catch Exception e
           (do (k-u/log (str "Internal error while closing consumer: " consumer-id) e "ERROR")
               (throw e))))))

(defn commit!
  [consumer]
  (try (.commitSync consumer)
       (catch Exception e
         (k-u/log "Unable to commit event on consumer, the record will be processed again " e "ERROR"))))

(defn run-consumer
  "A core.async process is created that polls Kafka for messages and sends them to the output channel.
  Clients must manually send a message to the commit-chan in order to continue receiving messages
  by incrementing the consumer's offset. Otherwise, the consumer will park until timeout waiting
  for the commit and no more messages will be put on the out chan."
  [consumer-id topics out-chan timeout-ch]
  (go-loop []
    (let [{_ :chan commit-chan :commit-chan consumer :consumer is-closing? :is-closing?}
          (consumer-id @consumers)
          records (if is-closing? [] (poll! consumer-id))
          records-per-topic (when (and records (not (empty? records)))
                              (->> (reduce conj (map (records-by-topic records) topics))
                                   (into {} (filter (comp not-empty val)))))
          timeout-ch (timeout (or timeout-ch (* 10 60 1000)))] ; default is 10 minutes

      (if (and is-closing? (empty? records-per-topic))
        (close-consumer consumer-id)
        (do (when-not (empty? records-per-topic)
              (when (alt! (timeout 200) false [[out-chan records-per-topic]] true)
                (if (alt! timeout-ch false commit-chan true)
                  (commit! consumer))))
            (recur)))
      )))

(defn- find-consumer
  "Check whether a consumer subscribed to the selected topics already exists,
   if so return the first strictly matching one, nil otherwise."
  [topics]
  (->> @consumers
       (filter #(and
                  (= 0 (compare topics (:topics (second %))))
                  (= false (:is-closing? (second %)))))
       first))

(defn- create
  "Instantiate org.apache.kafka.clients.consumer KafkaConsumer"
  [config]
  [(try
     (KafkaConsumer. (k-u/map->props config))
     (catch Exception e
       (k-u/log "error while creating consumer form config" e "ERROR")
       (throw e))) config])                                 ;todo

(defn create!
  "Create a Kafka Consumer client with a core.async interface given the broker's list and group id.
   After the Java Kafka consumer is created it's saved in the `consumers` atom.
   Return the existing one if a consumer subscribed to the topics in input already exists."
  [config topics & [timeout-ch]]
  (let [[id val] (find-consumer topics)
        [consumer config] (or (when val [(:consumer val) (:config val)]) (create config))
        consumer-id (or id (.toString (UUID/randomUUID)))
        out-chan (or (:chan val) (chan))
        commit-chan (or (:commit-chan val) (chan))]

    (when-not id
      (do
        (.subscribe consumer (java.util.ArrayList. topics))
        (swap! consumers assoc (keyword consumer-id) {:chan        out-chan
                                                      :commit-chan commit-chan
                                                      :consumer    consumer
                                                      :is-closing? false
                                                      :topics      topics
                                                      :config      config})
        (run-consumer (keyword consumer-id) topics out-chan timeout-ch)
        (k-u/log (str "Consumer " consumer-id " created and subscribed to topics: " topics) nil "INFO")))
    [out-chan commit-chan consumer-id]))

(defn consume-event
  "Create and subscribe the consumer to the topic. Reuse existing consumers when they are available.
  Consume all the available events from the topics in input, then park the consumer waiting for commit until timeout.
  Applies each fn in handlers coll to each message.
  Return the consumer-id that will carry on the operation."
  [config topics schema-name handlers & [filter-fn timeout-ch]]
  (let [[out-chan commit-chan consumer-id] (create! config topics timeout-ch)
        read-values (first (alts!! [(timeout 200) out-chan]))]

    (when read-values
      (doseq [read-event (sort-by :timestamp (apply concat (vals read-values)))]
        (let [event (json-serdes/deserialize! schema-name (:message read-event))
              key (:key read-event)]
          (if (or (nil? filter-fn) (filter-fn key event))
            (doseq [handler-fn handlers]
              (handler-fn key event)
              (k-u/log (str "Event with " key " correctly handled: " event) nil "INFO")))))
      (>!! commit-chan :kafka-commit))
    consumer-id
    ))

(defn close
  "Close a KafkaConsumer and the respective core-async channels given the consumer id obtained in `create!`.
  The consumer will close asynchronously to ensure thread safety and the processing of already polled messages.
  This is achieved setting :is-closing to true. If consumer-id is nil it will close all the registered consumers.
  If successful remove the closed consumer from the atom.
  Return nil"
  [& consumer-id]
  (doseq [consumer-id (or consumer-id (keys @consumers))]
    (let [{chan :chan commit-chan :commit-chan consumer :consumer} ((keyword consumer-id) @consumers)]
      (if (and chan consumer)
        (swap! consumers assoc-in [(keyword consumer-id) :is-closing?] true)
        (k-u/log (str "Unable to close consumer: " consumer-id " since it doesn't exist.") nil "INFO")))))