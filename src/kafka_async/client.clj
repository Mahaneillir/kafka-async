(ns kafka-async.client
  (:require [kafka-async.kafka-utils :as k-u]
            [kafka-async.producer :as prod]
            [kafka-async.consumer :as cons]
            [kafka-async.json-serdes :as json-serdes]
            [clojure.core.async :refer [chan <!! >!! go-loop alts!! timeout alt!!]]))

(defn send-event [& {:keys [config key topic schema event]}]
  "Create topic and producer if not already available,
  put message on producer's input channel in order to produce a message."
  (let [[in-chan _] (prod/create! config (keyword schema))]
    (if-let [msg (json-serdes/serialize! (keyword schema) event)]
      (do (>!! in-chan {:topic topic :key (str key) :event msg})
          (k-u/log (str "Message with key: " key " produced on topic: " topic) nil "INFO"))
      (k-u/log (str "Message with key: " key " not produced due to serialization error") nil "INFO"))))

(defn kafka-listener
  "Creates an async continuous polling on a set of topics, calling recursively consume-event fn,
  filtering by 'filter' fn if present."
  [& {:keys [config topics schema handlers filter timeout]}]
  (go-loop []
    (let [consumer-id (cons/consume-event config topics schema handlers filter timeout)]
      (when (and ((keyword consumer-id) @cons/consumers)
                 (not (:is-closing? ((keyword consumer-id) @cons/consumers))))
        (recur)))))



