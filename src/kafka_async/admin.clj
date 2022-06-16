(ns kafka-async.admin
  (:require [clojure.test :refer :all]
            [kafka-async.kafka-utils :as k-u])
  (:import (org.apache.kafka.common.errors TopicExistsException)
           (org.apache.kafka.clients.admin AdminClient NewTopic)))

(defn client
  "Create an AdminClient from a configuration map."
  [config]
  (AdminClient/create (k-u/map->props config)))

(defn create-topic! [config topics partitions replication]
  "Create topic"
  (let [ac (client config)]
    (try
      (let [topics-list (.get (.names (.listTopics ac)))]
        (when-not (some (set topics-list) topics)
          (.createTopics ac [(NewTopic. ^String topics (int partitions) (short replication))])))
      (catch TopicExistsException e nil)                    ; Ignore TopicExistsException, which would get thrown if the topic was previously created
      (finally
        (.close ac)))))
