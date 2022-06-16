(ns kafka-async.json-serdes
  (:require [kafka-async.kafka-utils :as k-u]
            [kafka-async.utils :as u]
            [clojure.java.io :as io]
            [cheshire.core :as cheshire]
            [json-schema.core :as json-schema]))


(def schema-resolver
  {:entity (json-schema/prepare-schema (cheshire/parse-string (slurp (io/resource "schemas/Entity.json")) true))})

(defn serialize!
  "Infer schema type and serialize into json using cheshire and validating it with jsonSchema"
  [entity-name event]
  (try
    (let [schema-validator (entity-name schema-resolver)]
      (json-schema/validate schema-validator (cheshire/generate-string event)))
    (catch Throwable e
      (k-u/log "Internal error, unable to serialize input event to produce msg." (:errors (:data (Throwable->map e))) "ERROR")
      nil)))

(defn deserialize!
  "Infer schema type and parse from json using cheshire and validating it with jsonSchema"
  [schema-name json-event]
  (try
    (let [schema-validator (schema-name schema-resolver)]
      (u/remove-nested-nils (cheshire/parse-string (json-schema/validate schema-validator json-event) true)))
    (catch Throwable e
      (k-u/log (str "Internal error, unable to deserialize input event to produce msg: " schema-name) (:errors (:data (Throwable->map e))) "ERROR")
      e)))


