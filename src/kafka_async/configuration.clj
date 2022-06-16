(ns kafka-async.configuration
  (:require [clojure.edn :as edn]
            [clojure.string :as string]))

(declare configuration)

(def configuration
  (merge ;;environment
    (apply merge (map (fn [[k v]] {(keyword k) v}) (System/getenv)))))

(defn get-configuration
  "Check if a corresponding key exists in configuration map,
  if so returns its value, checking against uppercase/underscore
  and lowercase/dash formatted keys"
  [key]
  (or (get configuration
           (-> key
               name
               (string/upper-case)
               (string/replace #"-" "_")
               (keyword)))
      (get configuration
           (-> key
               name
               (string/lower-case)
               (string/replace #"_" "-")
               (keyword)))
      ))
