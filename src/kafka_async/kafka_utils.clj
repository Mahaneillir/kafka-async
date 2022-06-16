(ns kafka-async.kafka-utils
  (:require [medley.core :as m])
  (:import (java.util Map)))

(defn log
  [m e l]
  (println
    (m/assoc-some
      {:message m}
      :exception (when e (Throwable->map e)))
    :kafka l))

(defn map->props
  "Converts a Clojure Map `config` into a `java.util.Properties` object"
  ^Map
  [opts]
  (into {}
        (comp
          (filter (fn [[k _]] (not (qualified-keyword? k))))
          (map (fn [[k v]] [(name k) (str v)])))
        opts))
