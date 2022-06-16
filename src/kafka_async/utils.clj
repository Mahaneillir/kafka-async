(ns kafka-async.utils
  (:import (java.util.concurrent TimeoutException TimeUnit)))

(defn remove-nested-nils
  "remove pairs of key-value that has nil value from a (possibly nested) map. also transform map to nil if all of its value are nil"
  [nm]
  (clojure.walk/postwalk
    (fn [el]
      (if (map? el)
        (let [m (into {} (remove (comp nil? second) el))]
          (when (seq m)
            m))
        el))
    nm))

(defn to-int->nil
  [s]
  (try
    (if (int? s) s (Long/parseLong (str s)))
    (catch NumberFormatException e nil)))

(defmacro with-timeout [millis & body]
  `(let [future# (future ~@body)]
     (try
       (.get future# ~millis TimeUnit/MILLISECONDS)
       (catch TimeoutException x#
         (do
           (future-cancel future#)
           nil)))))