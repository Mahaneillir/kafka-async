(defproject kafka-async "0.1.0-SNAPSHOT"
  :description "Kafka clojure client implementation"
  :license {:name "MIT License"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.apache.kafka/kafka-clients "2.7.2"]
                 [luposlip/json-schema "0.3.3"]
                 [org.clojure/core.async "1.5.648"]
                 [medley "0.8.3"]
                 [org.apache.kafka/kafka_2.12 "2.7.2"]
                 [org.apache.zookeeper/zookeeper "3.5.6"
                  :exclusions [io.netty/netty
                               jline
                               org.apache.yetus/audience-annotations
                               org.slf4j/slf4j-log4j12
                               log4j]]]
  :main ^:skip-aot kafka-async.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
