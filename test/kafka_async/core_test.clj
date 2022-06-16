(ns kafka-async.core-test
  (:require [clojure.test :refer :all]
            [embedded :as e]
            [kafka-async.client :as c]
            [kafka-async.admin :as a]
            [kafka-async.utils :as u]
            [kafka-async.consumer :as cons]
            [kafka-async.producer :as prod]
            [kafka-async.json-serdes :as json-serdes]
            [clojure.core.async :refer [>!! <!!]]))

(def host "localhost")
(def kafka-port 9093)
(def zk-port 2183)
(def bootstrap-servers (format "%s:%s" host kafka-port))
(def config {:bootstrap.servers  bootstrap-servers
             :group.id           "test"
             :auto.offset.reset  "earliest"
             :enable.auto.commit false})
(def consumer-config (assoc config
                       :key.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
                       :value.deserializer "org.apache.kafka.common.serialization.StringDeserializer"))
(def producer-config (assoc config
                       :key.serializer "org.apache.kafka.common.serialization.StringSerializer"
                       :value.serializer "org.apache.kafka.common.serialization.StringSerializer"))

(use-fixtures
  :once (fn [f]
          (let [z-dir (e/create-tmp-dir "zookeeper-data-dir")
                k-dir (e/create-tmp-dir "kafka-log-dir")]
            (try
              (with-open [k (e/start-embedded-kafka
                              {::e/host               host
                               ::e/kafka-port         kafka-port
                               ::e/zk-port            zk-port
                               ::e/zookeeper-data-dir (str z-dir)
                               ::e/kafka-log-dir      (str k-dir)
                               ::e/broker-config      {"auto.create.topics.enable" "true"}})]
                (f))
              (catch Throwable t
                (throw t))
              (finally
                (e/delete-dir z-dir)
                (e/delete-dir k-dir))))))

(defn close-consumers
  [& [wait max ids]]
  (if ids
    (doall (map #(cons/close %) ids))
    (cons/close))

  (let [s (atom (or 10 max))]
    (while (and (pos? @s)
                (not (empty? @cons/consumers)))
      (println "waiting for consumers to close...")
      (Thread/sleep (or wait 1000))
      (swap! s dec))))

(defn close-producers
  [& [wait max ids]]
  (do
    (if ids
      (doall (map #(prod/close %) ids))
      (prod/close))
    (let [s (atom (or 10 max))]
      (while (and (pos? @s)
                  (not (empty? @prod/producers)))
        (println "waiting for producers to close...")
        (Thread/sleep (or wait 1000))
        (swap! s dec)))))

(deftest consumer-creation-test
  (testing "Instatiates 6 consumers on 3 different sets of topics to test creation and reuse"
    (let [topic1 "test-topic1"
          topic2 "test-topic2"
          topic3 "test-topic3"
          _ (reset! cons/consumers {})
          _ (a/create-topic! config "test-topic1" 1 1)
          _ (a/create-topic! config "test-topic2" 1 1)
          _ (a/create-topic! config "test-topic3" 1 1)
          [_ _ con1] (cons/create! consumer-config [topic1])
          [_ _ con2] (cons/create! consumer-config [topic2])
          [_ _ con3] (cons/create! consumer-config [topic1 topic2 topic3])]
      (is (some? ((keyword con1) @cons/consumers)))
      (is (some? ((keyword con2) @cons/consumers)))
      (is (some? ((keyword con3) @cons/consumers)))
      (is (= 3 (count @cons/consumers)))

      (let [[_ _ con11] (cons/create! consumer-config [topic1])
            [_ _ con22] (cons/create! consumer-config [topic2])
            [_ _ con33] (cons/create! consumer-config [topic1 topic2 topic3])]
        (is (= 3 (count @cons/consumers)))
        (is (= con11 (keyword con1)))
        (is (= con22 (keyword con2)))
        (is (= con33 (keyword con3))))

      (close-consumers 1000 20 [con1 con2 con3])
      (is (empty? @cons/consumers))
      )))

(deftest producer-creation-test
  (testing "Instantiates producer to test creation and reuse"
    (close-producers)
    (is (empty? @prod/producers))
    (let [id (prod/create! producer-config :entity)]
      (is (not (empty? @prod/producers)))
      (is (some? (:entity @prod/producers)))

      (let [_ (prod/create! producer-config :entity)]
        (is (not (empty? @prod/producers)))
        (is (= 1 (count @prod/producers))))

      (close-producers 1000 20 [:entity])
      (is (empty? @prod/producers)))))

(deftest roundtrip
  (testing "Fires up kafka producer and consumer, serializes with json-schema, sends msg, reads msg, deserializes with json-schema, closes instances"
    (let [topic "test-topic"
          _ (a/create-topic! config topic 1 1)
          [out-chan commit-chan consumer-id] (cons/create! consumer-config [topic])
          msg (json-serdes/serialize! :entity {:id 1234})
          [in-chan _] (prod/create! producer-config :entity)]

      (>!! in-chan {:topic topic :key "key" :event msg})
      (let [read-values (<!! out-chan)]
        (when (is (some? (:test-topic read-values)))
          (is (= (str {:id 1234})
                 (str (json-serdes/deserialize! :entity (:message (first (:test-topic read-values)))))))
          ))

      (when (is (some? ((keyword consumer-id) @cons/consumers)))
        (is (false? (:is-closing? ((keyword consumer-id) @cons/consumers)))))

      (>!! commit-chan :kafka-commit)
      (close-consumers 1000 20)
      (close-producers)

      (is (empty? @cons/consumers))
      (is (empty? @prod/producers))
      )))

(deftest kafka-listener-roundtrip
  (testing "Start Consumer listener and process incoming messages."
    (let [topic "test"
          result (atom 0)]
      (a/create-topic! config topic 1 1)

      (c/kafka-listener :config consumer-config
                        :topics [topic]
                        :schema :entity
                        :handlers [(fn [k data] (swap! result inc))])

      (doseq [i (range 1000)]
        (c/send-event :config producer-config
                      :key i
                      :schema :entity
                      :topic topic
                      :event {:id (+ -1 i)}))

      (u/with-timeout 5000 (while (< @result 1000) (Thread/sleep 200)))

      (is (= 1000 @result))
      (is (= 1 (count @cons/consumers)))

      (close-consumers 2000 30)
      (close-producers)

      (is (empty? @cons/consumers))
      (is (empty? @prod/producers))
      )))

(deftest kafka-listener-timeout-expires-roundtrip
  (testing "Initializes a timeout listener, sends messages with longer timeout, checks that each msg is not committed and reprocessed."
    (let [topic "test"
          result1 (atom 0)
          result2 (atom 0)
          result3 (atom 0)
          handler1 (fn [k data] (swap! result1 inc) (Thread/sleep 500))
          handler2 (fn [k data] (swap! result2 inc) (Thread/sleep 100))
          handler3 (fn [k data] (swap! result3 inc) (Thread/sleep 100))]

      (a/create-topic! config topic 1 1)
      (doseq [i (range 5)]
        (c/send-event :config producer-config
                      :key i
                      :schema :entity
                      :topic topic
                      :event {:id i}))
      (close-producers)

      (c/kafka-listener :config consumer-config
                        :topics [topic]
                        :schema :entity
                        :handlers [handler1]
                        :timeout 200)
      (Thread/sleep 6000)
      (is (= @result1 5))
      (is (= 1 (count @cons/consumers)))
      (close-consumers 2000 30)
      (is (empty? @cons/consumers))
      (is (empty? @prod/producers))

      (c/kafka-listener :config consumer-config
                        :topics [topic]
                        :schema :entity
                        :handlers [handler2]
                        :timeout 1000)
      (Thread/sleep 6000)
      (is (= @result2 5))
      (is (= 1 (count @cons/consumers)))
      (close-consumers 2000 30)
      (is (empty? @cons/consumers))
      (is (empty? @prod/producers))

      (c/kafka-listener :config consumer-config
                        :topics [topic]
                        :schema :entity
                        :handlers [handler3]
                        :timeout 1000)
      (Thread/sleep 3000)
      (is (= @result3 0))
      (is (= 1 (count @cons/consumers)))
      (close-consumers 2000 30)
      (is (empty? @cons/consumers))
      (is (empty? @prod/producers))
      )))

