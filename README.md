# kafka-async

Clojure implementation to interact with Apache Kafka using a core.async interface. 
The purpose is to have a listener in a spring fashion such as 
org.springframework.kafka.annotation.KafkaListener that would listen on different topics.

JSON Schema is used as serdes.

Based on https://github.com/JoaoVasques/kafka-async

## Usage
There are two core functions:

Instantiate a listener to asynchronously poll incoming messages on the selected topics. 
Each handler fn will be applied in the order they are passed in, and will be executed within the given timeout.
If the timeout expires no commit is issued and the message will be processed again.

```
(def consumer-config
  {:bootstrap.servers  "localhost:9093"
   :group.id           "test"
   :auto.offset.reset  "earliest"
   :enable.auto.commit false
   :key.deserializer   "org.apache.kafka.common.serialization.StringDeserializer"
   :value.deserializer "org.apache.kafka.common.serialization.StringDeserializer"})

(c/kafka-listener :config consumer-config
                  :topics [topic-list]
                  :schema :schema-name
                  :handlers [f])
```

Send event to the selected topic given the schema name and the key
```
(def producer-config
  {:bootstrap.servers  "localhost:9093"
   :group.id           "test"
   :auto.offset.reset  "earliest"
   :enable.auto.commit false
   :key.serializer     "org.apache.kafka.common.serialization.StringSerializer"
   :value.serializer   "org.apache.kafka.common.serialization.StringSerializer"})

(c/send-event :config producer-config
              :key k
              :schema :schema-name
              :topic topic
              :event {})
```

And some utilities fns:

To get all consumers/To check consumers status:
```
@cons/consumers

(:consumer-id @cons/consumers)
```
To get all producers/To check producers status:
```
@prod/producers

(:producer-id @prod/producers)
```

To close all consumers or by id:
```
(cons/close)

(cons/close consumer-id)
```
To close all producers or by id:
```
(prod/close)

(prod/close producer-id)
```
## License
Copyright © 2022 Nicolò Ciaraldi

Distributed under the MIT Public License



