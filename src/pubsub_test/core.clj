(ns pubsub-test.core
  (:require [cheshire.core :as json])
  (:import [com.fasterxml.jackson.core JsonParseException]
           [com.google.pubsub.v1 TopicName PushConfig SubscriptionName PubsubMessage ListTopicsRequest]
           [com.google.cloud.pubsub.v1 TopicAdminClient Publisher SubscriptionAdminClient Subscriber$Builder MessageReceiver]
           [com.google.cloud.pubsub.v1.Subscriber]
           [com.google.api.core ApiService$Listener ApiFutures ApiFutureCallback]
           [com.google.protobuf ByteString]
           io.grpc.Status$Code
           [org.threeten.bp Duration]
           [com.google.common.util.concurrent MoreExecutors]
           [com.google.api.gax.rpc ApiException AlreadyExistsException]
           [com.google.api.gax.core InstantiatingExecutorProvider]
           [java.lang.reflect Method]
           [com.google.api.gax.batching FlowControlSettings]
           [com.google.pubsub.v1
            PubsubMessage
            TopicName
            ProjectName
            ListTopicsRequest]
           com.google.protobuf.ByteString
           com.fasterxml.jackson.core.JsonParseException))

(def project-name "meters-and-supply-points")
(def topic-name "pubsub-test-topic")
(def subscription-name "pubsub-test-subscription")

(defn java-topic-name
  [topic-name]
  (TopicName/of project-name topic-name))

(defn java-subscription-name
  [subscription-name]
  (SubscriptionName/of project-name subscription-name))

(defn- parse
  [message]
  (-> message
      .getData
      .toStringUtf8
      (json/parse-string true)))

(defprotocol Subscriber
  (stop [this])
  (start [this]))

(defn log
  [& args]
  (apply println (java.util.Date.) args))

(defrecord SubscriberImpl
  [java-subscriber]
  Subscriber
  (stop
    [this]
    (log "Attempting to shut down pubsub queue...")
    (.stopAsync java-subscriber)
    (log "Waiting for pubsub queue to terminate...")
    (.awaitTerminated java-subscriber)
    (log "Finished waiting for pubsub queue to terminate")
    this)
  (start
    [this]
    (.startAsync java-subscriber)
    (.awaitRunning java-subscriber)
    this))

(defn message-receiver
  [process-fn]
  (reify
    MessageReceiver
    (receiveMessage [_ message acker] (process-fn message acker))))

(defn logger
  []
  (proxy
    [ApiService$Listener]
    []
    (starting []
      (log "Starting pubsub queue"))
    (running []
      (log "Pubsub queue up and running"))
    (stopping [s]
      (log (str "Pubsub queue received call to stop")))
    (terminated [s]
      (log (str "Pubsub queue terminated:" s)))
    (failed [state e]
      (log e "Fatal error in worker!"))))

(defn executor-provider
  [thread-count]
  (-> (InstantiatingExecutorProvider/newBuilder)
      (.setExecutorThreadCount thread-count)
      .build))

(defn set-max-outstanding-element-count [subscriber-builder outstanding-element-count]
  (.setFlowControlSettings
    subscriber-builder
    (-> (FlowControlSettings/newBuilder)
        (.setMaxOutstandingElementCount (long outstanding-element-count))
        .build)))

(defn build-subscriber
  [process-fn]
  (log "building subscriber to " subscription-name)
  (let [subscriber (-> (com.google.cloud.pubsub.v1.Subscriber/newBuilder
                         (java-subscription-name subscription-name)
                         (message-receiver process-fn))
                       ;this is the max amount of time a message can be renewed to.
                       ;It should be the max amount of time taken to process a message
                       (.setMaxAckExtensionPeriod (Duration/ofHours 8))
                       (.setExecutorProvider (executor-provider 1))
                       (.setParallelPullCount 8)
                       (set-max-outstanding-element-count 1)
                       .build)]
    (.addListener subscriber (logger) (MoreExecutors/directExecutor))
    (->SubscriberImpl subscriber)))

(defn publish
  [[string-payload & others] topic-name]
  (if (nil? string-payload)
    (log "Finished publishing")
    (let [publisher (.build (Publisher/newBuilder (java-topic-name topic-name)))
          data (ByteString/copyFromUtf8 string-payload)
          message (.build (.setData (PubsubMessage/newBuilder) data))]
      (log (str "publishing " string-payload " to " topic-name))
      (ApiFutures/addCallback
        (.publish publisher message)
        (reify ApiFutureCallback
          (onSuccess [_ message-id]
            (log "published " string-payload " to " topic-name "with id" message-id)
            (publish others topic-name))
          (onFailure [_ t]
            (log "failed to publish!" + t)))))))

(defn message-details
  [msg sleep-time]
  {:id (.getMessageId msg)
   :sleep-ms sleep-time})

(defn print-and-sleep
  [unparsed-message acker]
  (let [{:keys [sleep-time] :as message} (parse unparsed-message)]
    (log "Starting" (message-details unparsed-message sleep-time))
    (Thread/sleep sleep-time)
    (log "Finished" (message-details unparsed-message sleep-time))
    (.ack acker)
    (log "Acked" (message-details unparsed-message sleep-time))))

(defn make-message
  [sleep-time]
  {:sleep-time sleep-time})

(def subscriber (build-subscriber print-and-sleep))

(defn go
  []
  (start subscriber)
  nil)

(defn halt
  []
  (stop subscriber)
  nil)

(defn publish-sleeps
  [& sleep-times]
  (publish (map (comp json/generate-string make-message) sleep-times) topic-name))





