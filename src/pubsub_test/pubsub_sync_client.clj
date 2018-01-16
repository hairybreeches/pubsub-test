(ns pubsub-test.pubsub-sync-client
  (:require [taoensso.timbre :as log])
  (:import [com.google.cloud.pubsub.v1 SubscriptionAdminSettings]
           [com.google.cloud.pubsub.v1.stub GrpcSubscriberStub]
           [com.google.pubsub.v1
            SubscriptionName
            PullRequest
            ModifyAckDeadlineRequest
            AcknowledgeRequest]
           [org.threeten.bp Duration]
           [java.util.concurrent
            Executors
            ScheduledExecutorService
            TimeUnit]
           [com.google.common.util.concurrent
            MoreExecutors
            Futures
            FutureCallback]
           java.util.ArrayList
           java.io.PrintStream
           org.apache.log4j.MDC
           org.slf4j.bridge.SLF4JBridgeHandler))

(defn- capture-console-messages!
  []
  (System/setOut (proxy [PrintStream] [System/out]
                   (print [s]
                     (log/info (str "Console message: " s))))))

(defn- init-logging!
  []
  (capture-console-messages!)
  (let [timbre-config (assoc log/example-config :level :info)]
    (SLF4JBridgeHandler/removeHandlersForRootLogger)
    (SLF4JBridgeHandler/install)
    (log/set-config! timbre-config)))

(defn client-settings
  [max-rpc-seconds]
  (let [client-settings-builder (SubscriptionAdminSettings/newBuilder)
        pull-retry-settings (-> client-settings-builder
                                (.pullSettings)
                                (.getRetrySettings)
                                (.toBuilder)
                                (.setInitialRpcTimeout (Duration/ofSeconds 1))
                                (.setMaxAttempts 1)
                                (.build))
        ack-retry-settings (-> client-settings-builder
                               (.acknowledgeSettings)
                               (.getRetrySettings)
                               (.toBuilder)
                               (.setInitialRpcTimeout (Duration/ofSeconds 1))
                               (.setTotalTimeout (Duration/ofSeconds max-rpc-seconds))
                               (.build))]
    (-> client-settings-builder
        (.pullSettings)
        (.setRetrySettings pull-retry-settings))
    (-> client-settings-builder
        (.acknowledgeSettings)
        (.setRetrySettings ack-retry-settings))
    (-> client-settings-builder
        (.modifyAckDeadlineSettings)
        (.setRetrySettings ack-retry-settings))
    (.build client-settings-builder)))

(defn pull-request
  [subname num-of-messages]
  (-> (PullRequest/newBuilder)
      (.setReturnImmediately Boolean/TRUE)
      (.setSubscription subname)
      (.setMaxMessages num-of-messages)
      .build))

(defn mod-ack-request
  [ack-deadline-seconds-increament sub-name ack-ids]
  (-> (ModifyAckDeadlineRequest/newBuilder)
      (.setAckDeadlineSeconds ack-deadline-seconds-increament)
      (.setSubscription sub-name)
      (.addAllAckIds (ArrayList. ack-ids))
      .build))

(defn send-mod-acks
  [ack-deadline-seconds-increament to-mod-ack subscriber sub-name]
  (try
    (when-not (empty? @to-mod-ack)
      (log/info "Sending mod-acks: " @to-mod-ack)
      (-> subscriber
          (.modifyAckDeadlineCallable)
          (.call (mod-ack-request ack-deadline-seconds-increament sub-name @to-mod-ack))))
    (catch Exception e
      (log/error e))))

(defn ack-request
  [sub-name ack-ids]
  (-> (AcknowledgeRequest/newBuilder)
      (.setSubscription sub-name)
      (.addAllAckIds (ArrayList. ack-ids))
      .build))

(defn send-acks
  [to-ack to-mod-ack subscriber sub-name]
  (try
    (let [local-to-acks @to-ack]
      (when-not (empty? local-to-acks)
        (log/info "Sending acks: " local-to-acks)
        (-> subscriber
            (.acknowledgeCallable)
            (.call (ack-request sub-name local-to-acks)))
        (swap! to-ack #(apply disj % local-to-acks))
        (swap! to-mod-ack #(apply disj % local-to-acks))))
    (catch Exception e
      (log/error e))))

(defn process-messages
  [message-fn worker-pool to-ack to-mod-ack subscriber subname num-threads]
  (try
    (when (> num-threads (count @to-mod-ack))
      (let [pull-response (-> subscriber
                              (.pullCallable)
                              (.call (pull-request subname (- num-threads (count @to-mod-ack)))))]
        (doseq [message (.getReceivedMessagesList pull-response)]
          (let [ack-id (.getAckId message)
                future (.submit worker-pool (partial message-fn message))]
            (swap! to-mod-ack conj ack-id)
            (Futures/addCallback future (proxy [FutureCallback] []
                                          (onSuccess [message-id]
                                            (swap! to-ack conj ack-id))
                                          (onFailure [thrown]
                                            (swap! to-mod-ack disj ack-id))))))))
    (catch Exception e
      (log/error e))))

(defn schedule-fn
  [project-id subscription-id pull-interval-milliseconds num-threads max-rpc-seconds]
  (fn [message-fn]
    (let [subscriber (GrpcSubscriberStub/create (client-settings max-rpc-seconds))
          send-ack-period-seconds (int 1)
          max-rpc-seconds (int 20)
          ack-deadline-seconds-increament (int 40)
          send-mod-ack-period-seconds (int 30)
          worker-pool (MoreExecutors/listeningDecorator (Executors/newFixedThreadPool num-threads))
          service-pool (Executors/newScheduledThreadPool 4)
          sub-name (str (SubscriptionName/of project-id subscription-id))
          to-mod-ack (atom #{})
          to-ack (atom #{})
          inital-delay 0]
      (.scheduleWithFixedDelay
       service-pool
       #(process-messages message-fn worker-pool to-ack to-mod-ack subscriber sub-name num-threads)
       inital-delay
       pull-interval-milliseconds
       TimeUnit/MILLISECONDS)
      (.scheduleWithFixedDelay
       service-pool
       #(send-mod-acks ack-deadline-seconds-increament to-mod-ack subscriber sub-name)
       (int (/ send-mod-ack-period-seconds 2))
       send-mod-ack-period-seconds
       TimeUnit/SECONDS)
      (.scheduleWithFixedDelay
       service-pool
       #(send-acks to-ack to-mod-ack subscriber sub-name)
       0
       send-ack-period-seconds
       TimeUnit/SECONDS))))

(init-logging!)

;; set pull-interval to 1 second (when testing this is what i used, in practice a higher value would decrease the network traffic)
;; set number of threads to 4 (or number of cores available)
;; set max-rpc-seconds to 30 seconds (this is the maximum time for a rpc call to timeout).

#_(def schedule
  (schedule-fn
   "Insert your project-id here!!"
   "dup-message-subscription"
   100
   4
   30))

#_(schedule #(do
             (log/info "Got message: " %)
             (Thread/sleep 3600000)))
