;; Copyright 2010-2011 Tim Dysinger <tim@dysinger.net>

;; Licensed under the Apache License, Version 2.0 (the "License"); you
;; may not use this file except in compliance with the License.  You
;; may obtain a copy of the License at

;; http://www.apache.org/licenses/LICENSE-2.0

;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
;; implied.  See the License for the specific language governing
;; permissions and limitations under the License.

(ns apparatus.cluster
  (use apparatus.remote-function)
  (require [clojure.set :as set])
  (:import [com.hazelcast.core Hazelcast DistributedTask MultiTask Member]
           [apparatus Eval]
           java.util.concurrent.Future))

(defn instance
  "Configures and creates the default cluster instance & returns it.
  If the default already exists, a new one is returned."
  [config]
  (try
    (Hazelcast/init config)
    (catch IllegalStateException e
      (Hazelcast/newHazelcastInstance config))))

(defn shutdown
  "Shuts down the entire JVM's clustered instances."
  []
  (Hazelcast/shutdownAll))

(defn members
  "Returns the members of this cluster."
  []
  (-> (Hazelcast/getCluster) (.getMembers)))

(defn local-member
  "Returns the local member of the cluster."
  []
  (-> (Hazelcast/getCluster) (.getLocalMember)))

(defn- remote-task
  "Returns the appropriate task object for the given target-type and target,
   as well as the process function that should be used on the results that
   are returned from the task."
  [^Callable f target-type target]
  (let [task (condp = target-type
                 nil (DistributedTask. f)
                 :member (DistributedTask. f ^Member target)
                 :members (MultiTask. f ^java.util.Set (set target))
                 :all  (MultiTask. f ^java.util.Set (members))
                 :with-key (DistributedTask. f ^Object target))]
    [task (if (instance? MultiTask task) seq identity)]))

(def target-types #{:member :members :all :with-key})

(defn remote-future-call
  "Like clojure.core/future-call but operates on a Hazelcast cluster.
   The main caveat is that *f needs to be serializable*.
   To help address this limitation you can use the remote-fn helper
   function which requires the function be AOTC and args be serializable:

   (remote-future-call (remote-fn some-fn arg1 arg2 arg3))

   Another option is to use techomancy's serializable-fn:
   https://github.com/technomancy/serializable-fn

   However, serializable-fn has limitations (e.g. only the simplest
   lexical contexts work) at the moment so proceed with caution. Once
   Clojure has complete support for serializable functions via this
   or natively we can add a remote-future macro (a'la clojure.core/future)."
  [f & options]
  (let [{:keys [executor-service] :as options} (apply hash-map options)
        target-type (let [types (set/intersection (set (keys options)) target-types)]
                      (when (> (count types) 1)
                        (throw (Exception. (str "You may only specify a single target. Possible targets: " target-types))))
                      (first types))
        [task process] (remote-task f target-type (get options target-types))
        service (if executor-service (Hazelcast/getExecutorService executor-service) (Hazelcast/getExecutorService))
        ^Future fut (.submit service ^Runnable task)]
    (reify
      clojure.lang.IDeref
      (deref [_] (process (.get fut)))
      Future
      (get [_] (process (.get fut)))
      (get [_ timeout unit] (process (.get fut timeout unit)))
      (isCancelled [_] (.isCancelled fut))
      (isDone [_] (.isDone fut))
      (cancel [_ interrupt?] (.cancel fut interrupt?)))))

(defn eval-on
  [sexp target]
  (let [eval (Eval. sexp)
        [task _] (if (instance? Member)
               (remote-task eval :member target)
               (remote-task eval :with-key target))]
    (-> (Hazelcast/getExecutorService)
         ;; any reason for using #execute here?  If #submit is fine we can use 'remote-future-call
        (.execute task))
    task))

(defn eval-any
  [sexp]
  (remote-future-call (Eval. sexp)))

(defn eval-each
  [sexp nodes]
  (let [task (MultiTask. (Eval. sexp) nodes)]
    (-> (Hazelcast/getExecutorService)
         ;; any reason for using #execute here?  If #submit is fine we can use 'remote-future-call
        (.execute task))
    task))

(defmacro with-tx
  "FIXME needs better docs"
  [& body]
  `(let [tx# (Hazelcast/getTransaction)]
     (try
       (.begin tx#)
       ~@body
       (.commit tx#)
       (catch Exception e#
         (.rollback tx#)
         (throw e#)))))

(defn get-topic
  "Returns a distributed topic for the given key."
  [k]
  (Hazelcast/getTopic k))

(defn get-queue
  "Returns a distributed queue for the given key."
  [k]
  (Hazelcast/getQueue k))

(defn get-map
  "Returns a distributed map for the given key."
  [k]
  (Hazelcast/getMap k))

(defn get-mmap
  "Returns a distributed multi-map for the given key."
  [k]
  (Hazelcast/getMultiMap k))

(defn get-list
  "Returns a distributed list for the given key."
  [k]
  (Hazelcast/getList k))

(defn get-set
  "Returns a distributed set for the given key."
  [k]
  (Hazelcast/getSet k))
