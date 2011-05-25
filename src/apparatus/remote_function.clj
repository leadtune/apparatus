;; Copyright 2010-2011 Tim Dysinger <tim@dysinger.net>
;; Original work by Ben Mabey <ben@benmabey.com>

;; Licensed under the Apache License, Version 2.0 (the "License"); you
;; may not use this file except in compliance with the License.  You
;; may obtain a copy of the License at

;; http://www.apache.org/licenses/LICENSE-2.0

;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
;; implied.  See the License for the specific language governing
;; permissions and limitations under the License.

(ns apparatus.remote-function)

(defrecord RemoteFunction [function args]
  java.util.concurrent.Callable
  (call [_] (apply function args)))

(defn remote-fn
  "Wraps the given fn and args in a RemoteFunction that can be passed to
   HazelCast's distributed executor service.  You can think of RemoteFunction
   as an annoying way of creating an anonymous function that can be executed
   somewhere on the cluster. This annoyance is needed because the distrubuted
   executor requires that it's Callables be serializable.  This also means
   that your fn be AOTC and available on all nodes and the other args be
   serializable as well.  Once clojure gets full support for serializable
   functions this won't be needed."
  [fn & args]
  (RemoteFunction. fn args))
