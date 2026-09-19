;; SPDX-License-Identifier: MIT
;; Moved from hive-mcp.swarm.datascript.registry.

(ns hive-datascript.swarm.registry
  "DataScript implementation of all ISP-segregated swarm protocols
   (hive-spi.swarm.protocol), plus the default instances a host installs."
  (:require [datascript.core :as d]
            [hive-spi.swarm.protocol :as proto]
            [hive-datascript.swarm.connection :as conn]
            [hive-datascript.swarm.lings :as lings]
            [hive-datascript.swarm.queries :as queries]
            [hive-datascript.swarm.coordination.wrap-queue :as wrap-queue]
            [hive-datascript.swarm.coordination.coordinator :as coordinator]
            [hive-datascript.swarm.coordination.cleanup :as cleanup]
            [hive-datascript.swarm.coordination.session-registry :as session-registry]))

(defrecord DataScriptRegistry []
  proto/ISwarmRegistry

  (add-slave! [_ slave-id opts]
    (lings/add-slave! slave-id opts))

  (get-slave [_ slave-id]
    (queries/get-slave slave-id))

  (update-slave! [_ slave-id updates]
    (lings/update-slave! slave-id updates))

  (remove-slave! [_ slave-id]
    (lings/remove-slave! slave-id))

  (get-all-slaves [_]
    (queries/get-all-slaves))

  (get-slaves-by-status [_ status]
    (queries/get-slaves-by-status status))

  (get-slaves-by-project [_ project-id]
    (queries/get-slaves-by-project project-id))

  (add-task! [_ task-id slave-id opts]
    (lings/add-task! task-id slave-id opts))

  (get-task [_ task-id]
    (queries/get-task task-id))

  (update-task! [_ task-id updates]
    (lings/update-task! task-id updates))

  (get-tasks-for-slave [_ slave-id]
    (queries/get-tasks-for-slave slave-id))

  (get-tasks-for-slave [_ slave-id status]
    (queries/get-tasks-for-slave slave-id status)))

(defrecord DataScriptClaimStore []
  proto/IClaimStore

  (-claim-file! [_ file-path slave-id]
    (lings/claim-file! file-path slave-id))

  (-claim-file! [_ file-path slave-id opts]
    (lings/claim-file! file-path slave-id opts))

  (-release-claim! [_ file-path]
    (lings/release-claim! file-path))

  (-release-claims-for-slave! [_ slave-id]
    (lings/release-claims-for-slave! slave-id))

  (-release-claims-for-task! [_ task-id]
    (lings/release-claims-for-task! task-id))

  (-get-claims-for-file [_ file-path]
    (queries/get-claims-for-file file-path))

  (-get-all-claims [_]
    (queries/get-all-claims))

  (-has-conflict? [_ file-path requesting-slave]
    (queries/has-conflict? file-path requesting-slave))

  (-check-file-conflicts [_ requesting-slave files]
    (queries/check-file-conflicts requesting-slave files))

  (-refresh-claim! [_ file-path]
    (lings/refresh-claim! file-path))

  (-cleanup-stale-claims! [_]
    (lings/cleanup-stale-claims!))

  (-cleanup-stale-claims! [_ threshold-ms]
    (lings/cleanup-stale-claims! threshold-ms))

  (-archive-claim-to-history! [_ file-path opts]
    (lings/archive-claim-to-history! file-path opts))

  (-get-recent-claim-history [_ opts]
    (apply queries/get-recent-claim-history (mapcat identity opts)))

  (-add-to-wait-queue! [_ ling-id file-path]
    (lings/add-to-wait-queue! ling-id file-path)))

(defrecord DataScriptCriticalOps []
  proto/ICriticalOps

  (-enter-critical-op! [_ slave-id op-type]
    (lings/enter-critical-op! slave-id op-type))

  (-exit-critical-op! [_ slave-id op-type]
    (lings/exit-critical-op! slave-id op-type))

  (-get-critical-ops [_ slave-id]
    (lings/get-critical-ops slave-id))

  (-can-kill? [_ slave-id]
    (lings/can-kill? slave-id)))

(defrecord DataScriptCoordination []
  proto/ICoordination

  (-add-wrap-notification! [_ wrap-id opts]
    (wrap-queue/add-wrap-notification! wrap-id opts))

  (-get-unprocessed-wraps [_]
    (wrap-queue/get-unprocessed-wraps))

  (-get-unprocessed-wraps-for-project [_ project-id]
    (wrap-queue/get-unprocessed-wraps-for-project project-id))

  (-get-unprocessed-wraps-for-hierarchy [_ project-id-prefix]
    (wrap-queue/get-unprocessed-wraps-for-hierarchy project-id-prefix))

  (-mark-wrap-processed! [_ wrap-id]
    (wrap-queue/mark-wrap-processed! wrap-id))

  (-register-coordinator! [_ coordinator-id opts]
    (coordinator/register-coordinator! coordinator-id opts))

  (-update-heartbeat! [_ coordinator-id]
    (coordinator/update-heartbeat! coordinator-id))

  (-get-coordinator [_ coordinator-id]
    (coordinator/get-coordinator coordinator-id))

  (-get-all-coordinators [_]
    (coordinator/get-all-coordinators))

  (-get-coordinators-for-project [_ project]
    (coordinator/get-coordinators-for-project project))

  (-mark-coordinator-terminated! [_ coordinator-id]
    (coordinator/mark-coordinator-terminated! coordinator-id))

  (-cleanup-stale-coordinators! [_]
    (cleanup/cleanup-stale-coordinators!))

  (-cleanup-stale-coordinators! [_ opts]
    (cleanup/cleanup-stale-coordinators! opts))

  (-remove-coordinator! [_ coordinator-id]
    (coordinator/remove-coordinator! coordinator-id))

  (-register-completed-task! [_ task-id opts]
    (session-registry/register-completed-task! task-id opts))

  (-get-completed-tasks-this-session [_]
    (session-registry/get-completed-tasks-this-session))

  (-get-completed-tasks-this-session [_ opts]
    (apply session-registry/get-completed-tasks-this-session (mapcat identity opts)))

  (-clear-completed-tasks! [_]
    (session-registry/clear-completed-tasks!))

  (-clear-completed-tasks! [_ task-ids]
    (session-registry/clear-completed-tasks! task-ids))

  (-register-kanban-movement! [_ opts]
    (session-registry/register-kanban-movement! opts))

  (-get-kanban-movements-this-session [_]
    (session-registry/get-kanban-movements-this-session))

  (-get-kanban-movements-this-session [_ opts]
    (apply session-registry/get-kanban-movements-this-session (mapcat identity opts)))

  (-clear-kanban-movements! [_]
    (session-registry/clear-kanban-movements!))

  (-clear-kanban-movements! [_ movement-ids]
    (session-registry/clear-kanban-movements! movement-ids)))

(defrecord DataScriptDb []
  proto/ISwarmDb

  (-transact! [_ tx-data]
    (d/transact! (conn/ensure-conn) tx-data))

  (-current-db [_]
    @(conn/ensure-conn))

  (-listen! [_ key callback]
    (d/listen! (conn/ensure-conn) key callback)
    key)

  (-unlisten! [_ key]
    (d/unlisten! (conn/ensure-conn) key))

  (-db-stats [_]
    (queries/db-stats))

  (-reset-db! [_]
    (conn/reset-conn!))

  (-close! [_]
    (conn/reset-conn!)))

;;; =============================================================================
;;; Default Instances
;;; =============================================================================

(defonce default-registry (->DataScriptRegistry))
(defonce default-claim-store (->DataScriptClaimStore))
(defonce default-critical-ops (->DataScriptCriticalOps))
(defonce default-coordination (->DataScriptCoordination))
(defonce default-db (->DataScriptDb))

(defn get-default-registry [] default-registry)
(defn get-default-claim-store [] default-claim-store)
(defn get-default-critical-ops [] default-critical-ops)
(defn get-default-coordination [] default-coordination)
(defn get-default-db [] default-db)
