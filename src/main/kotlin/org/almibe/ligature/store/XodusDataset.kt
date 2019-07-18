/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.almibe.ligature.*
import java.lang.RuntimeException
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Stream
import kotlin.concurrent.withLock

internal object WriteLock {
    val lock = ReentrantLock()
}

internal class XodusDataset private constructor(private val name: String,
                                                private val environment: Environment): Dataset {
    private val sparqlRunner = SparqlRunner(environment)

    companion object {
        val suffixes = mapOf(
            "cntr" to "#cntr",
            "graphId" to "#gid",
            "idGraph" to "#idg",
            "nodeId" to "#nid",
            "idNode" to "#idn",
            "literalId" to "#lid",
            "idLiteral" to "#idl",
            "spo" to "#spo",
            "sop" to "#sop",
            "pos" to "#pos",
            "pso" to "#pso",
            "osp" to "#osp",
            "ops" to "#ops"
        )

        fun createOrOpen(name: String, environment: Environment): XodusDataset {
            environment.executeInTransaction { txn ->
                suffixes.values.forEach {  suffix ->
                    environment.openStore("$name$suffix", StoreConfig.WITHOUT_DUPLICATES, txn)
                }
            }
            return XodusDataset(name, environment)
        }

        fun delete(name: String, environment: Environment) {
            WriteLock.lock.withLock {
                environment.executeInExclusiveTransaction {  txn ->
                    suffixes.values.forEach { suffix ->
                        val storeName = "$name$suffix"
                        val store = environment.openStore(storeName, StoreConfig.USE_EXISTING, txn, false)
                        if (store != null) {
                            environment.removeStore(storeName, txn)
                        }
                    }
                }
            }
        }
    }

    override fun getDatasetName(): String = name

    override fun addStatements(statements: Collection<Quad>) {
        WriteLock.lock.withLock {
            environment.executeInExclusiveTransaction { txn ->
                statements.forEach { statement ->
                    val graphId = fetchOrCreateGraphId(statement.graph, txn)
                    val subjectId = fetchOrCreateSubjectId(statement.subject, txn)
                    val predicateId = fetchOrCreatePredicateId(statement.predicate, txn)
                    val objectId = fetchOrCreateObjectId(statement.`object`, txn)
                    addStatement(graphId, subjectId, predicateId, objectId, txn)
                }
            }
        }
    }

    override fun executeSparql(sparql: String): Stream<List<SparqlResultField>> {
        return sparqlRunner.executeSparql(sparql)
    }

    override fun allStatements(): Stream<Quad> { //TODO rewrite to use streams better
        return environment.computeInReadonlyTransaction { txn ->
            val res = mutableListOf<Quad>()
            val spo = environment.openStore("$name${suffixes["spo"]}", StoreConfig.USE_EXISTING, txn)
            val cur = spo.openCursor(txn)

            while(cur.next) {
                val quad = EncodedQuad.fromByteIterable(cur.key)
                val graph = graphFromId(quad.graph, txn)
                val subject = subjectFromId(quad.first, txn)
                val predicate = predicateFromId(quad.second, txn)
                val `object` = objectFromId(quad.third, txn)
                res.add(Quad(subject, predicate, `object`, graph))
            }
            res.stream()
        }
    }

    override fun removeStatements(statements: Collection<Quad>) {
        WriteLock.lock.withLock {
            environment.executeInExclusiveTransaction { txn ->
                for (statement in statements) {
                    val graphId = fetchGraphId(statement.graph, txn) ?: continue
                    val subjectId = fetchSubjectId(statement.subject, txn) ?: continue
                    val predicateId = fetchPredicateId(statement.predicate, txn) ?: continue
                    val objectId = fetchObjectId(statement.`object`, txn) ?: continue
                    removeStatement(graphId, subjectId, predicateId, objectId, txn)
                }
            }
        }
    }

    private fun fetchGraphId(graph: Graph, txn: Transaction): Long? {
        val store = environment.openStore("$name${suffixes["gid"]}", StoreConfig.USE_EXISTING, txn)
        val res = when (graph) {
            is DefaultGraph -> store.get(txn, StringBinding.stringToEntry(""))
            is NamedGraph -> store.get(txn, StringBinding.stringToEntry("<${graph.iri.value}>"))
        }
        return if (res == null) {
            res
        } else {
            LongBinding.entryToLong(res)
        }
    }

    private fun fetchSubjectId(subject: Subject, txn: Transaction): Long? {
        val store = environment.openStore("$name${suffixes["nodeId"]}", StoreConfig.USE_EXISTING, txn)
        val res = when (subject) {
            is IRI -> store.get(txn, StringBinding.stringToEntry("<${subject.value}>"))
            is BlankNode -> store.get(txn, StringBinding.stringToEntry("_:${subject.label}"))
            else -> throw RuntimeException("Unexpected Subject (only IRI and BlankNode allowed) $subject")
        }
        return if (res == null) {
            res
        } else {
            LongBinding.entryToLong(res)
        }
    }

    private fun fetchPredicateId(predicate: Predicate, txn: Transaction): Long? {
        val store = environment.openStore("$name${suffixes["nodeId"]}", StoreConfig.USE_EXISTING, txn)
        val res = when (predicate) {
            is IRI -> store.get(txn, StringBinding.stringToEntry("<${predicate.value}>"))
            else -> throw RuntimeException("Unexpected Predicate (only IRI allowed) $predicate")
        }
        return if (res == null) {
            res
        } else {
            LongBinding.entryToLong(res)
        }
    }

    private fun fetchObjectId(`object`: Object, txn: Transaction): Long? {
        val res = when (`object`) {
            is IRI -> {
                val store = environment.openStore("$name${suffixes["nodeId"]}", StoreConfig.USE_EXISTING, txn)
                store.get(txn, StringBinding.stringToEntry("<${`object`.value}>"))
            }
            is BlankNode -> {
                val store = environment.openStore("$name${suffixes["nodeId"]}", StoreConfig.USE_EXISTING, txn)
                store.get(txn, StringBinding.stringToEntry("_:${`object`.label}"))
            }
            is LangLiteral -> {
                val store = environment.openStore("$name${suffixes["literalId"]}", StoreConfig.USE_EXISTING, txn)
                store.get(txn, StringBinding.stringToEntry("${`object`.value}@${`object`.langTag}"))
            }
            is TypedLiteral -> {
                val store = environment.openStore("$name${suffixes["literalId"]}", StoreConfig.USE_EXISTING, txn)
                store.get(txn, StringBinding.stringToEntry("${`object`.value}^^<${`object`.datatypeIRI.value}>"))
            }
            else -> throw RuntimeException("Unexpected Object (only IRI, BlankNode, or Literal allowed) $`object`")
        }
        return if (res == null) {
            res
        } else {
            LongBinding.entryToLong(res)
        }
    }

    private fun fetchOrCreateGraphId(graph: Graph, txn: Transaction): Long {
        val id = fetchGraphId(graph, txn)
        return if (id != null) {
            id
        } else {
            val newId = fetchNextId()
            val graphName = when (graph) {
                is DefaultGraph -> ""
                is NamedGraph -> "<${graph.iri.value}>"
            }
            val graphId = environment.openStore("$name${suffixes["graphId"]}", StoreConfig.USE_EXISTING, txn)
            val idGraph = environment.openStore("$name${suffixes["idGraph"]}", StoreConfig.USE_EXISTING, txn)
            graphId.put(txn, StringBinding.stringToEntry(graphName), LongBinding.longToEntry(newId))
            idGraph.putRight(txn, LongBinding.longToEntry(newId), StringBinding.stringToEntry(graphName))
            newId
        }
    }

    private fun fetchOrCreateSubjectId(subject: Subject, txn: Transaction): Long {
        val id = fetchSubjectId(subject, txn)
        return if (id != null) {
            id
        } else {
            val newId = fetchNextId()
            val subjectName = when (subject) {
                is IRI -> "<${subject.value}>"
                is BlankNode -> "_:${subject.label}"
                else -> throw RuntimeException("Unexpected Subject (only IRI and BlankNode allowed) $subject")
            }
            val nodeId = environment.openStore("$name${suffixes["nodeId"]}", StoreConfig.USE_EXISTING, txn)
            val idNode = environment.openStore("$name${suffixes["idNode"]}", StoreConfig.USE_EXISTING, txn)
            nodeId.put(txn, StringBinding.stringToEntry(subjectName), LongBinding.longToEntry(newId))
            idNode.putRight(txn, LongBinding.longToEntry(newId), StringBinding.stringToEntry(subjectName))
            newId
        }
    }

    private fun fetchOrCreatePredicateId(predicate: Predicate, txn: Transaction): Long {
        val id = fetchPredicateId(predicate, txn)
        return if (id != null) {
            id
        } else {
            val newId = fetchNextId()
            val predicateName = when (predicate) {
                is IRI -> "<${predicate.value}>"
                else -> throw RuntimeException("Unexpected Predicate (only IRI allowed) $predicate")
            }
            val nodeId = environment.openStore("$name${suffixes["nodeId"]}", StoreConfig.USE_EXISTING, txn)
            val idNode = environment.openStore("$name${suffixes["idNode"]}", StoreConfig.USE_EXISTING, txn)
            nodeId.put(txn, StringBinding.stringToEntry(predicateName), LongBinding.longToEntry(newId))
            idNode.putRight(txn, LongBinding.longToEntry(newId), StringBinding.stringToEntry(predicateName))
            newId
        }
    }

    private fun fetchOrCreateObjectId(`object`: Object, txn: Transaction): Long {
        val id = fetchObjectId(`object`, txn)
        if (id != null) {
            return id
        } else {
            val newId = fetchNextId()
            TODO()
        }
    }

    private fun fetchNextId(): Long {
        TODO()
    }

    private fun graphFromId(id: Long, txn: Transaction): Graph {
        TODO()
    }

    private fun subjectFromId(id: Long, txn: Transaction): Subject {
        TODO()
    }

    private fun predicateFromId(id: Long, txn: Transaction): Predicate {
        TODO()
    }

    private fun objectFromId(id: Long, txn: Transaction): Object {
        TODO()
    }

    private fun addStatement(graphId: Long, subjectId: Long, predicateId: Long, objectId: Long, txn: Transaction) {
        val spo = EncodedQuad(graphId, subjectId, predicateId, objectId)
        val sop = EncodedQuad(graphId, subjectId, objectId, predicateId)
        val pos = EncodedQuad(graphId, predicateId, objectId, subjectId)
        val pso = EncodedQuad(graphId, predicateId, subjectId, objectId)
        val osp = EncodedQuad(graphId, objectId, subjectId, predicateId)
        val ops = EncodedQuad(graphId, objectId, predicateId, subjectId)

        val spoStore = environment.openStore("$name${suffixes["spo"]}", StoreConfig.USE_EXISTING, txn)
        val sopStore = environment.openStore("$name${suffixes["sop"]}", StoreConfig.USE_EXISTING, txn)
        val posStore = environment.openStore("$name${suffixes["pos"]}", StoreConfig.USE_EXISTING, txn)
        val psoStore = environment.openStore("$name${suffixes["pso"]}", StoreConfig.USE_EXISTING, txn)
        val ospStore = environment.openStore("$name${suffixes["osp"]}", StoreConfig.USE_EXISTING, txn)
        val opsStore = environment.openStore("$name${suffixes["ops"]}", StoreConfig.USE_EXISTING, txn)

        spoStore.put(txn, spo.toByteIterable(), BooleanBinding.booleanToEntry(true))
        sopStore.put(txn, sop.toByteIterable(), BooleanBinding.booleanToEntry(true))
        posStore.put(txn, pos.toByteIterable(), BooleanBinding.booleanToEntry(true))
        psoStore.put(txn, pso.toByteIterable(), BooleanBinding.booleanToEntry(true))
        ospStore.put(txn, osp.toByteIterable(), BooleanBinding.booleanToEntry(true))
        opsStore.put(txn, ops.toByteIterable(), BooleanBinding.booleanToEntry(true))
    }

    private fun removeStatement(graphId: Long, subjectId: Long, predicateId: Long, objectId: Long, txn: Transaction) {
        val spo = EncodedQuad(graphId, subjectId, predicateId, objectId)
        val sop = EncodedQuad(graphId, subjectId, objectId, predicateId)
        val pos = EncodedQuad(graphId, predicateId, objectId, subjectId)
        val pso = EncodedQuad(graphId, predicateId, subjectId, objectId)
        val osp = EncodedQuad(graphId, objectId, subjectId, predicateId)
        val ops = EncodedQuad(graphId, objectId, predicateId, subjectId)

        val spoStore = environment.openStore("$name${suffixes["spo"]}", StoreConfig.USE_EXISTING, txn)
        val sopStore = environment.openStore("$name${suffixes["sop"]}", StoreConfig.USE_EXISTING, txn)
        val posStore = environment.openStore("$name${suffixes["pos"]}", StoreConfig.USE_EXISTING, txn)
        val psoStore = environment.openStore("$name${suffixes["pso"]}", StoreConfig.USE_EXISTING, txn)
        val ospStore = environment.openStore("$name${suffixes["osp"]}", StoreConfig.USE_EXISTING, txn)
        val opsStore = environment.openStore("$name${suffixes["ops"]}", StoreConfig.USE_EXISTING, txn)

        spoStore.delete(txn, spo.toByteIterable())
        sopStore.delete(txn, sop.toByteIterable())
        posStore.delete(txn, pos.toByteIterable())
        psoStore.delete(txn, pso.toByteIterable())
        ospStore.delete(txn, osp.toByteIterable())
        opsStore.delete(txn, ops.toByteIterable())

        cleanUpReferences(graphId, subjectId, predicateId, objectId, txn)
    }

    private fun cleanUpReferences(graphId: Long, subjectId: Long, predicateId: Long, objectId: Long, txn: Transaction) {
        TODO()
    }
}
