/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.Transaction
import org.libraryweasel.ligature.Entity
import org.libraryweasel.ligature.Object
import org.libraryweasel.ligature.Predicate

internal enum class Prefixes(val prefix: Int) {
    InternalCounter(0),
    ExternalCounter(1),
    EntityId(2),
    PredicateId(3),
    SPOC(4),
    SOPC(5),
    PSOC(6),
    POSC(7),
    OSPC(8),
    OPSC(9),
    CSPO(10),
    LangLiteral(11),
    StringLiteral(12),
    BooleanLiteral(13),
    LongLiteral(14),
    DoubleLiteral(15)
}

internal fun getEntityId(store: Store, tx: Transaction, entity: Entity): Long? {
    val result = store.get(tx, CompoundByteIterable(arrayOf(
            IntegerBinding.intToEntry(Prefixes.EntityId.prefix),
            StringBinding.stringToEntry(entity.identifier))))
    return if (result == null) {
        null
    } else {
        LongBinding.entryToLong(result)
    }
}

internal fun getPredicateId(store: Store, tx: Transaction, predicate: Predicate): Long? {
    val result = store.get(tx, CompoundByteIterable(arrayOf(
            IntegerBinding.intToEntry(Prefixes.PredicateId.prefix),
            StringBinding.stringToEntry(predicate.identifier))))
    return if (result == null) {
        null
    } else {
        LongBinding.entryToLong(result)
    }
}

internal fun getObjectId(store: Store, tx: Transaction, `object`: Object): Long? {
    TODO("Not yet implemented")
}


//    override fun allStatements(): Stream<Statement> { //TODO rewrite to use streams better
//        return environment.computeInReadonlyTransaction { txn ->
//            val res = mutableListOf<Statement>()
//            val spo = environment.openStore(Suffixes.EAVG.storeName(name), StoreConfig.USE_EXISTING, txn)
//            val cur = spo.openCursor(txn)
//
//            while(cur.next) {
//                val quad = EncodedQuad.fromByteIterable(cur.key)
//                val graph = graphFromId(quad.fourth, txn)
//                val subject = subjectFromId(quad.first, txn)
//                val predicate = predicateFromId(quad.second, txn)
//                val `object` = objectFromId(quad.third, txn)
//                res.add(Statement(subject, predicate, `object`, graph))
//            }
//            res.stream()
//        }
//    }
//
//    override fun removeStatements(statements: Collection<Statement>) {
//        WriteLock.lock.withLock {
//            environment.executeInExclusiveTransaction { txn ->
//                for (statement in statements) {
//                    val graphId = fetchGraphId(statement.graph, txn) ?: continue
//                    val subjectId = fetchSubjectId(statement.subject, txn) ?: continue
//                    val predicateId = fetchPredicateId(statement.predicate, txn) ?: continue
//                    val objectId = fetchObjectId(statement.`object`, txn) ?: continue
//                    removeStatement(graphId, subjectId, predicateId, objectId, txn)
//                }
//            }
//        }
//    }
//
//    private fun fetchGraphId(graph: Graph, txn: Transaction): Long? {
//        val store = environment.openStore(Suffixes.GraphId.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val encodedGraph = encodeGraph(graph)
//        val res = store.get(txn, encodedGraph)
//        return if (res == null) {
//            res
//        } else {
//            LongBinding.entryToLong(res)
//        }
//    }
//
//    private fun fetchSubjectId(subject: Subject, txn: Transaction): Long? {
//        val store = environment.openStore(Suffixes.NodeId.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val encodedSubject = encodeSubject(subject)
//        val res = store.get(txn, encodedSubject)
//        return if (res == null) {
//            res
//        } else {
//            LongBinding.entryToLong(res)
//        }
//    }
//
//    private fun fetchPredicateId(predicate: Predicate, txn: Transaction): Long? {
//        val store = environment.openStore(Suffixes.NodeId.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val encodedPredicate = encodePredicate(predicate)
//        val res = store.get(txn, encodedPredicate)
//        return if (res == null) {
//            res
//        } else {
//            LongBinding.entryToLong(res)
//        }
//    }
//
//    private fun fetchObjectId(`object`: Object, txn: Transaction): Long? {
//        val res = when (`object`) {
//            is IRI, is BlankNode -> {
//                val store = environment.openStore(Suffixes.NodeId.storeName(name), StoreConfig.USE_EXISTING, txn)
//                store.get(txn, encodeSubject(`object` as Subject))
//            }
//            is Literal -> {
//                fetchEncodedLiteral(`object`, txn)
//            }
//            else -> throw RuntimeException("Unexpected Object (only IRI, BlankNode, or Literal allowed) $`object`")
//        }
//        return if (res == null) {
//            res
//        } else {
//            LongBinding.entryToLong(res)
//        }
//    }
//
//    private fun fetchEncodedLiteral(literal: Literal, txn: Transaction): ByteIterable? {
//        return when (literal) {
//            is LangLiteral -> {
//                val store = environment.openStore(Suffixes.LiteralId.storeName(name), StoreConfig.USE_EXISTING, txn)
//                store.get(txn, encodeLangLiteral(literal))
//            }
//            is TypedLiteral -> {
//                val store = environment.openStore(Suffixes.LiteralId.storeName(name), StoreConfig.USE_EXISTING, txn)
//                val tid = environment.openStore(Suffixes.LiteralTypeId.storeName(name), StoreConfig.USE_EXISTING, txn)
//                val id = tid.get(txn, StringBinding.stringToEntry(literal.datatypeIRI.value)) ?: return null
//                val longId = LongBinding.entryToLong(id)
//                store.get(txn, encodeTypedLiteral(literal, longId))
//            }
//        }
//    }
//
//    private fun fetchOrCreateGraphId(graph: Graph, txn: Transaction): Long {
//        val id = fetchGraphId(graph, txn)
//        return if (id != null) {
//            id
//        } else {
//            val newId = fetchNextId(txn)
//            val encodedGraph = encodeGraph(graph)
//            val graphId = environment.openStore(Suffixes.GraphId.storeName(name), StoreConfig.USE_EXISTING, txn)
//            val idGraph = environment.openStore(Suffixes.IdGraph.storeName(name), StoreConfig.USE_EXISTING, txn)
//            graphId.put(txn, encodedGraph, LongBinding.longToEntry(newId))
//            idGraph.putRight(txn, LongBinding.longToEntry(newId), encodedGraph)
//            newId
//        }
//    }
//
//    private fun fetchOrCreateSubjectId(subject: Subject, txn: Transaction): Long {
//        val id = fetchSubjectId(subject, txn)
//        return if (id != null) {
//            id
//        } else {
//            val newId = fetchNextId(txn)
//            val encodedSubject = encodeSubject(subject)
//            val nodeId = environment.openStore(Suffixes.NodeId.storeName(name), StoreConfig.USE_EXISTING, txn)
//            val idNode = environment.openStore(Suffixes.IdNode.storeName(name), StoreConfig.USE_EXISTING, txn)
//            nodeId.put(txn, encodedSubject, LongBinding.longToEntry(newId))
//            idNode.putRight(txn, LongBinding.longToEntry(newId), encodedSubject)
//            newId
//        }
//    }
//
//    private fun fetchOrCreatePredicateId(predicate: Predicate, txn: Transaction): Long {
//        val id = fetchPredicateId(predicate, txn)
//        return if (id != null) {
//            id
//        } else {
//            val newId = fetchNextId(txn)
//            val encodedPredicate = encodePredicate(predicate)
//            val nodeId = environment.openStore(Suffixes.NodeId.storeName(name), StoreConfig.USE_EXISTING, txn)
//            val idNode = environment.openStore(Suffixes.IdNode.storeName(name), StoreConfig.USE_EXISTING, txn)
//            nodeId.put(txn, encodedPredicate, LongBinding.longToEntry(newId))
//            idNode.putRight(txn, LongBinding.longToEntry(newId), encodedPredicate)
//            newId
//        }
//    }
//
//    private fun fetchOrCreateObjectId(`object`: Object, txn: Transaction): Long {
//        val id = fetchObjectId(`object`, txn)
//        return if (id != null) {
//            id
//        } else {
//            val newId = fetchNextId(txn)
//            if (`object` is IRI || `object` is BlankNode) {
//                val encodedObject = encodeSubject(`object` as Subject)
//                val nodeId = environment.openStore(Suffixes.NodeId.storeName(name), StoreConfig.USE_EXISTING, txn)
//                val idNode = environment.openStore(Suffixes.IdNode.storeName(name), StoreConfig.USE_EXISTING, txn)
//                nodeId.put(txn, encodedObject, LongBinding.longToEntry(newId))
//                idNode.putRight(txn, LongBinding.longToEntry(newId), encodedObject)
//            } else if (`object` is Literal) {
//                val encodedLiteral = createEncodedLiteral(`object`, txn)
//                val literalId = environment.openStore(Suffixes.LiteralId.storeName(name), StoreConfig.USE_EXISTING, txn)
//                val idLiteral = environment.openStore(Suffixes.IdLiteral.storeName(name), StoreConfig.USE_EXISTING, txn)
//                literalId.put(txn, encodedLiteral, LongBinding.longToEntry(newId))
//                idLiteral.putRight(txn, LongBinding.longToEntry(newId), encodedLiteral)
//            } else {
//                throw RuntimeException("Unexpected Object (only IRI, BlankNode, or Literal allowed) $`object`")
//            }
//            newId
//        }
//    }
//
//    private fun createEncodedLiteral(literal: Literal, txn: Transaction): ByteIterable {
//        return when (literal) {
//            is LangLiteral -> encodeLangLiteral(literal)
//            is TypedLiteral -> {
//                val encodedIRI = StringBinding.stringToEntry(literal.datatypeIRI.value)
//                val tid = environment.openStore(Suffixes.LiteralTypeId.storeName(name), StoreConfig.USE_EXISTING, txn)
//                val id = tid.get(txn, encodedIRI)
//                val longId = if (id == null) {
//                    val nextId = fetchNextId(txn)
//                    val encodedNextId = LongBinding.longToEntry(nextId)
//                    val idt = environment.openStore(Suffixes.IdLiteralType.storeName(name), StoreConfig.USE_EXISTING, txn)
//                    tid.put(txn, encodedIRI, encodedNextId)
//                    idt.putRight(txn, encodedNextId, encodedIRI)
//                    nextId
//                } else {
//                    LongBinding.entryToLong(id)
//                }
//                encodeTypedLiteral(literal, longId)
//            }
//        }
//    }
//
//    private fun fetchNextId(txn: Transaction): Long {
//        val cntr = environment.openStore(Suffixes.Counter.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val oldValue = cntr.get(txn, StringBinding.stringToEntry("cntr"))
//        return if (oldValue == null) {
//            val firstValue = 0L
//            cntr.put(txn, StringBinding.stringToEntry("cntr"), LongBinding.longToEntry(firstValue))
//            firstValue
//        } else {
//            val newValue = LongBinding.entryToLong(oldValue) + 1L
//            cntr.put(txn, StringBinding.stringToEntry("cntr"), LongBinding.longToEntry(newValue))
//            newValue
//        }
//    }
//
//    private fun graphFromId(id: Long, txn: Transaction): Graph {
//        val idGraph = environment.openStore(Suffixes.IdGraph.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val graph = idGraph.get(txn, LongBinding.longToEntry(id)) ?: throw RuntimeException("Could not find Graph with id = $id.")
//        val graphString = StringBinding.entryToString(graph)
//        return decodeGraph(graphString)
//    }
//
//    private fun subjectFromId(id: Long, txn: Transaction): Subject {
//        val idNode = environment.openStore(Suffixes.IdNode.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val subject = idNode.get(txn, LongBinding.longToEntry(id)) ?: throw RuntimeException("Could not find Subject with id = $id.")
//        val subjectString = StringBinding.entryToString(subject)
//        return decodeSubject(subjectString)
//    }
//
//    private fun predicateFromId(id: Long, txn: Transaction): Predicate {
//        val idNode = environment.openStore(Suffixes.IdNode.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val predicate = idNode.get(txn, LongBinding.longToEntry(id)) ?: throw RuntimeException("Could not find Predicate with id = $id.")
//        val predicateString = StringBinding.entryToString(predicate)
//        return decodePredicate(predicateString)
//    }
//
//    private fun objectFromId(id: Long, txn: Transaction): Object {
//        val idNode = environment.openStore(Suffixes.IdNode.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val nodeObject = idNode.get(txn, LongBinding.longToEntry(id))
//        if (nodeObject != null) {
//            val objectString = StringBinding.entryToString(nodeObject)
//            return decodeSubject(objectString) as Object
//        }
//
//        val idLiteral = environment.openStore(Suffixes.IdLiteral.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val literalObject = idLiteral.get(txn, LongBinding.longToEntry(id))
//        if (literalObject != null) {
//            val objectString = StringBinding.entryToString(literalObject)
//            return handleDecodeLiteral(objectString, txn)
//        }
//        throw RuntimeException("Could not find Object with id = $id")
//    }
//
//    private fun handleDecodeLiteral(literalString: String, txn: Transaction): Literal {
//        val res = decodeLangLiteral(literalString)
//        if (res != null) {
//            return res
//        }
//        val values = literalString.split("^^")
//        if (values.size > 1) {
//            val lastId = values.last().toLongOrNull()
//            if (lastId != null) {
//                val idt = environment.openStore(Suffixes.IdLiteralType.storeName(name), StoreConfig.USE_EXISTING, txn)
//                val typeIri = idt.get(txn, LongBinding.longToEntry(lastId))
//                if (typeIri != null) {
//                    val iri = IRI(StringBinding.entryToString(typeIri))
//                    val typeRes = decodeTypedLiteral(literalString, iri)
//                    if (typeRes != null) {
//                        return typeRes
//                    }
//                } else {
//                    throw RuntimeException("Could not find Type with id = $lastId")
//                }
//            }
//        }
//        throw RuntimeException("Invalid Literal - $literalString")
//    }
//
//    private fun removeStatement(graphId: Long, subjectId: Long, predicateId: Long, objectId: Long, txn: Transaction) {
//        val spo = EncodedQuad(graphId, subjectId, predicateId, objectId)
//        val sop = EncodedQuad(graphId, subjectId, objectId, predicateId)
//        val pos = EncodedQuad(graphId, predicateId, objectId, subjectId)
//        val pso = EncodedQuad(graphId, predicateId, subjectId, objectId)
//        val osp = EncodedQuad(graphId, objectId, subjectId, predicateId)
//        val ops = EncodedQuad(graphId, objectId, predicateId, subjectId)
//
//        val spoStore = environment.openStore(Suffixes.EAVG.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val sopStore = environment.openStore(Suffixes.EVAG.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val posStore = environment.openStore(Suffixes.AVEG.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val psoStore = environment.openStore(Suffixes.AEVG.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val ospStore = environment.openStore(Suffixes.VEAG.storeName(name), StoreConfig.USE_EXISTING, txn)
//        val opsStore = environment.openStore(Suffixes.VAEG.storeName(name), StoreConfig.USE_EXISTING, txn)
//
//        spoStore.delete(txn, spo.toByteIterable())
//        sopStore.delete(txn, sop.toByteIterable())
//        posStore.delete(txn, pos.toByteIterable())
//        psoStore.delete(txn, pso.toByteIterable())
//        ospStore.delete(txn, osp.toByteIterable())
//        opsStore.delete(txn, ops.toByteIterable())
//
//        cleanUpReferences(graphId, subjectId, predicateId, objectId, txn)
//    }
//
//    private fun cleanUpReferences(graphId: Long, subjectId: Long, predicateId: Long, objectId: Long, txn: Transaction) {
//        TODO()
//    }
//}
