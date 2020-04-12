/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.libraryweasel.ligature.*
import java.lang.RuntimeException

internal class XodusLigatureWriteTx(private val environment: Environment): WriteTx {
    private val writeTx = environment.beginTransaction()

    @Synchronized override fun addStatement(collection: CollectionName, statement: Statement) {
        if (isOpen()) {
            val store = environment.openStore(collection.name, StoreConfig.WITHOUT_DUPLICATES, writeTx)
            val subjectId = checkEntityId(store, writeTx, statement.subject)
            val predicateId = getOrCreatePredicateId(store, statement.predicate)
            val objectId = getOrCreateObjectId(store, statement.`object`)
            val contextId = checkEntityId(store, writeTx, statement.context)
            addStatement(subjectId, predicateId, objectId, contextId, store)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    private fun addStatement(subjectId: Long, predicateId: Long, objectId: Long, contextId: Long, store: Store) {
        val spoc = EncodedQuad(Prefixes.SPOC.prefix, subjectId, predicateId, objectId, contextId)
        val sopc = EncodedQuad(Prefixes.SOPC.prefix, subjectId, objectId, predicateId, contextId)
        val posc = EncodedQuad(Prefixes.POSC.prefix, predicateId, objectId, subjectId, contextId)
        val psoc = EncodedQuad(Prefixes.PSOC.prefix, predicateId, subjectId, objectId, contextId)
        val ospc = EncodedQuad(Prefixes.OSPC.prefix, objectId, subjectId, predicateId, contextId)
        val opsc = EncodedQuad(Prefixes.OPSC.prefix, objectId, predicateId, subjectId, contextId)
        val cspo = EncodedQuad(Prefixes.CSPO.prefix, contextId, subjectId, predicateId, objectId)

        store.put(writeTx, spoc.toByteIterable(), BooleanBinding.booleanToEntry(true))
        store.put(writeTx, sopc.toByteIterable(), BooleanBinding.booleanToEntry(true))
        store.put(writeTx, posc.toByteIterable(), BooleanBinding.booleanToEntry(true))
        store.put(writeTx, psoc.toByteIterable(), BooleanBinding.booleanToEntry(true))
        store.put(writeTx, ospc.toByteIterable(), BooleanBinding.booleanToEntry(true))
        store.put(writeTx, opsc.toByteIterable(), BooleanBinding.booleanToEntry(true))
        store.put(writeTx, cspo.toByteIterable(), BooleanBinding.booleanToEntry(true))
    }

    @Synchronized override fun cancel() = writeTx.abort()

    @Synchronized override fun commit() {
        writeTx.commit()
    }

    @Synchronized override fun createCollection(collection: CollectionName) {
        if (isOpen()) {
            environment.openStore(collection.name, StoreConfig.WITHOUT_DUPLICATES, writeTx)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun deleteCollection(collection: CollectionName) {
        if (isOpen()) {
            if (environment.storeExists(collection.name, writeTx)) {
                environment.removeStore(collection.name, writeTx)
            }
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun isOpen(): Boolean = !writeTx.isFinished

    @Synchronized override fun newEntity(collection: CollectionName): Entity {
        if (isOpen()) {
            val store = environment.openStore(collection.name, StoreConfig.WITHOUT_DUPLICATES, writeTx)
            val result = store.get(writeTx, IntegerBinding.intToEntry(Prefixes.EntityIdCounter.prefix))
            val nextId = if (result == null) {
                1
            } else {
                LongBinding.entryToLong(result)+1
            }
            store.put(writeTx, IntegerBinding.intToEntry(Prefixes.EntityIdCounter.prefix), LongBinding.longToEntry(nextId))
            return Entity(nextId)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun removeStatement(collection: CollectionName, statement: Statement) {
        TODO("Not yet implemented")
    }

    private fun getOrCreatePredicateId(store: Store, predicate: Predicate): Long {
        return getPredicateId(store, writeTx, predicate) ?: createPredicateId(store, predicate)
    }

    private fun getOrCreateObjectId(store: Store, `object`: Object): Long {
        return getObjectId(store, writeTx, `object`) ?: createObjectId(store, `object`)
    }

    private fun createPredicateId(store: Store, predicate: Predicate): Long {
        TODO("Not yet implemented.")
    }

    private fun createObjectId(store: Store, `object`: Object): Long {
        TODO("Not yet implemented.")
    }
}
