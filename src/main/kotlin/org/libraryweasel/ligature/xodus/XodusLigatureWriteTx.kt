/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import org.libraryweasel.ligature.*
import java.lang.RuntimeException

internal class XodusLigatureWriteTx(private val environment: Environment): WriteTx {
    private val writeTx = environment.beginTransaction()

    override fun addStatement(collection: CollectionName, statement: Statement) {
        if (isOpen()) {
            val store = environment.openStore(collection.name, StoreConfig.WITHOUT_DUPLICATES, writeTx)
            val subjectId = getOrCreateEntityId(statement.subject)
            val predicateId = getOrCreatePredicateId(statement.predicate)
            val objectId = getOrCreateObjectId(statement.`object`)
            val contextId = getOrCreateEntityId(statement.context)

        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun cancel() = writeTx.abort()

    override fun commit() {
        writeTx.commit()
    }

    override fun createCollection(collection: CollectionName) {
        if (isOpen()) {
            environment.openStore(collection.name, StoreConfig.WITHOUT_DUPLICATES, writeTx)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun deleteCollection(collection: CollectionName) {
        if (isOpen()) {
            if (environment.storeExists(collection.name, writeTx)) {
                environment.removeStore(collection.name, writeTx)
            }
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun isOpen(): Boolean = !writeTx.isFinished

    override fun newEntity(collection: CollectionName): Entity {
        TODO("Not yet implemented")
    }

    override fun removeStatement(collection: CollectionName, statement: Statement) {
        TODO("Not yet implemented")
    }

    private fun getOrCreateEntityId(entity: Entity): Long {
        TODO("Not yet implemented")
    }

    private fun getOrCreatePredicateId(predicate: Predicate): Long {
        TODO("Not yet implemented")
    }

    private fun getOrCreateObjectId(`object`: Object): Long {
        TODO("Not yet implemented")
    }
}
