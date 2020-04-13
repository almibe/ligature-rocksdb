/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.libraryweasel.ligature.*
import java.lang.RuntimeException

internal class XodusLigatureReadTx(private val environment: Environment): ReadTx {
    private val readTx = environment.beginReadonlyTransaction()

    override suspend fun allStatements(collection: CollectionName): Flow<Statement> {
        return flow<Statement> {
            val store = environment.openStore(collection.name, StoreConfig.WITHOUT_DUPLICATES, readTx)
            val cursor = store.openCursor(readTx)
            cursor.use {
                val result = cursor.getSearchKeyRange(IntegerBinding.intToEntry(Prefixes.SPOC.prefix))
                if (result != null && keyHasPrefix(cursor.key, Prefixes.SPOC.prefix)) {
                    TODO()
                } else {
                    return@flow
                }
            }
        }
    }

    private fun keyHasPrefix(key: ByteIterable, prefix: Int): Boolean {
        TODO("check if the cursor.key starts with the correct prefix")
    }

    override suspend fun cancel() {
        readTx.abort()
    }

    override suspend fun collections(): Flow<CollectionName> {
        return if (isOpen()) { environment.getAllStoreNames(readTx).asFlow().map {
            CollectionName(it)
        }} else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override suspend fun collections(prefix: CollectionName): Flow<CollectionName> {
        TODO("Not yet implemented")
    }

    override suspend fun collections(from: CollectionName, to: CollectionName): Flow<CollectionName> {
        TODO("Not yet implemented")
    }

    override suspend fun isOpen(): Boolean = !readTx.isFinished

    override suspend fun matchStatements(collection: CollectionName, subject: Entity?, predicate: Predicate?, `object`: Object?, context: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override suspend fun matchStatements(collection: CollectionName, subject: Entity?, predicate: Predicate?, range: Range<*>, context: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }
}

