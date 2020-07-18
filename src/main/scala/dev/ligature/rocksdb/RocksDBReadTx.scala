/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.rocksdb

import dev.ligature.ReadTx
import org.rocksdb.{RocksDB, TransactionDB}

class RocksDBReadTx(private val db: TransactionDB) extends ReadTx {
    override def allStatements(collection: CollectionName): Flow<Statement> {
        return flow<Statement> {
            val store = environment.openStore(collection.name, StoreConfig.WITHOUT_DUPLICATES, readTx)
            val cursor = store.openCursor(readTx)
            cursor.use {
                var curValue = cursor.getSearchKeyRange(IntegerBinding.intToEntry(Prefixes.SPOC.prefix))
                var curKey = cursor.key
                while (curValue != null && keyHasPrefix(curKey, Prefixes.SPOC.prefix)) {
                    emit(extractStatement(curKey))
                    if (!cursor.next) {
                        return@flow
                    }
                    curValue = cursor.value
                    curKey = cursor.key
                }
            }
        }
    }

    private fun keyHasPrefix(key: ByteIterable, prefix: Int): Boolean {
        return prefix == IntegerBinding.entryToInt(key.subIterable(0, Int.SIZE_BYTES))
    }

    private fun extractStatement(key: ByteIterable): Statement {
        val subject = Entity(extractLong(key, 0))
        val predicate = lookupPredicate(extractLong(key, 1))
        val `object` = lookupObject(extractLong(key, 2))
        val context = Entity(extractLong(key, 3))
        return Statement(subject, predicate, `object`, context)
    }

    private fun extractLong(key: ByteIterable, offset: Int): Long {
        TODO()
    }

    private fun lookupPredicate(id: Long): Predicate {
        TODO()
    }

    private fun lookupObject(id: Long): Object {
        TODO()
    }

    override def cancel() {
        readTx.abort()
    }

    override def collections(): Flow<CollectionName> {
        return if (isOpen()) { environment.getAllStoreNames(readTx).asFlow().map {
            CollectionName(it)
        }} else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override def collections(prefix: CollectionName): Flow<CollectionName> {
        TODO("Not yet implemented")
    }

    override def collections(from: CollectionName, to: CollectionName): Flow<CollectionName> {
        TODO("Not yet implemented")
    }

    override def isOpen(): Boolean = !readTx.isFinished

    override def matchStatements(collection: CollectionName, subject: Entity?, predicate: Predicate?, `object`: Object?, context: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override def matchStatements(collection: CollectionName, subject: Entity?, predicate: Predicate?, range: Range<*>, context: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }
}

