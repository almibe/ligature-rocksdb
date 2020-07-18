/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.rocksdb

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

import dev.ligature.Ligature
import dev.ligature.ReadTx
import dev.ligature.WriteTx
import java.util.concurrent.locks.ReentrantLock

import org.rocksdb.{RocksDB, TransactionDB}

sealed trait StorageType
case class DirectoryStorage(path: Path) extends StorageType
case object InMemoryStorage extends StorageType

class RocksDBStore(val storage: StorageType) extends LigatureStore {
    private val lock = new ReentrantLock()
    private val db: TransactionDB = open(storage)
    private val open = new AtomicBoolean(true)

    private def open(storageType: StorageType): RocksDB = {
        match (storageType) {
            ds: DirectoryStorage => {
                ???
            }
            ms: InMemoryStorage => {
                ???
            }
        }
    }

    override def close(): Unit = {
        db.close()
        open.set(false)
    }

    override def isOpen(): Boolean = open.get()

    override def readTx(): ReadTx = RocksDBReadTx(store)

    override def writeTx(): WriteTx = RocksDBWriteTx(store)
}
