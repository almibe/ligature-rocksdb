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

import cats.effect.{IO, Resource}
import org.rocksdb.{RocksDB, TransactionDB}

class LigatureRocksDB(val path: Path) extends Ligature {


    override def compute: Resource[IO, ReadTx] = ???

    override def write: Resource[IO, WriteTx] = ???

    override def close(): Unit = ???

    override def isOpen: Boolean = ???
}
