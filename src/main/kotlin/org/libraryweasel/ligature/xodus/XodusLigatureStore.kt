/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentConfig
import java.nio.file.Path
import jetbrains.exodus.io.inMemory.MemoryDataWriter
import jetbrains.exodus.io.inMemory.MemoryDataReader
import jetbrains.exodus.io.inMemory.Memory
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.env.Environments
import org.libraryweasel.ligature.LigatureStore
import org.libraryweasel.ligature.ReadTx
import org.libraryweasel.ligature.WriteTx
import java.util.concurrent.locks.ReentrantLock

sealed class StorageType
data class DirectoryStorage(val path: Path): StorageType()
object InMemoryStorage: StorageType()

class XodusLigatureStore private constructor(private val environment: Environment): LigatureStore {
    private val lock = ReentrantLock()

    companion object {
        fun open(storageType: StorageType): LigatureStore {
            return when (storageType) {
                is DirectoryStorage -> {
                    XodusLigatureStore(Environments.newInstance(storageType.path.toFile()))
                }
                is InMemoryStorage -> {
                    XodusLigatureStore(createInMemoryEnvironment())
                }
            }
        }

        private fun createInMemoryEnvironment(): Environment {
            val memory = Memory()
            return Environments.newInstance(
                    LogConfig.create(MemoryDataReader(memory), MemoryDataWriter(memory)),
                    EnvironmentConfig().setGcUtilizationFromScratch(true)
            )
        }
    }

    override suspend fun close() {
        environment.close()
    }

    override suspend fun isOpen(): Boolean = environment.isOpen

    override suspend fun readTx(): ReadTx = XodusLigatureReadTx(environment)

    override suspend fun writeTx(): WriteTx = XodusLigatureWriteTx(environment)
}
