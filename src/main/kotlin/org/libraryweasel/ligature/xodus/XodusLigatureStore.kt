/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentConfig
import java.lang.RuntimeException
import java.nio.file.Path
import java.util.stream.Stream
import jetbrains.exodus.io.inMemory.MemoryDataWriter
import jetbrains.exodus.io.inMemory.MemoryDataReader
import jetbrains.exodus.io.inMemory.Memory
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.TransactionalComputable
import org.libraryweasel.ligature.Entity
import org.libraryweasel.ligature.LigatureCollection
import org.libraryweasel.ligature.LigatureStore

sealed class StorageType
data class DirectoryStorage(val path: Path): StorageType()
object InMemoryStorage: StorageType()

class XodusLigatureStore private constructor(private val environment: Environment): LigatureStore {
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

    override fun close() {
        environment.close()
    }

    override fun deleteCollection(name: Entity) {
        if (environment.isOpen) {
            XodusDataset.delete(name, environment)
        } else {
            throw RuntimeException("Store is closed.")
        }
    }

    override fun collection(name: String): LigatureCollection {
        return if (environment.isOpen) {
            XodusDataset.createOrOpen(name, environment)
        } else {
            throw RuntimeException("Store is closed.")
        }
    }

    override fun getDatasetNames(): Stream<String> {
        return if (environment.isOpen) {
            val names = environment.computeInReadonlyTransaction {
                environment.getAllStoreNames(it)
            }
            names.stream()
                .filter { it.endsWith("#pso") }
                .map { it.removeSuffix("#pso") }
        } else {
            throw RuntimeException("Store is closed.")
        }
    }

    fun <T>computeInReadonlyTransaction(computable: TransactionalComputable<T>): T {
        return if (environment.isOpen) {
            environment.computeInReadonlyTransaction(computable)
        } else {
            throw RuntimeException("Store is closed.")
        }
    }
}
