/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.env.Environment
import org.almibe.ligature.*
import java.lang.RuntimeException
import java.nio.file.Path
import java.util.stream.Stream

sealed class StorageType
data class DirectoryStorage(val path: Path): StorageType()
object InMemoryStorage: StorageType()

class XodusLigatureStore private constructor(private val environment: Environment): Store {

    companion object {
        fun open(storageType: StorageType) {
            when (storageType) {
                is DirectoryStorage -> {
                    TODO()
                }
                is InMemoryStorage -> {
                    TODO()
                }
            }
        }
    }

    override fun close() {
        environment.close()
    }

    override fun deleteDataset(name: String) {
        if (environment.isOpen) {
            XodusDataset.delete(name, environment)
        } else {
            throw RuntimeException("Store is closed.")
        }
    }

    override fun getDataset(name: String): Dataset {
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
}
