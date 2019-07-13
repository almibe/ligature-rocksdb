/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import org.almibe.ligature.*
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Stream
import kotlin.concurrent.withLock

internal object WriteLock {
    val lock = ReentrantLock()
}

internal class XodusDataset private constructor(private val name: String, private val environment: Environment): Dataset {
    companion object {
        val suffixes = mapOf(
            "cntr" to "#cntr",
            "graphId" to "#gid",
            "idGraph" to "#idg",
            "nodeId" to "#nid",
            "idNode" to "#idn",
            "literalId" to "#lid",
            "idLiteral" to "#idl",
            "spo" to "#spo",
            "sop" to "#sop",
            "pos" to "#pos",
            "pso" to "#pso",
            "osp" to "#osp",
            "ops" to "#ops"
        )

        fun createOrOpen(name: String, environment: Environment): XodusDataset {
            environment.executeInTransaction { txn ->
                suffixes.values.forEach {  suffix ->
                    environment.openStore("$name$suffix", StoreConfig.WITHOUT_DUPLICATES, txn)
                }
            }
            return XodusDataset(name, environment)
        }

        fun delete(name: String, environment: Environment) {
            WriteLock.lock.withLock {
                environment.executeInTransaction {  txn ->
                    suffixes.values.forEach { suffix ->
                        environment.removeStore("$name$suffix", txn)
                    }
                }
            }
        }
    }

    private data class EncodedQuad(val graph: Long, val first: Long, val second: Long, val third: Long): Comparable<EncodedQuad> {
        override fun compareTo(other: EncodedQuad): Int {
            val result0 = graph.compareTo(other.graph)
            if (result0 != 0) {
                return result0
            }
            val result1 = first.compareTo(other.first)
            if (result1 != 0) {
                return result1
            }
            val result2 = second.compareTo(other.second)
            if (result2 != 0) {
                return result2
            }
            return third.compareTo(other.third)
        }
    }

    override fun getDatasetName(): String = name

    override fun addStatements(statements: Collection<Quad>) {
        WriteLock.lock.withLock {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }

    override fun executeSparql(sparql: String): Stream<List<SparqlResultField>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun findAll(subject: Subject?, predicate: Predicate?, `object`: Object?, graph: Graph?): Stream<Quad> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun removeStatements(statements: Collection<Quad>) {
        WriteLock.lock.withLock {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }
}
