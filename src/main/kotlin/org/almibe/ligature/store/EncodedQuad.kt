/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.ByteIterable

internal data class EncodedQuad(val graph: Long, val first: Long, val second: Long, val third: Long): Comparable<EncodedQuad> {
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

    fun toByteIterable(): ByteIterable {
        TODO()
    }

    companion object {
        fun fromByteIterable(byteIterable: ByteIterable): EncodedQuad {
            TODO()
        }
    }
}
