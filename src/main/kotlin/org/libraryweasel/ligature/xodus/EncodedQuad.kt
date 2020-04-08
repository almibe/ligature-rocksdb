/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.bindings.IntegerBinding.entryToInt
import jetbrains.exodus.bindings.IntegerBinding.intToEntry
import jetbrains.exodus.bindings.LongBinding.entryToLong
import jetbrains.exodus.bindings.LongBinding.longToEntry

internal data class EncodedQuad(val prefix: Int, val first: Long, val second: Long, val third: Long, val fourth: Long): Comparable<EncodedQuad> {
    override fun compareTo(other: EncodedQuad): Int {
        val result0 = first.compareTo(other.first)
        if (result0 != 0) {
            return result0
        }
        val result1 = second.compareTo(other.second)
        if (result1 != 0) {
            return result1
        }
        val result2 = third.compareTo(other.third)
        if (result2 != 0) {
            return result2
        }
        return fourth.compareTo(other.fourth)
    }

    fun toByteIterable(): ByteIterable {
        return CompoundByteIterable(arrayOf(
                intToEntry(prefix),
                longToEntry(first),
                longToEntry(second),
                longToEntry(third),
                longToEntry(fourth)
        ))
    }

    companion object {
        fun fromByteIterable(byteIterable: ByteIterable): EncodedQuad {
            return EncodedQuad(
                    entryToInt(byteIterable.subIterable(0, Int.SIZE_BYTES)),
                    entryToLong(byteIterable.subIterable(Int.SIZE_BYTES, Int.SIZE_BYTES + Long.SIZE_BYTES)),
                    entryToLong(byteIterable.subIterable(Int.SIZE_BYTES + Long.SIZE_BYTES, Int.SIZE_BYTES + Long.SIZE_BYTES*2)),
                    entryToLong(byteIterable.subIterable(Int.SIZE_BYTES + Long.SIZE_BYTES*2, Int.SIZE_BYTES + Long.SIZE_BYTES*3)),
                    entryToLong(byteIterable.subIterable(Int.SIZE_BYTES + Long.SIZE_BYTES*3, Int.SIZE_BYTES + Long.SIZE_BYTES*4))
            )
        }
    }
}
