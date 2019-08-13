/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.bindings.LongBinding.entryToLong
import jetbrains.exodus.bindings.LongBinding.longToEntry

internal data class EncodedQuad(val first: Long, val second: Long, val third: Long, val fourth: Long): Comparable<EncodedQuad> {
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
                longToEntry(first),
                longToEntry(second),
                longToEntry(third),
                longToEntry(fourth)
        ))
    }

    companion object {
        fun fromByteIterable(byteIterable: ByteIterable): EncodedQuad {
            val offset = byteIterable.length/4
            return EncodedQuad(
                    entryToLong(byteIterable.subIterable(0, offset)),
                    entryToLong(byteIterable.subIterable(offset, offset*2)),
                    entryToLong(byteIterable.subIterable(offset*2, offset*3)),
                    entryToLong(byteIterable.subIterable(offset*3, offset*4))
            )
        }
    }
}
