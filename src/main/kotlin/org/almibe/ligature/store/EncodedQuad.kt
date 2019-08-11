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
        val result0 = fourth.compareTo(other.fourth)
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
        return CompoundByteIterable(arrayOf(
                longToEntry(fourth),
                longToEntry(first),
                longToEntry(second),
                longToEntry(third)
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
