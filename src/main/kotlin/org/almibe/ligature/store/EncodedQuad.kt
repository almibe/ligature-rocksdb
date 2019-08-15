/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.bindings.IntegerBinding.entryToInt
import jetbrains.exodus.bindings.IntegerBinding.intToEntry

internal data class EncodedQuad(val first: Int, val second: Int, val third: Int, val fourth: Int): Comparable<EncodedQuad> {
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
                intToEntry(first),
                intToEntry(second),
                intToEntry(third),
                intToEntry(fourth)
        ))
    }

    companion object {
        fun fromByteIterable(byteIterable: ByteIterable): EncodedQuad {
            val offset = byteIterable.length/4
            return EncodedQuad(
                    entryToInt(byteIterable.subIterable(0, offset)),
                    entryToInt(byteIterable.subIterable(offset, offset*2)),
                    entryToInt(byteIterable.subIterable(offset*2, offset*3)),
                    entryToInt(byteIterable.subIterable(offset*3, offset*4))
            )
        }
    }
}
