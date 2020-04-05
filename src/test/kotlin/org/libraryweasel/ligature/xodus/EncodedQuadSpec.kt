/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlin.random.Random

class EncodedQuadSpec: StringSpec() {
    init {
        "test encoding and decoding a quad" {
            val encodedQuad = EncodedQuad(0, 1, 2, 3)
            val byteIterable = encodedQuad.toByteIterable()
            val encodedQuad2 = EncodedQuad.fromByteIterable(byteIterable)
            encodedQuad shouldBe encodedQuad2
        }

        "test random quads" {
            for (i in 0..100) {
                val encodedQuad = EncodedQuad(Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt())
                val byteIterable = encodedQuad.toByteIterable()
                val encodedQuad2 = EncodedQuad.fromByteIterable(byteIterable)
                encodedQuad shouldBe encodedQuad2
            }
        }
    }
}
