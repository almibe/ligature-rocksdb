/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.env.Environment
import org.almibe.ligature.SparqlResultField
import java.util.stream.Stream

class SparqlRunner(private val environment: Environment) {
    fun executeSparql(sparql: String): Stream<List<SparqlResultField>> {
        TODO()
    }
}
