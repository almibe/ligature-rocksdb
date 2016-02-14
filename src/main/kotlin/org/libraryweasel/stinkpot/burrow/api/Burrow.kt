/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.stinkpot.burrow.api

interface Burrow {
    fun saveTriple(triple: org.libraryweasel.stinkpot.ntriples.Triple)
}
