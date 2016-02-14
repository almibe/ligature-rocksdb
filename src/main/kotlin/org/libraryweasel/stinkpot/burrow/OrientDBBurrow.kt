/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.stinkpot.burrow


import org.libraryweasel.database.api.DatabasePool
import org.libraryweasel.servo.Component
import org.libraryweasel.servo.Service
import org.libraryweasel.stinkpot.burrow.api.Burrow
import org.libraryweasel.stinkpot.ntriples.Triple

@Component(Burrow::class)
class OrientDBBurrow : Burrow {

    @Service
    private var databasePool: DatabasePool? = null

    override fun saveTriple(triple: Triple) {
        val graphdb = databasePool!!.acquire()

    }
}
