/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.stinkpot.burrow

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.libraryweasel.database.api.DatabasePool
import spock.lang.Specification

public class BurrowSpec extends Specification {

    def burrow = new OrientDBBurrow()
    OrientGraph inMemoryGraph

    def setup() {
        inMemoryGraph = new OrientGraph('memory:test')
        inMemoryGraph.create()
        burrow.databasePool = new DatabasePool() {
            @Override OrientGraph acquire() { return inMemoryGraph }
            @Override ODatabaseDocumentTx acquireRawDocument() { return null }
            @Override OrientGraphNoTx acquireNoTx() { return null }
        }
    }

    def cleanup() {
        inMemoryGraph.drop()
    }


}
