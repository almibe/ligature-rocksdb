/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.stinkpot.burrow

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.libraryweasel.database.api.DatabasePool
import org.libraryweasel.stinkpot.ntriples.IRI
import org.libraryweasel.stinkpot.ntriples.Triple
import spock.lang.Specification

public class BurrowSpec extends Specification {

    def burrow = new OrientDBBurrow()
    OrientGraph inMemoryGraph

    def setup() {
        inMemoryGraph = new OrientGraph('memory:test')
        burrow.databasePool = new DatabasePool() {
            @Override OrientGraph acquire() { return inMemoryGraph }
            @Override ODatabaseDocumentTx acquireRawDocument() { return null }
            @Override OrientGraphNoTx acquireNoTx() { return null }
        }
    }

    def cleanup() {
        inMemoryGraph.drop()
    }

    def "check saving a simple triple made of three IRIs"() {
        given:
        def triple = new Triple(new IRI("http://example.org/#spiderman"),
            new IRI("http://www.perceive.net/schemas/relationship/enemyOf"), new IRI("http://example.org/#green-goblin"))
        when:
        burrow.saveTriple(triple)
        then:
        inMemoryGraph.getVerticesOfClass("IRI").size() == 3
    }
}
