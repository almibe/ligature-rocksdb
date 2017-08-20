/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import org.almibe.ligature.IRI
import org.almibe.ligature.LangLiteral
import org.almibe.ligature.TypedLiteral
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

@Stepwise
class LigatureStoreSpec extends Specification {
    @Shared
    OrientDB orientDB = new OrientDB("memory:test","root","root_passwd",OrientDBConfig.defaultConfig())
    @Shared
    ODatabasePool pool = new ODatabasePool(orientDB,"test","admin","admin")
    @Shared
    def store = new OrientDBLigatureStore(pool)

    def setupSpec() {

    }

    def cleanupSpec() {
        pool.close()
        orientDB.close()
    }

    def "check saving a simple triple made of three IRIs"() {
        when:
        store.addStatement(new IRI("http://example.org/#spiderman"),
                new IRI("http://www.perceive.net/schemas/relationship/enemyOf"), new IRI("http://example.org/#green-goblin"))
        then:
        testGraph.getVerticesOfClass("IRI").size() == 2
        testGraph.getEdgesOfClass("PredicateIRI").size() == 1
    }

    def "support sharing vertices if IRI is the same"() {
        when:
        store.addStatement(new IRI("http://example.org/#spiderman"),
                new IRI("http://www.perceive.net/schemas/relationship/enemyOf"), new IRI("http://example.org/#black-cat"))
        then:
        testGraph.getVerticesOfClass("IRI").size() == 3
        testGraph.getEdgesOfClass("PredicateIRI").size() == 2
    }

    def "support literals in object"() {
        when:
        store.addStatement(new IRI("http://example.org/show/218"), new IRI("http://www.w3.org/2000/01/rdf-schema#label"), new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string")))
        store.addStatement(new IRI("http://example.org/show/218"), new IRI("http://example.org/show/localName"), new LangLiteral("That Seventies Show", "en"))
        then:
        testGraph.getVerticesOfClass("IRI").size() == 4
        testGraph.getVerticesOfClass("Literal").size() == 3
        testGraph.getVerticesOfClass("LangLiteral").size() == 1
        testGraph.getVerticesOfClass("PlainLiteral").size() == 1
        testGraph.getVerticesOfClass("TypedLiteral").size() == 1
    }

//    def "support blank nodes"() {
//
//    }
}
