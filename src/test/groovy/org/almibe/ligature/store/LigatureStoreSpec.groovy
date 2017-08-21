/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import kotlin.Pair
import org.almibe.ligature.IRI
import org.almibe.ligature.LangLiteral
import org.almibe.ligature.TypedLiteral
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

@Stepwise
class LigatureStoreSpec extends Specification {
    @Shared
    OrientDB orientDB = new OrientDB("memory:test",OrientDBConfig.defaultConfig())
    @Shared
    ODatabasePool pool
    @Shared
    def store

    def setupSpec() {
        orientDB.create("test", ODatabaseType.MEMORY)
        pool = new ODatabasePool(orientDB,"test","admin","admin")
        store = new OrientDBLigatureStore(pool)
    }

    def cleanupSpec() {
        pool.close()
        orientDB.close()
    }

    def "check saving a simple triple made of three IRIs"() {
        when:
        store.addStatement(new IRI("http://example.org/#spiderman"),
                new IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
                new IRI("http://example.org/#green-goblin"))
        then:
        store.IRIs.size() == 3
        store.statementsFor(new IRI("http://example.org/#spiderman")).first() ==
                new Pair(new IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
                new IRI("http://example.org/#green-goblin"))
    }

    def "support sharing vertices if IRI is the same"() {
        when:
        store.addStatement(new IRI("http://example.org/#spiderman"),
                new IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
                new IRI("http://example.org/#black-cat"))
        then:
        store.IRIs.size() == 4
        store.statementsFor(new IRI("http://example.org/#spiderman")).size() == 2
    }

    def "support literals in object"() {
        when:
        store.addStatement(new IRI("http://example.org/show/218"),
                new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
                new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string")))
        store.addStatement(new IRI("http://example.org/show/218"),
                new IRI("http://example.org/show/localName"),
                new LangLiteral("That Seventies Show", "en"))
        then:
        store.IRIs.size() == 8
        store.literals == [
                new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string")),
                new LangLiteral("That Seventies Show", "en")].toSet()
        store.statementsFor(new IRI("http://example.org/show/218")) == [
                new Pair(new IRI("http://example.org/show/localName"),
                    new LangLiteral("That Seventies Show", "en")),
                new Pair(new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
                    new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string")))
        ].toSet()
    }

//    def "support blank nodes"() {
//
//    }
}
