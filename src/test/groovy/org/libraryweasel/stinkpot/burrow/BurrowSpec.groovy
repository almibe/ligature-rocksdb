/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.stinkpot.burrow

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.libraryweasel.database.api.DatabasePool
import org.libraryweasel.stinkpot.ntriples.*
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

@Stepwise
public class BurrowSpec extends Specification {

    @Shared
    def burrow = new OrientDBBurrow()
    @Shared
    OrientGraphFactory graphFactory
    @Shared
    OrientGraph testGraph

    def setupSpec() {
        graphFactory = new OrientGraphFactory('memory:test')
        burrow.databasePool = new DatabasePool() {
            @Override OrientGraph acquire() { return graphFactory.getTx() }
            @Override ODatabaseDocumentTx acquireRawDocument() { return null }
            @Override OrientGraphNoTx acquireNoTx() { return null }
        }
    }

    def cleanupSpec() {
        graphFactory.close()
    }

    def setup() {
        testGraph = graphFactory.getTx()
    }

    def cleanup() {
        testGraph.shutdown()
    }

    def "check saving a simple triple made of three IRIs"() {
        given:
        def triple = new Triple(new IRI("http://example.org/#spiderman"),
            new IRI("http://www.perceive.net/schemas/relationship/enemyOf"), new IRI("http://example.org/#green-goblin"))
        when:
        burrow.saveTriple(triple)
        then:
        testGraph.getVerticesOfClass("IRI").size() == 2
        testGraph.getEdgesOfClass("PredicateIRI").size() == 1
    }

    def "support sharing vertices if IRI is the same"() {
        given:
        def triple = new Triple(new IRI("http://example.org/#spiderman"),
                new IRI("http://www.perceive.net/schemas/relationship/enemyOf"), new IRI("http://example.org/#black-cat"))
        when:
        burrow.saveTriple(triple)
        then:
        testGraph.getVerticesOfClass("IRI").size() == 3
        testGraph.getEdgesOfClass("PredicateIRI").size() == 2
    }

    def "support literals in object"() {
        given:
        def triples = []
        triples.add(new Triple(new IRI("http://example.org/show/218"), new IRI("http://www.w3.org/2000/01/rdf-schema#label"), new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string"))))
        triples.add(new Triple(new IRI("http://example.org/show/218"), new IRI("http://www.w3.org/2000/01/rdf-schema#label"), new PlainLiteral("That Seventies Show")))
        triples.add(new Triple(new IRI("http://example.org/show/218"), new IRI("http://example.org/show/localName"), new LangLiteral("That Seventies Show", "en")))
        when:
        triples.each { burrow.saveTriple(it) }
        then:
        testGraph.getVerticesOfClass("IRI").size() == 4
        testGraph.getVerticesOfClass("Literal").size() == 3
        testGraph.getVerticesOfClass("LangLiteral").size() == 1
        testGraph.getVerticesOfClass("PlainLiteral").size() == 1
        testGraph.getVerticesOfClass("TypedLiteral").size() == 1
    }

    def "support sharing vertices if literal is the same"() {
        given:
        def triples = []
        triples.add(new Triple(new IRI("http://otherexample.org/show/0xDEADBEEF"), new IRI("http://www.w3.org/2000/01/rdf-schema#label"), new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string"))))
        triples.add(new Triple(new IRI("http://otherexample.org/show/0xDEADBEEF"), new IRI("http://www.w3.org/2000/01/rdf-schema#label"), new PlainLiteral("That Seventies Show")))
        triples.add(new Triple(new IRI("http://otherexample.org/show/0xDEADBEEF"), new IRI("http://example.org/show/localName"), new LangLiteral("That Seventies Show", "en")))
        when:
        triples.each { burrow.saveTriple(it) }
        then:
        testGraph.getVerticesOfClass("IRI").size() == 5
        testGraph.getVerticesOfClass("Literal").size() == 3
        testGraph.getVerticesOfClass("LangLiteral").size() == 1
        testGraph.getVerticesOfClass("PlainLiteral").size() == 1
        testGraph.getVerticesOfClass("TypedLiteral").size() == 1
    }

//    def "support blank nodes"() {
//
//    }
}
