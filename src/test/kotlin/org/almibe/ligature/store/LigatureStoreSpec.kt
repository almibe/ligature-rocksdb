/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.PersistentEntityStores
import org.almibe.ligature.*
import kotlin.Pair
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

class LigatureStoreSpec: StringSpec() {
    init {
        "add statement and test it's existence" {
            val store = InMemoryStore()
            val testDataSet = store.getDataset("test")
            testDataSet.addStatements(listOf(Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"))))

            testDataSet.findAll().count() shouldBe 1

            testDataSet.findAll().toList().first() shouldBe Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"))
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
            store.statementsFor(new IRI("http://example.org/show/218")) == [
                new Pair(new IRI("http://example.org/show/localName"),
                new LangLiteral("That Seventies Show", "en")),
            new Pair(new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
            new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string")))
            ].toSet()
        }

        def "support blank nodes"() {
            given:
            def test = new BlankNode("test")
            def test2 = new BlankNode("test2")
            def test3 = new BlankNode("test3")
            when:
            store.addStatement(test, new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
                    test2)
            store.addStatement(test3, new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
                    new LangLiteral("Test 3", "en"))
            then:
            store.statementsFor(test) == [new Pair(new IRI("http://www.w3.org/2000/01/rdf-schema#label"), test2)].toSet()
            store.statementsFor(test2) == [].toSet()
            store.statementsFor(test3) == [
                new Pair(new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
                new LangLiteral("Test 3", "en"))].toSet()
        }

        def "support remove statement"() {
            when:
            store.removeStatement(new IRI("http://example.org/#spiderman"),
                    new IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
                    new IRI("http://example.org/#green-goblin"))
            then:
            store.IRIs.size() == 8
        }

//    def "support remove subject"() {
//
//    }
//
//    def "support add model"() {
//
//    }
    }
}
