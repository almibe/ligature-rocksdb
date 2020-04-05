/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import org.almibe.ligature.*
import java.util.stream.Collectors
import kotlin.streams.toList

class LigatureStoreSpec: StringSpec() {
    init {
        "store should start with no datasets" {
            val store = XodusLigatureStore.open(InMemoryStorage)
            store.getDatasetNames().count() shouldBe 0
        }

        "test creating a dataset" {
            val store = XodusLigatureStore.open(InMemoryStorage)
            val ds = store.getDataset("test")
            ds.getDatasetName() shouldBe "test"
            store.getDatasetNames().count() shouldBe 1
        }

        "test deleting a dataset that doesn't exist" {
            val store = XodusLigatureStore.open(InMemoryStorage)
            store.deleteDataset("test")
            store.getDatasetNames().count() shouldBe 0
        }

        "test creating and removing dataset" {
            val store = XodusLigatureStore.open(InMemoryStorage)
            val ds = store.getDataset("test")
            ds.getDatasetName() shouldBe "test"
            store.getDatasetNames().count() shouldBe 1
            store.deleteDataset("test")
            store.getDatasetNames().count() shouldBe 0
        }

        "add a statement and test its existence" {
            val store = XodusLigatureStore.open(InMemoryStorage)
            val testDataSet = store.getDataset("test")
            testDataSet.addStatements(listOf(Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"))))

            testDataSet.allStatements().count() shouldBe 1

            testDataSet.allStatements().toList().first() shouldBe Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"))
        }

        "add a statement twice and test its existence" {
            val store = XodusLigatureStore.open(InMemoryStorage)
            val testDataSet = store.getDataset("test")
            testDataSet.addStatements(listOf(Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"))))

            testDataSet.addStatements(listOf(Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"))))

            testDataSet.allStatements().count() shouldBe 1

            testDataSet.allStatements().toList().first() shouldBe Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"))
        }


        "add a statement twice in two different graphs and test existence" {
            val store = XodusLigatureStore.open(InMemoryStorage)
            val testDataSet = store.getDataset("test")
            testDataSet.addStatements(listOf(Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"),
                    NamedGraph(IRI("http://hello")))))

            testDataSet.addStatements(listOf(Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"))))

            testDataSet.allStatements().count() shouldBe 2

            testDataSet.allStatements().toList() shouldBe listOf(Quad(IRI("http://localhost/people/7"),
                    IRI("http://localhost/people#name"),
                    TypedLiteral("Alex"),
                    NamedGraph(IRI("http://hello"))),

                    Quad(IRI("http://localhost/people/7"),
                            IRI("http://localhost/people#name"),
                            TypedLiteral("Alex")))
        }


//        def "check saving a simple triple made of three IRIs"() {
//            when:
//            store.addStatement(new IRI("http://example.org/#spiderman"),
//                    new IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
//                    new IRI("http://example.org/#green-goblin"))
//            then:
//            store.IRIs.size() == 3
//            store.statementsFor(new IRI("http://example.org/#spiderman")).first() ==
//                    new Pair(new IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
//            new IRI("http://example.org/#green-goblin"))
//        }
//
//        def "support sharing vertices if IRI is the same"() {
//            when:
//            store.addStatement(new IRI("http://example.org/#spiderman"),
//                    new IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
//                    new IRI("http://example.org/#black-cat"))
//            then:
//            store.IRIs.size() == 4
//            store.statementsFor(new IRI("http://example.org/#spiderman")).size() == 2
//        }
//
//        def "support literals in object"() {
//            when:
//            store.addStatement(new IRI("http://example.org/show/218"),
//                    new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
//                    new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string")))
//            store.addStatement(new IRI("http://example.org/show/218"),
//                    new IRI("http://example.org/show/localName"),
//                    new LangLiteral("That Seventies Show", "en"))
//            then:
//            store.IRIs.size() == 8
//            store.statementsFor(new IRI("http://example.org/show/218")) == [
//                new Pair(new IRI("http://example.org/show/localName"),
//                new LangLiteral("That Seventies Show", "en")),
//            new Pair(new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
//            new TypedLiteral("That Seventies Show", new IRI("http://www.w3.org/2001/XMLSchema#string")))
//            ].toSet()
//        }
//
//        def "support blank nodes"() {
//            given:
//            def test = new BlankNode("test")
//            def test2 = new BlankNode("test2")
//            def test3 = new BlankNode("test3")
//            when:
//            store.addStatement(test, new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
//                    test2)
//            store.addStatement(test3, new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
//                    new LangLiteral("Test 3", "en"))
//            then:
//            store.statementsFor(test) == [new Pair(new IRI("http://www.w3.org/2000/01/rdf-schema#label"), test2)].toSet()
//            store.statementsFor(test2) == [].toSet()
//            store.statementsFor(test3) == [
//                new Pair(new IRI("http://www.w3.org/2000/01/rdf-schema#label"),
//                new LangLiteral("Test 3", "en"))].toSet()
//        }
//
//        def "support remove statement"() {
//            when:
//            store.removeStatement(new IRI("http://example.org/#spiderman"),
//                    new IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
//                    new IRI("http://example.org/#green-goblin"))
//            then:
//            store.IRIs.size() == 8
//        }
//
////    def "support remove subject"() {
////
////    }
////
////    def "support add model"() {
////
////    }
    }
}
