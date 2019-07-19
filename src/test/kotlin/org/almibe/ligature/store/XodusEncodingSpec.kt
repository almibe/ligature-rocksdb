/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import jetbrains.exodus.bindings.StringBinding
import org.almibe.ligature.*
import java.lang.RuntimeException

class XodusEncodingSpec: StringSpec() {
    init {
        "test graph encoding/decoding" {
            val defaultGraph = DefaultGraph
            val namedGraph = NamedGraph(IRI("http://test"))

            defaultGraph shouldBe decodeGraph(StringBinding.entryToString(encodeGraph(defaultGraph)))
            namedGraph shouldBe decodeGraph(StringBinding.entryToString(encodeGraph(namedGraph)))
        }

        "test subject encoding/decoding" {
            val iri = IRI("http://test")
            val blankNode = BlankNode("blank")
            val exception = object: Subject {}

            iri shouldBe decodeSubject(StringBinding.entryToString(encodeSubject(iri)))
            blankNode shouldBe decodeSubject(StringBinding.entryToString(encodeSubject(blankNode)))
            shouldThrow<RuntimeException> {
                encodeSubject(exception)
            }
            shouldThrow<RuntimeException> {
                decodeSubject("throw")
            }
        }

        "test predicate encoding/decoding" {
            val iri = IRI("http://test")
            val exception = object: Predicate {}

            iri shouldBe decodePredicate(StringBinding.entryToString(encodePredicate(iri)))
            shouldThrow<RuntimeException> {
                encodePredicate(exception)
            }
            shouldThrow<RuntimeException> {
                decodePredicate("throw")
            }
        }

        "test lang literal encoding/decoding" {
            val langLiteral = LangLiteral("test", "en")
            val langLiteral2 = LangLiteral("test test@", "en-0a9")
            val exception = LangLiteral("test test", "56")

            langLiteral shouldBe decodeLangLiteral(StringBinding.entryToString(encodeLangLiteral(langLiteral)))
            langLiteral2 shouldBe decodeLangLiteral(StringBinding.entryToString(encodeLangLiteral(langLiteral2)))
            decodeLangLiteral(StringBinding.entryToString(encodeLangLiteral(exception))) shouldBe null
        }

        "test typed literal encoding/decoding" {
            val typedLiteral = TypedLiteral("5", IRI("http://test"))
            val typedLiteral2 = TypedLiteral("5 ex^^ re^^", IRI("http://test"))

            typedLiteral shouldBe
                    decodeTypedLiteral(
                            StringBinding.entryToString(
                                    encodeTypedLiteral(typedLiteral, 5)), IRI("http://test"))
            typedLiteral2 shouldBe
                    decodeTypedLiteral(
                            StringBinding.entryToString(
                                    encodeTypedLiteral(typedLiteral2, 5)), IRI("http://test"))
        }
    }
}
