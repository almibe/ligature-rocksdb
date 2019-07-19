/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.StringBinding
import org.almibe.ligature.*
import java.lang.RuntimeException

fun encodeGraph(graph: Graph): ByteIterable {
    return when (graph) {
        is DefaultGraph -> StringBinding.stringToEntry("")
        is NamedGraph -> StringBinding.stringToEntry("<${graph.iri.value}>")
    }
}

fun encodeSubject(subject: Subject): ByteIterable {
    return when (subject) {
        is IRI -> StringBinding.stringToEntry("<${subject.value}>")
        is BlankNode -> StringBinding.stringToEntry("_:${subject.label}")
        else -> throw RuntimeException("Unexpected Node (only IRI and BlankNode allowed) $subject")
    }
}

fun encodePredicate(predicate: Predicate): ByteIterable {
    return when (predicate) {
        is IRI -> StringBinding.stringToEntry("<${predicate.value}>")
        else -> throw RuntimeException("Unexpected Predicate (only IRI allowed) $predicate")
    }
}

fun encodeLiteral(literal: Literal): ByteIterable {
    return when (literal) {
        is LangLiteral -> {
            StringBinding.stringToEntry("${literal.value}@${literal.langTag}")
        }
        is TypedLiteral -> {
            StringBinding.stringToEntry("${literal.value}^^<${literal.datatypeIRI.value}>")
        }
    }
}

fun decodeGraph(graphString: String): Graph {
    return if (graphString == "") {
        DefaultGraph
    } else if (graphString.startsWith("<") && graphString.endsWith(">")) {
        val graphIri = IRI(graphString.removePrefix("<").removeSuffix(">"))
        NamedGraph(graphIri)
    } else {
        throw RuntimeException("Invalid Graph - $graphString")
    }
}

fun decodeSubject(subjectString: String): Subject {
    return when {
        subjectString.startsWith("<") && subjectString.endsWith(">") -> {
            IRI(subjectString.removePrefix("<").removeSuffix(">"))
        }
        subjectString.startsWith("_:") -> {
            BlankNode(subjectString.removePrefix("_:"))
        }
        else -> throw RuntimeException("Invalid Node - $subjectString")
    }
}

fun decodePredicate(predicateString: String): Predicate {
    return when {
        predicateString.startsWith("<") && predicateString.endsWith(">") -> {
            IRI(predicateString.removePrefix("<").removeSuffix(">"))
        }
        else -> throw RuntimeException("Invalid Predicate - $predicateString")
    }
}

fun decodeLiteral(literalString: String): Literal {
    return when {
        literalString.matches("^.+@[a-zA-Z]+(\\-[a-zA-Z0-9]+)*]$".toRegex()) -> {
            TODO()
        }
        literalString.matches("^.+\\^\\^[0-9]+$".toRegex()) -> {
            TODO()
        }
        else -> throw RuntimeException("Invalid Literal - $literalString")
    }
}
