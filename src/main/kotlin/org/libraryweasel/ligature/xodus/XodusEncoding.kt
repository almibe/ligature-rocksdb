/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.StringBinding
import org.libraryweasel.ligature.Entity
import org.libraryweasel.ligature.LangLiteral
import org.libraryweasel.ligature.Predicate
import java.lang.RuntimeException

fun encodeContext(context: Entity): ByteIterable {
    return when (graph) {
        is DefaultGraph -> StringBinding.stringToEntry("")
        is NamedGraph -> StringBinding.stringToEntry("<${graph.iri.value}>")
    }
}

fun encodeSubject(subject: Entity): ByteIterable {
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

fun encodeLangLiteral(literal: LangLiteral): ByteIterable {
    return StringBinding.stringToEntry("${literal.value}@${literal.langTag}")
}

fun encodeTypedLiteral(literal: TypedLiteral, id: Long): ByteIterable {
    return StringBinding.stringToEntry("${literal.value}^^$id")
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

fun decodeLangLiteral(literalString: String): LangLiteral? {
    val regex = "^(.+)@([a-zA-Z]+(\\-[a-zA-Z0-9]+)*)$".toRegex()
    return when {
        literalString.matches(regex) -> {
            val res = regex.find(literalString)
            if (res == null || res.groupValues.count() < 3) return null
            return LangLiteral(res.groupValues[1], res.groupValues[2])
        }
        else -> null
    }
}

fun decodeTypedLiteral(literalString: String, typeIri: IRI): TypedLiteral? {
    val regex = "^(.+)\\^\\^[0-9]+$".toRegex()
    return when {
        literalString.matches(regex) -> {
            val res = regex.find(literalString)
            if (res == null || res.groupValues.count() < 2) return null
            return TypedLiteral(res.groupValues[1], typeIri)
        }
        else -> null
    }
}
