/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.ByteIterable
import org.almibe.ligature.Graph
import org.almibe.ligature.Object
import org.almibe.ligature.Predicate
import org.almibe.ligature.Subject

fun encodeGraph(graph: Graph): ByteIterable {
    TODO()
}

fun encodeSubject(subject: Subject): ByteIterable {
    TODO()
}

fun encodePredicate(predicate: Predicate): ByteIterable {
    TODO()
}

fun encodeObject(`object`: Object): ByteIterable {
    TODO()
}

fun isGraph(graphString: String): Graph? {
    TODO()
}

fun isSubject(subjectString: String): Subject? {
    TODO()
}

fun isPredicate(predicateString: String): Predicate? {
    TODO()
}

fun isObject(objectString: String): Object? {
    TODO()
}
