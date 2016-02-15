/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.stinkpot.burrow


import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.libraryweasel.database.api.DatabasePool
import org.libraryweasel.servo.Component
import org.libraryweasel.servo.Service
import org.libraryweasel.stinkpot.burrow.api.Burrow
import org.libraryweasel.stinkpot.ntriples.IRI
import org.libraryweasel.stinkpot.ntriples.Object
import org.libraryweasel.stinkpot.ntriples.Predicate
import org.libraryweasel.stinkpot.ntriples.Subject

@Component(Burrow::class)
class OrientDBBurrow : Burrow {

    @Service
    private var databasePool: DatabasePool? = null

    //TODO put index on value for all types
    fun checkSchema(graphdb: OrientGraph) {
        if (graphdb.getEdgeType("PredicateIRI") == null) {
            graphdb.createEdgeType("PredicateIRI")
        }
        if (graphdb.getVertexType("IRI") == null) {
            graphdb.createVertexType("IRI")
        }
    }

    override fun saveTriple(triple: org.libraryweasel.stinkpot.ntriples.Triple) {
        val graphdb = databasePool!!.acquire()
        checkSchema(graphdb)

        val subjectVertex = fetchOrAddSubject(triple.subject, graphdb)
        val objectVertex = fetchOrAddObject(triple.`object`, graphdb)
        addPredicate(triple.predicate, subjectVertex, objectVertex, graphdb)

        graphdb.commit()
        graphdb.shutdown()
    }

    fun fetchOrAddSubject(subject: Subject, graphdb: OrientGraph): Vertex {
        if (subject is IRI) {
            val result = graphdb.query().has("@class", "IRI").has("value", subject.value).vertices()
            if (result.iterator().hasNext()) {
                return result.first()
            } else {
                return graphdb.addVertex("class:IRI", "value", subject.value)
            }
        } else {
            throw RuntimeException("can't handle non IRI subjects")
        }
    }

    fun fetchOrAddObject(`object`: Object, graphdb: OrientGraph): Vertex {
        if (`object` is IRI) {
            val result = graphdb.query().has("@class", "IRI").has("value", `object`.value).vertices()
            if (result.iterator().hasNext()) {
                return result.first()
            } else {
                return graphdb.addVertex("class:IRI", "value", `object`.value)
            }
        } else {
            throw RuntimeException("can't handle non IRI objects")
        }
    }

    fun addPredicate(predicate: Predicate, subjectVertx: Vertex, objectVertex: Vertex,
             graphdb: OrientGraph) {
        if (predicate is IRI) {
            val eP = graphdb.addEdge("class:PredicateIRI", subjectVertx, objectVertex, null)
            eP.setProperty("value", predicate.value)
        } else {
            throw RuntimeException("can't handle non IRI predicates")
        }
    }
}
