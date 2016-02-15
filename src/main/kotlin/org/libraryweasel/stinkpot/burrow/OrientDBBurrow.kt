/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.stinkpot.burrow


import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.libraryweasel.database.api.DatabasePool
import org.libraryweasel.servo.Component
import org.libraryweasel.servo.Service
import org.libraryweasel.stinkpot.burrow.api.Burrow
import org.libraryweasel.stinkpot.ntriples.IRI


@Component(Burrow::class)
class OrientDBBurrow : Burrow {

    @Service
    private var databasePool: DatabasePool? = null

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
        val subject = triple.subject
        val `object` = triple.`object`
        val predicate = triple.predicate

        if (subject is IRI && `object` is IRI && predicate is IRI) {
            val vS = graphdb.addVertex("class:IRI", "value", subject.value)
            val vO = graphdb.addVertex("class:IRI", "value", `object`.value)
            val eP = graphdb.addEdge("class:PredicateIRI", vS, vO, null)
            eP.setProperty("value", predicate.value)
        }
        graphdb.commit()
        graphdb.shutdown()
    }
}
