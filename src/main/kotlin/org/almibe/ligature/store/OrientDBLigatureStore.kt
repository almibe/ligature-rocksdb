/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import com.orientechnologies.orient.core.db.ODatabasePool
import org.almibe.ligature.*

class OrientDBLigatureStore(val databasePool: ODatabasePool): Model {
    override fun addModel(model: ReadOnlyModel) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun addStatement(subject: Subject, predicate: Predicate, `object`: Object) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getIRIs(): Set<IRI> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLiterals(): Set<Literal> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getObjects(): Set<Object> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getPredicates(): Set<Predicate> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSubjects(): Set<Subject> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun statementsFor(subject: Subject): Set<Pair<Predicate, Object>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
