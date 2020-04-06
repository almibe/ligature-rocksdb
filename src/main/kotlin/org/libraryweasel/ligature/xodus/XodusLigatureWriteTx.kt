/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.xodus

import jetbrains.exodus.env.Environment
import org.libraryweasel.ligature.CollectionName
import org.libraryweasel.ligature.Entity
import org.libraryweasel.ligature.Statement
import org.libraryweasel.ligature.WriteTx
import java.lang.RuntimeException

internal class XodusLigatureWriteTx(private val environment: Environment): WriteTx {
//    override fun deleteCollection(name: Entity) {
//        if (environment.isOpen) {
//            XodusDataset.delete(name, environment)
//        } else {
//            throw RuntimeException("Store is closed.")
//        }
//    }
//
//    override fun collection(name: String): LigatureCollection {
//        return if (environment.isOpen) {
//            XodusDataset.createOrOpen(name, environment)
//        } else {
//            throw RuntimeException("Store is closed.")
//        }
//    }

    override fun addStatement(collection: CollectionName, statement: Statement) {
        TODO("Not yet implemented")
    }

    override fun cancel() {
        TODO("Not yet implemented")
    }

    override fun commit() {
        TODO("Not yet implemented")
    }

    override fun createCollection(collection: CollectionName) {
        TODO("Not yet implemented")
    }

    override fun deleteCollection(collection: CollectionName) {
        TODO("Not yet implemented")
    }

    override fun isOpen(): Boolean {
        TODO("Not yet implemented")
    }

    override fun newEntity(collection: CollectionName): Entity {
        TODO("Not yet implemented")
    }

    override fun removeStatement(collection: CollectionName, statement: Statement) {
        TODO("Not yet implemented")
    }

}

