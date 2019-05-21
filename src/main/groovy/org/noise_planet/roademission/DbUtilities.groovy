package org.noise_planet.roademission

import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.BatchingStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.h2gis.functions.io.shp.SHPWrite
import org.h2gis.functions.spatial.convert.ST_Force3D
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.LineString
import org.locationtech.jts.geom.Polygon
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.JTSUtility
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

class DbUtilities {


    private static String getDataBasePath(String dbName) {
        return dbName.startsWith("file:/") ? (new File(URI.create(dbName))).getAbsolutePath() : (new File(dbName)).getAbsolutePath()
    }


    static Connection createSpatialDataBase(String dbName, boolean initSpatial) throws SQLException, ClassNotFoundException {
        String dbFilePath = getDataBasePath(dbName);
        File dbFile = new File(dbFilePath + ".mv.db")

        String databasePath = "jdbc:h2:" + dbFilePath + ";LOCK_MODE=0;LOG=0;DB_CLOSE_DELAY=5"

        if (dbFile.exists()) {
            dbFile.delete()
        }

        dbFile = new File(dbFilePath + ".mv.db")
        if (dbFile.exists()) {
            dbFile.delete()
        }
        Driver.load()
        Connection connection = DriverManager.getConnection(databasePath, "sa", "sa")
        if (initSpatial) {
            H2GISFunctions.load(connection)
        }

        return connection
    }

}
