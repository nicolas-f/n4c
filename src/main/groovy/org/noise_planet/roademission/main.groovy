package org.noise_planet.roademission

import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.utilities.SFSUtilities
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.text.DateFormat
import java.text.SimpleDateFormat

/**
 * To run
 * Just type "gradlew -Pworkdir=out/"
 */
@CompileStatic
class Main {
    static void main(String[] args) {
        // Read working directory argument
        String workingDir = ""
        if (args.length > 0) {
            workingDir = args[0]
        }

        // Init output logger
        Logger logger = LoggerFactory.getLogger(Main.class)
        logger.info(String.format("Working directory is %s", new File(workingDir).getAbsolutePath()))
        ProgressVisitor progressVisitor = new EmptyProgressVisitor()

        // Create spatial database
        //TimeZone tz = TimeZone.getTimeZone("UTC")
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.getDefault())

        //df.setTimeZone(tz)
        String dbName = new File(workingDir + df.format(new Date())).toURI()
        Connection connection = SFSUtilities.wrapConnection(DbUtilities.createSpatialDataBase(dbName, true))
        Sql sql = new Sql(connection)

        // Evaluate receiver points using provided buildings

        sql.execute("DROP TABLE IF EXISTS BUILDINGS")

        logger.info("Read building file")
        SHPRead.readShape(connection, "data/buildings.shp", "BUILDINGS")
        SHPRead.readShape(connection, "data/study_area.shp", "STUDY_AREA")
        sql.execute("CREATE SPATIAL INDEX ON BUILDINGS(THE_GEOM)")
        logger.info("Building file loaded")

        // Load or create receivers points
        if(!new File("data/receivers.shp").exists()) {
            DbUtilities.createReceiversFromBuildings(sql, "BUILDINGS", "STUDY_AREA")
        } else {
            SHPRead.readShape(connection, "data/receivers.shp", "RECEIVERS")
        }

        // Load roads
        logger.info("Read road geometries and traffic")
        SHPRead.readShape(connection, "data/troncon2012.shp", "ROADS")
        logger.info("Road file loaded")

        // Load ground type
        logger.info("Read ground surface categories")
        SHPRead.readShape(connection, "data/ground_type.shp", "GROUND_TYPE")
        logger.info("Surface categories file loaded")

        // Init NoiseModelling
        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS", "ROADS", "RECEIVERS")
        pointNoiseMap.setSoilTableName("GROUND_TYPE")
        pointNoiseMap.setMaximumPropagationDistance(1200.0d)
        pointNoiseMap.soundReflectionOrder = 2
        pointNoiseMap.computeHorizontalDiffraction = true
        pointNoiseMap.computeVerticalDiffraction = true
        pointNoiseMap.setHeightField("HAUTEUR")
        PropagationPathStorageFactory storageFactory = new PropagationPathStorageFactory()
        pointNoiseMap.setComputeRaysOutFactory(storageFactory)

        try {
            pointNoiseMap.initialize(connection, progressVisitor)
            // Set of already processed receivers
            Set<Long> receivers = new HashSet<>()

            for (int i = 0; i < pointNoiseMap.getGridDim(); i++) {
                for (int j = 0; j < pointNoiseMap.getGridDim(); j++) {
                    IComputeRaysOut out = pointNoiseMap.evaluateCell(connection, i, j, progressVisitor, receivers)
                }
            }
        } finally {
            storageFactory.closeWriteThread()
        }

    }
}
