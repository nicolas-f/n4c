package org.noise_planet.roademission

import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.utilities.SFSUtilities
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.RootProgressVisitor
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
        String dataFolder = "data/"
        String workingDir = ""
        if (args.length > 0) {
            workingDir = args[0]
        }

        // Init output logger
        Logger logger = LoggerFactory.getLogger(Main.class)
        logger.info(String.format("Working directory is %s", new File(workingDir).getAbsolutePath()))


        // Create spatial database
        //TimeZone tz = TimeZone.getTimeZone("UTC")
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmss", Locale.getDefault())

        //df.setTimeZone(tz)
        String dbName = new File(workingDir + df.format(new Date())).toURI()
        Connection connection = SFSUtilities.wrapConnection(DbUtilities.createSpatialDataBase(dbName, true))
        Sql sql = new Sql(connection)

        // Evaluate receiver points using provided buildings

        sql.execute("DROP TABLE IF EXISTS BUILDINGS")

        logger.info("Read building file")
        SHPRead.readShape(connection, dataFolder+"Buildings.shp", "BUILDINGS")
        sql.execute("CREATE SPATIAL INDEX ON BUILDINGS(THE_GEOM)")
        logger.info("Building file loaded")

        // Load receivers points (evaluation points)
        SHPRead.readShape(connection, dataFolder+"Receivers.shp", "RECEIVERS")
        sql.execute("CREATE SPATIAL INDEX ON RECEIVERS(THE_GEOM)")

        // Load sound sources
        logger.info("Read sound sources")
        SHPRead.readShape(connection, dataFolder+"Sound_source.shp", "SOURCE")
        sql.execute("CREATE SPATIAL INDEX ON SOURCE(THE_GEOM)")
        logger.info("Source file loaded")

        // Init NoiseModelling
        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS", "SOURCE", "RECEIVERS")

        pointNoiseMap.setMaximumPropagationDistance(750.0d)
        pointNoiseMap.soundReflectionOrder = 2
        pointNoiseMap.computeHorizontalDiffraction = true
        pointNoiseMap.computeVerticalDiffraction = true
        pointNoiseMap.setHeightField("HEIGHT")
        pointNoiseMap.setThreadCount(1) // Single thread
        RootProgressVisitor progressLogger = new RootProgressVisitor(1, false, 1)
        pointNoiseMap.initialize(connection, new EmptyProgressVisitor())


        // Set of already processed receivers
        Set<Long> receivers = new HashSet<>()
        ProgressVisitor progressVisitor = progressLogger.subProcess(pointNoiseMap.getGridDim()*pointNoiseMap.getGridDim())

        // Processing are subdivided in sub domains
        // Process all sub domains

        logger.info("Receiver\tSource\tLAeq")
        for (int i = 0; i < pointNoiseMap.getGridDim(); i++) {
            for (int j = 0; j < pointNoiseMap.getGridDim(); j++) {
                ComputeRaysOut out = (ComputeRaysOut) pointNoiseMap.evaluateCell(connection, i, j, progressVisitor, receivers)
                out.getVerticesSoundLevel().each { receiver ->
                    double globalLevel = ComputeRays.wToDba(ComputeRays.sumArray(ComputeRays.dbaToW(receiver.value)))
                    logger.info(String.format(Locale.ROOT, "%d\t%d\t%.2f dB", receiver.receiverId, receiver.sourceId, globalLevel))
                }
            }
        }

    }
}
