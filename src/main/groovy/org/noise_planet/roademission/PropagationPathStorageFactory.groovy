package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.noise_planet.noisemodelling.propagation.GeoJSONDocument
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPOutputStream

@CompileStatic
class PropagationPathStorageFactory implements PointNoiseMap.IComputeRaysOutFactory {
    ConcurrentLinkedDeque<PointToPointPaths> pathQueue = new ConcurrentLinkedDeque<>()
    GZIPOutputStream gzipOutputStream
    AtomicBoolean waitForMorePaths = new AtomicBoolean(true)
    public static final int GZIP_CACHE_SIZE = (int)Math.pow(2, 19)
    String workingDir

    void openPathOutputFile(String path) {
        gzipOutputStream = new GZIPOutputStream(new FileOutputStream(path), GZIP_CACHE_SIZE)
        new Thread(new WriteThread(pathQueue, waitForMorePaths, gzipOutputStream)).start()
    }

    void setWorkingDir(String workingDir) {
        this.workingDir = workingDir
    }

    @Override
    IComputeRaysOut create(PropagationProcessData propagationProcessData, PropagationProcessPathData propagationProcessPathData) {
        return new PropagationPathStorage(propagationProcessData, propagationProcessPathData, pathQueue)
    }

    void closeWriteThread() {
        waitForMorePaths.set(false)
    }

    /**
     * Write paths on disk using a single thread
     */
    static class WriteThread implements Runnable {
        ConcurrentLinkedDeque<PointToPointPaths> pathQueue
        AtomicBoolean waitForMorePaths
        GZIPOutputStream gzipOutputStream

        WriteThread(ConcurrentLinkedDeque<PointToPointPaths> pathQueue, AtomicBoolean waitForMorePaths, GZIPOutputStream gzipOutputStream) {
            this.pathQueue = pathQueue
            this.waitForMorePaths = waitForMorePaths
            this.gzipOutputStream = gzipOutputStream
        }

        @Override
        void run() {
            DataOutputStream dataOutputStream = new DataOutputStream(gzipOutputStream)
            while (waitForMorePaths.get()) {
                while(!pathQueue.isEmpty()) {
                    PointToPointPaths paths = pathQueue.pop()
                    paths.writePropagationPathListStream(dataOutputStream)
                }
                Thread.sleep(10)
            }
            dataOutputStream.flush()
            gzipOutputStream.close()
        }
    }
}
