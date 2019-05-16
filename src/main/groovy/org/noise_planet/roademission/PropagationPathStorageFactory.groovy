package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap

import java.util.concurrent.ConcurrentLinkedDeque

@CompileStatic
class PropagationPathStorageFactory implements PointNoiseMap.IComputeRaysOutFactory {
    ConcurrentLinkedDeque<PointToPointPaths> pathQueue = new ConcurrentLinkedDeque<>()
    private static final int GZIP_CACHE_SIZE = Math.pow(2, 19)

    void openPathOutputFile(String path) {

    }

    @Override
    IComputeRaysOut create(PropagationProcessData propagationProcessData, PropagationProcessPathData propagationProcessPathData) {
        return new PropagationPathStorage(propagationProcessData, pathQueue)
    }

    void closeWriteThread() {

    }
}
