package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.PropagationProcessData

import java.util.concurrent.ConcurrentLinkedDeque

@CompileStatic
class PropagationPathStorage implements IComputeRaysOut {
    // Thread safe queue object
    protected PropagationProcessData inputData
    ConcurrentLinkedDeque<PointToPointPaths> pathQueue

    PropagationPathStorage(PropagationProcessData inputData, ConcurrentLinkedDeque<PointToPointPaths> pathQueue) {
        this.inputData = inputData
        this.pathQueue = pathQueue
    }

    @Override
    double[] addPropagationPaths(long sourceId, double sourceLi, long receiverId, List<PropagationPath> propagationPath) {
        if(inputData != null && sourceId < inputData.sourcesPk.size() &&
                receiverId < inputData.receiversPk.size()) {
            PointToPointPaths paths = new PointToPointPaths()
            paths.li = sourceLi
            paths.receiverId = receiverId
            paths.sourceId = sourceId
            paths.propagationPath = new ArrayList<>(propagationPath.size())
            for (PropagationPath path : propagationPath) {
                // Copy path content in order to keep original ids for other method calls
                PropagationPath pathPk = new PropagationPath(path.isFavorable(), path.getPointList(),
                        path.getSegmentList(), path.getSRList());
                pathPk.setIdReceiver(inputData.receiversPk.get((int) receiverId).intValue())
                pathPk.setIdSource(inputData.sourcesPk.get((int) sourceId).intValue())
                paths.propagationPath.add(pathPk)
            }
            pathQueue.add(paths)
        }
        return new double[0]
    }

    @Override
    void finalizeReceiver(long l) {

    }

    @Override
    IComputeRaysOut subProcess(int i, int i1) {
        return this
    }
}
