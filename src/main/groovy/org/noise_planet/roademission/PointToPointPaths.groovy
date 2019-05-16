package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.noise_planet.noisemodelling.propagation.PropagationPath

@CompileStatic
class PointToPointPaths {
    List<PropagationPath> propagationPath;
    double li
    long sourceId
    long receiverId

    /**
     * Writes the content of this object into <code>out</code>.
     * @param out the stream to write into
     * @throws java.io.IOException if an I/O-error occurs
     */
    void writePropagationPathListStream( DataOutputStream out, List<PropagationPath> propagationPaths, double li ) throws IOException {
        out.writeLong(receiverId)
        out.writeLong(sourceId)
        out.writeDouble(li)
        out.writeInt(propagationPaths.size())
        for(PropagationPath propagationPath : propagationPaths) {
            propagationPath.writeStream(out);
        }
    }

    /**
     * Reads the content of this object from <code>out</code>. All
     * properties should be set to their default value or to the value read
     * from the stream.
     * @param in the stream to read
     * @throws IOException if an I/O-error occurs
     */
    void readPropagationPathListStream( DataInputStream inputStream , ArrayList<PropagationPath> propagationPaths) throws IOException {
        receiverId = inputStream.readLong()
        sourceId = inputStream.readLong()
        li = inputStream.readDouble()
        int propagationPathsListSize = inputStream.readInt();
        propagationPaths.ensureCapacity(propagationPathsListSize);
        for(int i=0; i < propagationPathsListSize; i++) {
            PropagationPath propagationPath = new PropagationPath();
            propagationPath.readStream(inputStream);
            propagationPaths.add(propagationPath);
        }
    }

}
