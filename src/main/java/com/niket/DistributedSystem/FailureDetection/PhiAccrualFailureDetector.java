package com.niket.DistributedSystem.FailureDetection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.niket.DistributedSystem.Node;

/**
 * Phi Accrual failure detector.
 * <p>
 * Based on a paper titled: "The φ Accrual Failure Detector" by Hayashibara, et al.
 */
public class PhiAccrualFailureDetector {
    private final Map<Node, History> states = new ConcurrentHashMap<>();

    // Default value
    private static final int DEFAULT_WINDOW_SIZE = 250;
    private static final int DEFAULT_MIN_SAMPLES = 5;
    private static final long DEFAULT_MIN_STANDARD_DEVIATION_MILLIS = 1000;

    // If a node does not have any heartbeats, this is the phi
    // value to report. Indicates the node is inactive (from the
    // detectors perspective.
    private static final double DEFAULT_BOOTSTRAP_PHI_VALUE = 0;

    private final int minSamples;
    private final long minStandardDeviationMillis;
    private final double bootstrapPhiValue = DEFAULT_BOOTSTRAP_PHI_VALUE;

    private static PhiAccrualFailureDetector single_instance = null;

    public static PhiAccrualFailureDetector getInstance(){
        if(single_instance == null){
            single_instance = new PhiAccrualFailureDetector();
        }
        return single_instance;
    }


    private PhiAccrualFailureDetector() {
        this(DEFAULT_MIN_SAMPLES, DEFAULT_MIN_STANDARD_DEVIATION_MILLIS);
    }

    private PhiAccrualFailureDetector(long minStandardDeviationMillis) {
        this(DEFAULT_MIN_SAMPLES, minStandardDeviationMillis);
    }

    private PhiAccrualFailureDetector(int minSamples, long minStandardDeviationMillis) {
        this.minSamples = minSamples;
        this.minStandardDeviationMillis = minStandardDeviationMillis;
    }

    /**
     * Returns the last heartbeat time for the given node.
     *
     * @param node the node identifier
     * @return the last heartbeat time for the given node
     */
    public long getLastHeartbeatTime(Node node) {
        History nodeState = states.get(node);
        return nodeState.latestHeartbeatTime();
    }

    public void addNode(Node node){
        this.states.put(node,new History());
    }

    /**
     * Report a new heart beat for the specified node id.
     * @param nodeId node id
     */
    public void report(Node node) {
        if(states.get(node) == null){
            addNode(node);
        }
        report(node, System.currentTimeMillis());
    }

    /**
     * Report a new heart beat for the specified node id.
     * @param nodeId node id
     * @param arrivalTime arrival time
     */
    public void report(Node node, long arrivalTime) {
        History nodeState = states.get(node);
        synchronized (nodeState) {
            long latestHeartbeat = nodeState.latestHeartbeatTime();
            if (latestHeartbeat != -1) {
                nodeState.samples().addValue(arrivalTime - latestHeartbeat);
            }
            nodeState.setLatestHeartbeatTime(arrivalTime);
        }
    }

    /**
     * Resets the failure detector for the given node.
     *
     * @param nodeId node identifier for the node for which to reset the failure detector
     */
    public void reset(Node node) {
        states.remove(node);
    }

    /**
     * Compute phi for the specified node id.
     * @param nodeId node id
     * @return phi value
     */
    public double phi(Node node) {
        if (!states.containsKey(node)) {
            return bootstrapPhiValue;
        }
        History nodeState = states.get(node);
        synchronized (nodeState) {
            long latestHeartbeat = nodeState.latestHeartbeatTime();
            DescriptiveStatistics samples = nodeState.samples();
            if (latestHeartbeat == -1 || samples.getN() < minSamples) {
                return 0.0;
            }
            return computePhi(samples, latestHeartbeat, System.currentTimeMillis());
        }
    }

    private double computePhi(DescriptiveStatistics samples, long tLast, long tNow) {
        long elapsedTime = tNow - tLast;
        double meanMillis = samples.getMean();
        double y = (elapsedTime - meanMillis) / Math.max(samples.getStandardDeviation(), minStandardDeviationMillis);
        double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
        if (elapsedTime > meanMillis) {
            return -Math.log10(e / (1.0 + e));
        } else {
            return -Math.log10(1.0 - 1.0 / (1.0 + e));
        }
    }

    private static class History {
        DescriptiveStatistics samples = new DescriptiveStatistics(DEFAULT_WINDOW_SIZE);
        long lastHeartbeatTime = -1;

        DescriptiveStatistics samples() {
            return samples;
        }

        long latestHeartbeatTime() {
            return lastHeartbeatTime;
        }

        void setLatestHeartbeatTime(long value) {
            lastHeartbeatTime = value;
        }
    }
}