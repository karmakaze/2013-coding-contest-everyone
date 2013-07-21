package ca.kijiji.contest.mapred;

import java.util.concurrent.ExecutorService;

public class TaskTracker {
    private ExecutorService executorService;
    private int startedTasks;
    private int finishedTasks;
    private boolean waitingForWake;
    
    /**
     * TaskTracker uses an ExecutorService to run MapReduceTask and provides an easy way of waiting until all tasks finish
     */
    public TaskTracker(ExecutorService executorService) {
        this.executorService = executorService;
        reset();
    }
    
    /**
     * Shuts down the underlying executor service
     */
    public void shutdown() {
        executorService.shutdown();
    }
    
    /**
     * Blocks current thread until all tasks are finished, then reset the task tracker.
     */
    public void waitForTasksAndReset() throws InterruptedException {
        while (true) {
            synchronized (this) {
                if (startedTasks != finishedTasks) {
                    waitingForWake = true;
                    this.wait();
                }
                else {
                    waitingForWake = false;
                    break;
                }
            }
        }
        reset();
    }
    
    /**
     * Start a new task. Note: This is not synchronized. All tasks should be started from the same thread.
     */
    public void startTask(MapReduceTask task) {
        startedTasks++;
        executorService.submit(task);
    }
    
    /**
     * Called from individual tasks to indicate they finished execution.
     */
    public void finishTask() {
        synchronized (this) {
            finishedTasks++;
            // Wake the main thread up if needed
            if (waitingForWake && startedTasks == finishedTasks) {
                notify();
            }
        }
    }
    
    private void reset() {
        startedTasks = 0;
        finishedTasks = 0;
        waitingForWake = false;
    }
}
