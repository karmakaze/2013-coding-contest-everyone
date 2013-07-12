package ca.kijiji.contest.mapred;

import java.util.concurrent.ExecutorService;

public class TaskTracker
{
    private int startedTasks = 0;
    private int finishedTasks = 0;
    private boolean waitingForWake = false;
    private ExecutorService executorService;
    
    public TaskTracker(ExecutorService executorService)
    {
        this.executorService = executorService;
    }
    
    public void waitForTasks() throws InterruptedException
    {
        while (true)
        {
            synchronized (this)
            {
                if (startedTasks != finishedTasks)
                {
                    waitingForWake = true;
                    this.wait();
                }
                else
                {
                    waitingForWake = false;
                    break;
                }
            }
        }
    }
    
    /**
     * Not synchronized, assuming all tasks are started from the same thread
     */
    public void startTask(MapReduceTask task)
    {
        startedTasks++;
        executorService.submit(task);
    }
    
    /**
     * Called from individual tasks to notify that they finished execution
     */
    public void finishTask()
    {
        synchronized (this)
        {
            finishedTasks++;
            if (waitingForWake && startedTasks == finishedTasks)
            {
                notify();
            }
        }
    }
}
