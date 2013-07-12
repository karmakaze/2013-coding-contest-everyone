package ca.kijiji.contest.mapred;


public abstract class MapReduceTask implements Runnable
{
    private TaskTracker taskTracker;
    
    public MapReduceTask(TaskTracker taskTracker)
    {
        this.taskTracker = taskTracker;
    }
    
    public void run()
    {
        try
        {
            perform();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        taskTracker.finishTask();
    }
    
    public abstract void perform();
}
