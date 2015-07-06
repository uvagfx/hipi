package hipi.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Job.JobState;

public class HipiJob extends Job {

  public static final String HIPI_IMAGE_CLASS_ATTR = "hipi.image.class";

  /* Replicated from Job.java 2.7.0 */
  private void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state "+ this.state + 
                                      " instead of " + state);
    }

    if (state == JobState.RUNNING && cluster == null) {
      throw new IllegalStateException
        ("Job in state " + this.state
         + ", but it isn't attached to any job tracker!");
    }
  }

  /**
   * Set the {@link HipiImage} class for the job.
   * @param cls the <code>HipiImage</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setHipiImageClass(Class<? extends HipiImage> cls
				) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(HIPI_IMAGE_CLASS_ATTR, cls, Mapper.class);
  }  

}
