HIPI (Hadoop Image Processing Interface) is a library designed to provide efficient and high-throughput image processing in the Apache Hadoop MapReduce parallel programming framework. It also provides support for [OpenCV](http://opencv.org).

For more detail about what HIPI is, see the [Main HIPI Webpage](http://hipi.cs.virginia.edu).

To stay in touch with the HIPI development community join the [HIPI Users Group](https://groups.google.com/forum/#!forum/hipi-users).

# Getting started

## 1. Setup Hadoop

HIPI works with a standard installation of the Apache Hadoop Distributed File System (HDFS) and MapReduce. HIPI has been tested with Hadoop version 2.7.1.
	
If you haven't already done so, download and install Hadoop by following the instructions on the official [Apache Hadoop website](http://hadoop.apache.org/). A very useful resource is their [Quickstart Guide](http://wiki.apache.org/hadoop/QuickStart), in particular, the [Single Cluster Setup](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) for first-time users.

Ensure that the Hadoop bin directory is in your system path:

      $> which hadoop
      /usr/local/bin/hadoop

## 2. Install Gradle

The HIPI distribution uses the [Gradle](http://gradle.org) build automation system for code compilation and assembly. HIPI has been tested with Gradle version 2.5:

Install `gradle` on your system and verify that it is in your path as well:

      $> which gradle
      /usr/local/bin/gradle

## 3. Clone the HIPI Repository

Clone the latest HIPI distribution from GitHub:

      $> git clone git@github.com:uvagfx/hipi.git

## 4. Build the HIPI Library and Tools/Example Programs

From the HIPI root directory, simply run `gradle` to build the HIPI library along with the complete set of tools and example programs:

      $> gradle
      :checkJavaVersion
      Found Java 1.8.
      :core:compileJava
      :core:processResources
      :core:classes
      :core:jar
      :tools:downloader:compileJava
      :tools:downloader:processResources
      :tools:downloader:classes
      :tools:downloader:jar
      :tools:dumpHib:compileJava
      :tools:dumpHib:processResources
      :tools:dumpHib:classes
      :tools:dumpHib:jar
      ...
      :install

      Finished building the HIPI library along with all tools and examples.

      BUILD SUCCESSFUL

      Total time: 2.058 secs

If the build fails, first carefully review the steps above. If you are convinced that you are doing everything correctly and that you've found an issue with the HIPI distribution or documentation please post a question to the [HIPI Users Group](https://groups.google.com/forum/#!forum/hipi-users) or use the [Issue Tracker](https://github.com/uvagfx/hipi/issues) to file a bug report.

After the build successfully finishes, you may want to inspect the `settings.gradle` file in the root directory along with the `build.gradle` files in the root, core, and tools directories in order to familiarize yourself with the various build targets. If you're new to Gradle, we recommend reading the [Gradle Java Tutorial](https://docs.gradle.org/current/userguide/tutorial_java_projects.html). For example, to build only the [tools/hibImport](http://hipi.cs.virginia.edu/tools/hibImport.html) tool from scratch:


      $> gradle clean tools:hibImport:jar
      :core:clean
      ...
      :core:compileJava
      :core:processResources UP-TO-DATE
      :core:classes
      :core:jar
      :tools:hibImport:compileJava
      :tools:hibImport:processResources UP-TO-DATE
      :tools:hibImport:classes
      :tools:hibImport:jar

      BUILD SUCCESSFUL

      Total time: 1.197 secs

HIPI is now installed on your system. To learn about future updates to the HIPI distribution you should join the [HIPI Users Group](https://groups.google.com/forum/#!forum/hipi-users) and watch this repository. You can always obtain the latest version of HIPI on the release branch with the following command:

      $> git pull origin release
      From github.com:uvagfx/hipi
       * branch            release    -> FETCH_HEAD
      Already up-to-date.

Also, you can experiment with the `development` branch, which often contains new features that have not yet been integrated into the main release branch. Note that the `development` branch is generally less stable than the `release` branch.

## Next Steps

Be sure to check out the [HIPI Tools and Example Programs](http://hipi.cs.virginia.edu/examples.html) to learn more about HIPI.
