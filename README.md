HIPI (Hadoop Image Processing Interface) is a library designed to provide efficient and high-throughput image processing in the Apache Hadoop MapReduce parallel programming framework.

For more detail about what HIPI is, see the [Main HIPI Webpage](http://hipi.cs.virginia.edu).

To stay in touch with the HIPI development community join the [HIPI Users Group](https://groups.google.com/forum/#!forum/hipi-users).

# Getting started

## 1. Setup Hadoop

HIPI works with a standard installation of the Apache Hadoop Distributed File System (HDFS) and MapReduce. HIPI has been tested with Hadoop version 2.6.0.
	
If you haven't already done so, download and install Hadoop by following the instructions on the official [Apache Hadoop website](http://hadoop.apache.org/). A very useful resource is their [Quickstart Guide](http://wiki.apache.org/hadoop/QuickStart), in particular, the [Single Cluster Setup](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) for first-time users.

Ensure that the Hadoop bin directory is in your system path:

      $> which hadoop
      /usr/local/bin/hadoop

## 2. Install Apache Ant

The HIPI distribution uses [Apache Ant](http://ant.apache.org) for code compilation. HIPI has been tested with Apache Ant version 1.9.4.

Install `ant` on your system and verify that it is in your path as well:

      $> which ant
      /usr/local/bin/ant

## 3. Clone the HIPI Repository

Clone the latest HIPI distribution from GitHub:

      $> git clone git@github.com:uvagfx/hipi.git

## 4. Update build.xml

After the repository has been cloned, you must edit two lines in the `build.xml` file in the HIPI root directory to indicate the path to your Hadoop installation and the version of Hadoop you are using:

      <property name="hadoop.home" value="/opt/hadoop-2.6.0/share/hadoop" />
      <property name="hadoop.version" value="2.6.0" />

The correct value of `hadoop.home` and `hadoop.version` may differ on your system from what is shown above. For example, Hadoop version 2.5.1 on a Mac OS X system installed using homebrew might have the following:

      <property name="hadoop.home" value="/usr/local/Cellar/hadoop/2.5.1/libexec/share/hadoop" />
      <property name="hadoop.version" value="2.5.1" />

## 5. Build the HIPI Library and Example Programs

From the HIPI root directory, simply run `ant` to build the HIPI library along with all of the tools and example programs:

      $> ant
      ...
      hipi:
         [javac] Compiling 30 source files to /users/horton/hipi/lib
           [jar] Building jar: /users/horton/hipi/lib/hipi-2.0.jar
          [echo] Hipi library built.

      compile:
         [javac] Compiling 1 source file to /users/horton/hipi/bin
           [jar] Building jar: /users/horton/hipi/examples/covariance.jar
          [echo] Covariance built.

      all:
          BUILD SUCCESSFUL
          Total time: 3 seconds

If the build fails, first carefully review the steps above. If you are convinced that you are doing everything correctly and that you've found an issue with the HIPI distribution or documentation please post a question to the [HIPI Users Group](https://groups.google.com/forum/#!forum/hipi-users) or use the [Issue Tracker](https://github.com/uvagfx/hipi/issues) to file a bug report.

After the build successfully finishes, open the `build.xml` file in a text editor and have a look at the various build tasks. They should be straightforward. For example, to build only the [DumpHib Example Program](http://hipi.cs.virginia.edu/examples/dumphib.html) you would execute:

      $> ant dumphib

      Buildfile: /users/horton/hipi/build.xml

      dumphib:
         [echo] Building dumphib example...

      ...

      compile:
        [javac] Compiling 1 source file to /users/horton/hipi/bin
          [jar] Building jar: /users/horton/hipi/examples/dumphib.jar
         [echo] Dumphib built.

      BUILD SUCCESSFUL
      Total time: 1 second

HIPI is now installed on your system. To learn about future updates to the HIPI distribution you should join the [HIPI Users Group](https://groups.google.com/forum/#!forum/hipi-users) and watch this repository. You can always obtain the latest version of HIPI on the release branch with the following command:


      $> git pull origin release
      From github.com:uvagfx/hipi
       * branch            release    -> FETCH_HEAD
      Already up-to-date.

Also, you can experiment with the `development` branch, which contains the latest features that have not yet been integrated into the main release branch. Note that the `development` branch is generally less stable than the `release` branch.

## Next Steps

Be sure to check out the [HIPI Tools and Example Programs](http://hipi.cs.virginia.edu/examples.html) to learn more about HIPI.
