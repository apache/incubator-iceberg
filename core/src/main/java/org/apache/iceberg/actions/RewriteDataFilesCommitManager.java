/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.actions;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functionality used by RewriteDataFile Actions from different platforms to handle commits.
 */
public class RewriteDataFilesCommitManager {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesCommitManager.class);

  private final Table table;

  public RewriteDataFilesCommitManager(Table table) {
    this.table = table;
  }

  /**
   * Perform a commit operation on the table adding and removing files as
   * required for this set of file groups
   * @param fileGroups fileSets to commit
   */
  public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
    Set<DataFile> rewrittenDataFiles = Sets.newHashSet();
    Set<DataFile> addedDataFiles = Sets.newHashSet();
    for (RewriteFileGroup group : fileGroups) {
      rewrittenDataFiles = Sets.union(rewrittenDataFiles, group.rewrittenFiles());
      addedDataFiles = Sets.union(addedDataFiles, group.addedFiles());
    }

    table.newRewrite()
        .rewriteFiles(rewrittenDataFiles, addedDataFiles)
        .commit();
  }

  /**
   * Clean up a specified file set by removing any files created for that operation, should
   * not throw any exceptions
   * @param fileGroup group of files which has already been rewritten
   */
  public void abortFileGroup(RewriteFileGroup fileGroup) {
    Preconditions.checkState(fileGroup.addedFiles() != null,
        "Cannot abort a fileGroup that was not rewritten");

    Tasks.foreach(fileGroup.addedFiles())
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((dataFile, exc) -> LOG.warn("Failed to delete: {}", dataFile.path(), exc))
        .run(dataFile -> table.io().deleteFile(dataFile.path().toString()));
  }

  public void commitOrClean(Set<RewriteFileGroup> rewriteGroups) {
    try {
      commitFileGroups(rewriteGroups);
    } catch (CommitStateUnknownException e) {
      LOG.error("Commit state unknown for {}, cannot clean up files because they may have been committed successfully.",
          rewriteGroups, e);
      throw e;
    } catch (Exception e) {
      LOG.error("Cannot commit groups {}, attempting to clean up written files", rewriteGroups, e);
      rewriteGroups.forEach(this::abortFileGroup);
      throw e;
    }
  }

  /**
   * An async service which allows for committing multiple file groups as their rewrites complete. The service also
   * allows for partial-progress since commits can fail. Once the service has been closed no new file groups should not
   * be offered.
   * @param rewritesPerCommit number of file groups to include in a commit
   * @return the service for handling commits
   */
  public CommitService service(int rewritesPerCommit) {
    return new CommitService(rewritesPerCommit);
  }

  public class CommitService implements Closeable {
    private final ExecutorService committerService;
    private final ConcurrentLinkedQueue<RewriteFileGroup> completedRewrites;
    private final List<RewriteFileGroup> committedRewrites;
    private final int rewritesPerCommit;
    private final AtomicBoolean running = new AtomicBoolean(false);

    CommitService(int rewritesPerCommit) {
      LOG.info("Creating commit service for table {} with {} groups per commit", table, rewritesPerCommit);
      this.rewritesPerCommit = rewritesPerCommit;

      committerService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
          .setNameFormat("Committer-Service")
          .build());

      completedRewrites = Queues.newConcurrentLinkedQueue();
      committedRewrites = Lists.newArrayList();
    }

    /**
     * Starts a single threaded executor service for handling file group commits.
     */
    public void start() {
      Preconditions.checkState(running.compareAndSet(false, true), "Rewrite Commit service already started");
      LOG.info("Starting commit service for {}", table);
      // Partial progress commit service
      committerService.execute(() -> {
        while (running.get() || completedRewrites.size() > 0) {
          try {
            if (completedRewrites.size() == 0) {
              // Give other threads a chance to make progress
              Thread.sleep(100);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while processing commits", e);
          }

          // Either we have a full commit group, or we have completed writing and need to commit what is left over
          if (completedRewrites.size() >= rewritesPerCommit || !running.get() && completedRewrites.size() > 0) {
            Set<RewriteFileGroup> batch = Sets.newHashSetWithExpectedSize(rewritesPerCommit);
            for (int i = 0; i < rewritesPerCommit && !completedRewrites.isEmpty(); i++) {
              batch.add(completedRewrites.poll());
            }

            try {
              commitOrClean(batch);
              committedRewrites.addAll(batch);
            } catch (Exception e) {
              LOG.error("Failure during rewrite commit process, partial progress enabled. Ignoring", e);
            }
          }
        }
      });
    }

    /**
     * Places a file group in the queue to be asynchronously committed either when the queue has enough elements
     * to do a batch of size {@link #rewritesPerCommit} or the service has been closed.
     * @param group file group to eventually be committed
     */
    public void offer(RewriteFileGroup group) {
      LOG.debug("Offered to commit service: {}", group);
      Preconditions.checkState(running.get(), "Cannot add rewrites to a service which has already been closed");
      completedRewrites.add(group);
    }

    /**
     * Returns all File groups which have been committed
     */
    public List<RewriteFileGroup> results() {
      Preconditions.checkState(committerService.isShutdown(),
          "Cannot get results from a service which has not been closed");
      return committedRewrites;
    }

    @Override
    public void close() {
      Preconditions.checkState(running.compareAndSet(true, false),
          "Cannot close already closed RewriteService");
      LOG.info("Closing commit service for {}", table);
      committerService.shutdown();

      try {
        // All rewrites have completed and all new files have been created, we are now waiting for the commit
        // pool to finish doing it's commits to Iceberg State. In the case of partial progress this should
        // have been occurring simultaneously with rewrites, if not there should be only a single commit operation.
        // In either case this should take much less than 10 minutes to actually complete.
        if (!committerService.awaitTermination(10, TimeUnit.MINUTES)) {
          LOG.warn("Commit operation did not complete within 10 minutes of the files being written. This may mean " +
              "that changes were not successfully committed to the the Iceberg table.");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Cannot complete commit for rewrite, commit service interrupted", e);
      }

      Preconditions.checkState(completedRewrites.isEmpty(), "File groups offered after service was closed, " +
          "they were not successfully committed.");
    }
  }
}
