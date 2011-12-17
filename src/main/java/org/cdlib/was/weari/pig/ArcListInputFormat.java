/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Parts (c) 2011 University of California Regents
 */

package org.cdlib.was.weari.pig;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Special InputFormat with small splits, because each line is an arc
 * file that takes a long time to process
 *
 * @author Erik Hetzner
 */
public class ArcListInputFormat extends TextInputFormat {
    private static final Log LOG = LogFactory.getLog(ArcListInputFormat.class);
    
    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

    @Override
    public long getFormatMinSplitSize () {
        return 0L;
    }

    public static long getMinSplitSize(JobContext job) {
        return 0L;
    }

    public static long getMaxSplitSize(JobContext job) {
        return 512L;
    }

    /** 
     * Generate the list of files and make them into FileSplits.
     */ 
    public List<InputSplit> getSplits(JobContext job
                                      ) throws IOException {
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);

        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus>files = listStatus(job);
        for (FileStatus file: files) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            long length = file.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            if ((length != 0) && isSplitable(job, path)) { 
                long blockSize = file.getBlockSize();
                long splitSize = computeSplitSize(blockSize, minSize, maxSize);

                long bytesRemaining = length;
                while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                    int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
                    splits.add(new FileSplit(path, length-bytesRemaining, splitSize, 
                                             blkLocations[blkIndex].getHosts()));
                    bytesRemaining -= splitSize;
                }
        
                if (bytesRemaining != 0) {
                    splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, 
                                             blkLocations[blkLocations.length-1].getHosts()));
                }
            } else if (length != 0) {
                splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
            } else { 
                //Create empty hosts array for zero length files
                splits.add(new FileSplit(path, 0, length, new String[0]));
            }
        }
    
        // Save the number of input files in the job-conf
        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

        LOG.debug("Total # of splits: " + splits.size());
        return splits;
    }
}
