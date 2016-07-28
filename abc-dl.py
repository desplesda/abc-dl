#!/usr/bin/env python
# 
# MIT License
# 
# Copyright (c) 2016 Jon Manning
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import requests
import os

try:
    from progressbar import ProgressBar
    from progressbar.widgets import Bar, Percentage, ETA, SimpleProgress, Timer
    progressbar_available = True
except: 
    progressbar_available = False

from optparse import OptionParser
import logging
from datetime import datetime, timedelta
import time

FROM = 6000000
TO = 7000000

if progressbar_available:
	class AdaptiveETA(Timer):
	    """Widget which attempts to estimate the time of arrival.
	    Uses a weighted average of two estimates:
	      1) ETA based on the total progress and time elapsed so far
	      2) ETA based on the progress as per the last 10 update reports
	    The weight depends on the current progress so that to begin with the
	    total progress is used and at the end only the most recent progress is
	    used.
	    """
	
	    TIME_SENSITIVE = True
	    NUM_SAMPLES = 10
	
	    def _update_samples(self, currval, elapsed):
	        sample = (currval, elapsed)
	        if not hasattr(self, 'samples'):
	            self.samples = [sample] * (self.NUM_SAMPLES + 1)
	        else:
	            self.samples.append(sample)
	        return self.samples.pop(0)
	
	    def _eta(self, maxval, currval, elapsed):
	        return elapsed * maxval / float(currval) - elapsed
	
	    def update(self, pbar):
	        """Updates the widget to show the ETA or total time when finished."""
	        if pbar.currval == 0:
	            return 'ETA:  --:--:--'
	        elif pbar.finished:
	            return 'Time: %s' % self.format_time(pbar.seconds_elapsed)
	        else:
	            elapsed = pbar.seconds_elapsed
	            currval1, elapsed1 = self._update_samples(pbar.currval, elapsed)
	            eta = self._eta(pbar.maxval, pbar.currval, elapsed)
	            if pbar.currval > currval1:
	                etasamp = self._eta(pbar.maxval - currval1,
	                                    pbar.currval - currval1,
	                                    elapsed - elapsed1)
	                weight = (pbar.currval / float(pbar.maxval)) ** 0.5
	                eta = (1 - weight) * eta + weight * etasamp
	            return 'ETA:  %s' % self.format_time(eta)

URL_TEMPLATE = "https://content-api-govhack.abc-prod.net.au/v1/{}"

def main():
    
    global FROM
    global TO
    
    # parse command line options
    parser = OptionParser()
    
    parser.add_option("-o", "--output-dir", action="store", default="output", help="The destination folder. Will be created if it doesn't exist. Default: output/")
    
    parser.add_option("-i", "--index-file", action="store", default="index.txt", help="The file containing IDs that have been downloaded. These will be skipped on subsequent runs. Default: index.txt")
    
    parser.add_option("-e", "--errors-file", action="store", default="errors.txt", help="The file containing IDs to skip. This will be appended to during operation. Default: errors.txt")
    
    parser.add_option("--log", action="store", default="log.txt", help="The log file.")
    
    parser.add_option("-s", "--slowdown-factor", default=0.5, type=float, help="The time (in seconds) between requests when throttling.")
    
    (options, args) = parser.parse_args()
    
    if len(args) < 2:
        print("Defaulting to the range {} to {}. For help, see --help.".format(FROM, TO))
    else:
        FROM=int(args[0])
        TO=int(args[1])
    
    # set up the logger
    logging.basicConfig(format='%(asctime)s %(message)s', filename=options.log, level=logging.INFO)
    
    # make the output directory if it doesn't exist
    if os.path.isdir(options.output_dir) == False:        
        os.makedirs(options.output_dir)
        logging.info("Creating output directory {}".format(options.output_dir))
    
    # we'll be requesting stuff within this range
    id_range = range(FROM, TO)
    
    # we'll skip all requests whose ID is in this list
    SKIP_IDS = []
    
    # does the error file exist? read all IDs found in it
    if os.path.isfile(options.errors_file):
        SKIP_IDS = [int(line) for line in open(options.errors_file).readlines()]
    
    # we'll append to the skip file
    skip_file = open(options.errors_file, "a")
    
    # does the index file exist? read all IDs found in it, too
    if os.path.isfile(options.index_file):
        SKIP_IDS += [int(line) for line in open(options.index_file).readlines()]
    
    # and open the index file so we can append to it
    index_file = open(options.index_file, "a")
    
    # make this a set to make searching faster
    SKIP_IDS = set(SKIP_IDS)
    
    # show a pretty progress bar
    if progressbar_available:
        pbar = ProgressBar(widgets=[AdaptiveETA(), Percentage(), Bar(), SimpleProgress()], maxval=len(id_range)).start()
    
    # this var contains the time after which we'll resume normal speed; 
    # it starts in the past
    stop_delaying_at = datetime.now() - timedelta(seconds=5)
    
    # let's a-go!
    for i, article_id in enumerate(id_range):
        
        # skip odd-numbered IDs, which are always folders
        if article_id % 2 != 0:
            continue
        
        # skip articles that have either errored or downloaded already
        if article_id in SKIP_IDS:
            continue
        
        # wait a bit before doing this request, if necessary
        if datetime.now() < stop_delaying_at:
            time.sleep(options.slowdown_factor)
        
        # get the resource
        url = URL_TEMPLATE.format(article_id)
        
        result = requests.get(url)
        
        # update the progress bar
        if progressbar_available:
            pbar.update(i)
        elif i % 50 == 0:
        		# a more primitive progressbar
        		print("{}/{}".format(i, len(id_range)))
        
        # did the server tell us to slow down?
        if result.status_code == requests.codes.too_many_requests:
            
            # indicate that we should stop delaying in 10 minutes
            stop_delaying_at = datetime.now() + timedelta(minutes = 10)
            
            logging.info("Server indicated too many requests; slowing for 10 minutes (will resume full speed at {})".format(stop_delaying_at))
            
            # skip this resource, which is probably OK
            # TODO: wait a moment and try this request again
            continue
            
        # was it successful?
        if result.status_code == requests.codes.ok:
            
            # attempt to parse the json and determine if it's worth keeping
            try:
                if "docType" in result.json():
                    
                    # it's a valid resource, save it
                    filename = "{}_{}.json".format(result.json()["docType"], article_id)
            
                    output_path = os.path.join(options.output_dir, filename)
                    
                    with open(output_path, 'wb') as fd:
                        fd.write(result.content)
            
                    logging.info("Successfully retrieved {}".format(url))
            
                    if options.index_file:
                        index_file.write("{}\n".format(article_id))
                else:
                    
                    # it's not a valid document
                    logging.info("Skipping {} (HTTP 200 but not a valid document)".format(url))
                    if options.errors_file:
                        skip_file.write("{}\n".format(article_id))
                        
            except ValueError as e:
                # we couldn't parse the JSON
                
                logging.info("Skipping {} (invalid JSON)".format(url))
                
                # record that this is a bad entry
                if options.errors_file:
                    skip_file.write("{}\n".format(article_id))
        else:
            # we got some other HTTP code, so skip it and record that it's bad
            logging.info("Skipping {} (HTTP {})".format(url, result.status_code))
            if options.errors_file:
                skip_file.write("{}\n".format(article_id))

    # all done!
    if progressbar_available:
        pbar.finish()
    
    logging.info("Process complete!")

if __name__ == '__main__':
    main()