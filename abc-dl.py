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
from progressbar import ProgressBar
from progressbar.widgets import Bar, Percentage, ETA, SimpleProgress, Timer
from optparse import OptionParser
import logging
from datetime import datetime, timedelta
import time

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

SKIP_IDS = []

def main():
    
    parser = OptionParser()
    
    parser.add_option("-o", "--output-dir", action="store", default="output", help="The destination folder. Will be created if it doesn't exist. Default: output/")
    
    parser.add_option("-i", "--index-file", action="store", default="index.txt", help="The file containing IDs that have been downloaded. These will be skipped on subsequent runs. Default: index.txt")
    
    parser.add_option("-e", "--errors-file", action="store", default="errors.txt", help="The file containing IDs to skip. This will be appended to during operation. Default: errors.txt")
    
    parser.add_option("--log", action="store", default="log.txt", help="The log file.")
    
    parser.add_option("-s", "--slowdown-factor", default=0.5, type=float, help="The time (in seconds) between requests when throttling.")
    
    
    (options, args) = parser.parse_args()
    
    if len(args) < 2:
        print("Usage: abc-dl.py <from id> <to id>\n\tFor more information, run abc-dl.py --help")
        return
    
    if os.path.isdir(options.output_dir) == False:        
        os.makedirs(options.output_dir)
        print("Creating output directory {}".format(options.output_dir))
        
    logging.basicConfig(format='%(asctime)s %(message)s', filename=options.log, level=logging.INFO)
        
    
    id_range = range(int(args[0]), int(args[1]))
    
    
    SKIP_IDS = []
    
    if os.path.isfile(options.errors_file):
        SKIP_IDS = [int(line) for line in open(options.errors_file).readlines()]
    
    skip_file = open(options.errors_file, "a")
    
    if os.path.isfile(options.index_file):
        SKIP_IDS += [int(line) for line in open(options.index_file).readlines()]
        
    SKIP_IDS = set(SKIP_IDS)
    
    index_file = open(options.index_file, "a")
    
    pbar = ProgressBar(widgets=[AdaptiveETA(), Percentage(), Bar(), SimpleProgress()], maxval=len(id_range)).start()
    
    
    stop_delaying_at = datetime.now() - timedelta(seconds=5)
    
    
    for i, article_id in enumerate(id_range):
        
        if article_id in SKIP_IDS:
            continue
            
        now = datetime.now()
        
        if now < stop_delaying_at:
            time.sleep(options.slowdown_factor)
        
        url = URL_TEMPLATE.format(article_id)
        
        #print("Getting {}".format(url))
        
        result = requests.get(url)
        
        pbar.update(i)
        
        if result.status_code == requests.codes.too_many_requests:
            
            stop_delaying_at = datetime.now() + timedelta(minutes = 10)
            
            logging.info("Server indicated too many requests; slowing for 10 minutes (will resume full speed at {})".format(stop_delaying_at))
            
            continue
            
        
        if result.status_code == requests.codes.ok:
            
            try:
                if "docType" in result.json():
                    filename = "{}_{}.json".format(result.json()["docType"], article_id)
            
                    output_path = os.path.join(options.output_dir, filename)
                    
                    with open(output_path, 'wb') as fd:
                        fd.write(result.content)
            
                    logging.info("Successfully retrieved {}".format(url))
            
                    if options.index_file:
                        index_file.write("{}\n".format(article_id))
                else:
                    logging.info("Skipping {} (HTTP 200 but not a valid document)".format(url))
                    if options.errors_file:
                        skip_file.write("{}\n".format(article_id))
                        
            except ValueError as e:
                logging.info("Skipping {} (invalid JSON)".format(url))
                if options.errors_file:
                    skip_file.write("{}\n".format(article_id))
            
            
            
        else:
            logging.info("Skipping {} (HTTP {})".format(url, result.status_code))
            if options.errors_file:
                skip_file.write("{}\n".format(article_id))

    pbar.finish()

if __name__ == '__main__':
    main()