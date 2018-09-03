# Copyright 2018 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

# Utility to inspect and print training details saved in a flow file.

import os
import pickle
import sling
import sling.flags as flags
import sling.myelin.flow as flow
import sys

# Needed by pickle to load the data back.
sys.path.insert(0, 'sling/nlp/parser/trainer')
import trainer as trainer

if __name__ == '__main__':
  flags.define('--flow',
               help='Flow file',
               default='',
               type=str,
               metavar='FLOW')
  flags.define('--strip',
               help='Output flow file which drops "dev" blobs',
               default='',
               type=str,
               metavar='OUTPUT_FLOW')
  flags.define('--training_details',
               help='Print training details or not',
               default=False,
               action='store_true')
  flags.define('--output_commons',
               help='Output file to store commons',
               default='',
               type=str,
               metavar='STORE')
  flags.parse()
  assert os.path.exists(flags.arg.flow), flags.arg.flow
 
  f = flow.Flow()
  f.load(flags.arg.flow)

  if flags.arg.training_details:
    details = f.blobs.get('training_details', None)
    if not details:
      print 'No training details in the flow file.'
    else:
      dictionary = pickle.loads(details.data)
      print 'Hyperparams:\n', dictionary['hyperparams'], '\n'
      print 'Number of examples seen:', dictionary['num_examples_seen']

      (final_loss, final_count) = dictionary['losses'][-1]['total']
      print 'Final loss', (final_loss / final_count)

      final_metrics = dictionary['checkpoint_metrics'][-1]
      for metric in ['SPAN_F1', 'FRAME_F1', 'ROLE_F1', 'TYPE_F1', 'SLOT_F1']:
        print 'Final', metric, ':', final_metrics[metric]
      
  if flags.arg.output_commons:
    data = f.blobs['commons'].data
    with open(flags.arg.output_commons, 'wb') as outfile:
      outfile.write(data)
      print len(data), 'bytes written to', flags.arg.output_commons

  if flags.arg.strip:
    count = 0
    for name in list(f.blobs.keys()):
      blob = f.blobs[name]
      if 'dev' in blob.attrs:
        f.blobs.pop(name)
        count += 1
    f.save(flags.arg.strip)
    print count, 'blobs removed, flow output to', flags.arg.strip

