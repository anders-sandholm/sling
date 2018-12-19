# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Class for extracting birth death date facts from English Wikipedia articles and storing them in a record file"""

import sling
import sling.flags as flags
import re

class ExtractWikipediaDates:
  def __init__(self):
    self.kb = sling.Store()
    self.kb.lockgc()
    print "loading kb"
    self.kb.load("local/data/e/wiki/kb.sling", snapshot=True)
    print "kb loaded"
    self.instanceof = self.kb['P31']
    self.date_of_birth = self.kb['P569']
    self.date_of_death = self.kb['P570']
    self.human = self.kb['Q5']
    self.item = self.kb["item"]
    self.facts = self.kb["facts"]
    self.provenance = self.kb["provenance"]
    self.url = self.kb["url"]
    self.method = self.kb["method"]

    self.kb.freeze()

    self.months = {
      "January": 1,
      "February": 2,
      "March": 3,
      "April": 4,
      "May": 5,
      "June": 6,
      "July": 7,
      "August": 8,
      "September": 9,
      "October": 10,
      "November": 11,
      "December": 12
    }

  """Sling Date from regex match"""
  def date_from_match(self, offset, match):
    if match.group(1 + offset):
      # DD MONTH YYYY format
      date = sling.Date(int(match.group(3 + offset)),
                        self.months[match.group(2 + offset)],
                        int(match.group(1 + offset)))
    else:
      # MONTH DD, YYYY format"""
      date = sling.Date(int(match.group(6 + offset)),
                        self.months[match.group(4 + offset)],
                        int(match.group(5 + offset)))
    return date

  def run(self):
    month = "(January|February|March|April|May|June|July|August|September|"
    month += "October|November|December)"
    day = "(\d{1,2})"
    year = "(\d{4})"
    date = "(?:(?:" + day + " " + month + " " + year + ")|"
    date += "(?:" + month + " " + day + ", " + year + "))"
    date += "(?:[^)]+?)?"
    dates = date + "\s*-+\s*" + date
    dates = "(?:(?:born " + date + ")|(?:" + dates + "))"
    pat = "(?:[^(]|\([^0-9]*\))*?\([, ;a-zA-Z\-]*?" + dates + "\s*\)"
    rec = re.compile(pat)

    self.out_file = "local/data/e/wikibot/birth-death-dates.rec"
    record_file = sling.RecordWriter(self.out_file)
    records = 0
    store = sling.Store(self.kb)

    for i in range(10):
      i_file = "local/data/e/wiki/en/documents-0000"+str(i)+"-of-00010.rec"
      for (item_id, record) in sling.RecordReader(i_file):
        item = self.kb[item_id]
        if self.human not in item(self.instanceof): continue
        if item[self.date_of_birth] is not None:
          if item[self.date_of_death] is not None: continue
        parsed_record = sling.Store().parse(record)
        doc = sling.Document(parsed_record)
        raw_text = parsed_record['text']
        if len(raw_text) ==  0:
          print item_id, "has an empty 'text' field"
          continue
        start_index = raw_text.find("<b>") + len("<b>")
        first = 1
        while first < len(doc.tokens) and \
              doc.tokens[first].start <= start_index: first += 1
        last = first
        while last < len(doc.tokens) and doc.tokens[last].brk < 3:
          last += 1
        text = doc.phrase(max(0, first - 1), min(len(doc.tokens), last + 15))
        m = rec.match(text)
        if m is None: continue
        if text.find("(baptised") >= 0 or text.find("throne") >= 0: continue
        # Birth date only match
        if m.group(1) or m.group(4):
          if item[self.date_of_birth] is not None: continue
          facts = store.frame({
            self.date_of_birth: self.date_from_match(0, m).value(),
          })
        else:
          facts = store.frame({
            self.date_of_birth: self.date_from_match(6, m).value(),
            self.date_of_death: self.date_from_match(12, m).value()
          })
        print "Match", records, item.id, item.name
        records += 1
        provenance = store.frame({
          self.url: parsed_record['url'],
          self.method: "English Wikipedia dates for '" + str(item.name) + "'"
        })
        fact = store.frame({
          self.item: item,
          self.facts: facts,
          self.provenance: provenance
        })
        record_file.write(item.id, fact.data(binary=True))
    record_file.close()
    print records, "birth/death date records written to file:", self.out_file

if __name__ == '__main__':
  flags.parse()
  extract_dates = ExtractWikipediaDates()
  extract_dates.run()
