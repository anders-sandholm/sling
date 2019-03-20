# Copyright 2017 Google Inc.
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

import sling
import sling.log as log

from collections import defaultdict
from enum import Enum
from sling.task.workflow import register_task
from util import load_kb


"""
Match-type of a proposed fact vs any existing fact.
"""
class FactMatchType(Enum):
  NEW = 0                   # no fact currently exists for the property
  EXACT = 1                 # proposed fact exactly matches an existing one
  SUBSUMES_EXISTING = 2     # proposed fact is coarser than an existing one
  SUBSUMED_BY_EXISTING = 3  # an existing fact is coarser than the proposed one
  CONFLICT = 4              # for unique-valued pids, conflicts with existing
  ADDITIONAL = 5            # proposed fact would be an extra one


"""
FactMatcher matches a proposed (source_qid, pid, target_qid) fact against
any existing (source_qid, pid, _) facts.

For example, consider the category "Danish cyclists", with a candidate parse:
  country_of_citizenship=QID(Denmark) and sport=QID(cycling).

For this parse, we would separately compute and report fact matching
statistics for each of the two assertions over all members of the category.
"""
class FactMatcher:
  # For CONFLICT, SUBSUMES_EXISTING, and EXACT, we store only a handful
  # of source items. For the rest, we store all source items.
  MAX_SOURCE_ITEMS = 10

  TYPES_WITH_EXEMPLARS_ONLY = set([
      FactMatchType.EXACT,
      FactMatchType.CONFLICT,
      FactMatchType.SUBSUMES_EXISTING
  ])


  """
  Output of match statistics computation across multiple source_qids.
  This is comprised of a histogram over various FactMatchType buckets,
  and corresponding source qids.
  """
  class Output:
    def __init__(self):
      self.counts = defaultdict(int)         # match type -> count
      self.source_items = defaultdict(list)  # match type -> [some] source items

    # Adds a match type with corresponding source item to the output.
    def add(self, match_type, source_item):
      self.counts[match_type] += 1
      if match_type not in FactMatcher.TYPES_WITH_EXEMPLARS_ONLY or \
          len(self.source_items[match_type]) < FactMatcher.MAX_SOURCE_ITEMS:
          self.source_items[match_type].append(source_item)

    # Saves and returns the histogram as a frame.
    def as_frame(self, store):
      buckets = []
      for match_type, count in self.counts.iteritems():
        bucket_frame = store.frame([
          ("match_type", match_type.name),
          ("count", count),
          ("source_items", self.source_items[match_type])])
        if match_type in self.source_items:
          items = []
        buckets.append(bucket_frame)
      buckets = store.array(buckets)
      frame = store.frame({"buckets": buckets})

      return frame


    # String representation of the histogram.
    def __repr__(self):
      keys = list(sorted(self.counts.keys()))
      kv = [(k, self.counts[k]) for k in keys]
      return '{%s}' % ', '.join(["%s:%d" % (x[0].name, x[1]) for x in kv])


  def __init__(self, kb, extractor):
    self.kb = kb
    self.extractor = extractor
    self.unique_properties = set()
    self.date_properties = set()
    self.location_properties = set()

    # Collect unique-valued, date-valued, and location-valued properties.
    # The former will be used to compute CONFLICT counts, and the latter need to
    # be processed in a special manner while matching existing facts.
    constraint_role = kb["P2302"]
    unique = kb["Q19474404"]         # single-value constraint
    w_time = kb["/w/time"]
    w_item = kb["/w/item"]
    p_subproperty_of = kb["P1647"]
    p_location = kb["P276"]
    for prop in kb["/w/entity"]("role"):
      if prop.target == w_time:
        self.date_properties.add(prop)
      if prop.target == w_item:
        for role, value in prop:
          if role == p_subproperty_of:
            if kb.resolve(value) == p_location:
              self.location_properties.add(prop)
      for constraint_type in prop(constraint_role):
        if constraint_type == unique or constraint_type["is"] == unique:
          self.unique_properties.add(prop)

    log.info("%d unique-valued properties" % len(self.unique_properties))
    log.info("%d date-valued properties" % len(self.date_properties))
    log.info("%d location-valued properties" % len(self.location_properties))

    # Set closure properties.
    self.closure_properties = {}
    self.p_subclass = kb["P279"]
    self.p_parent_org = kb["P749"]
    p_located_in = kb["P131"]
    for p in self.location_properties:
      self.closure_properties[p] = p_located_in

    # 'Educated at' -> 'Part of'.
    self.closure_properties[kb["P69"]] = kb["P361"]


  # Returns whether 'prop' is a date-valued property.
  def _date_valued(self, prop):
    return prop in self.date_properties


  # Returns existing targets for the given property for the given item.
  # The property could be a pid path.
  def _existing_facts(self, store, item, prop, closure):
    assert type(prop) is list
    pid = prop[0]
    facts = self.extractor.facts_for(store, item, [pid], closure)
    output = []
    for fact in facts:
      if list(fact[:-1]) == prop:
        output.append(fact[-1])
    return output


  # Returns whether 'first' is a finer-precision date than 'second'.
  # 'first' and 'second' should be sling.Date objects.
  def _finer_date(self, first, second):
    if first.precision <= second.precision:
      return False
    if second.precision == sling.MILLENNIUM:
      return first.year >= second.year and first.year < second.year + 1000
    if second.precision == sling.CENTURY:
      return first.year >= second.year and first.year < second.year + 100
    if second.precision == sling.DECADE:
      return first.year >= second.year and first.year < second.year + 10
    if second.precision == sling.YEAR:
      return first.year == second.year
    if second.precision == sling.MONTH:
      return first.year == second.year and first.month == second.month

    # Should not reach here.
    return False


  # Returns whether 'coarse' subsumes 'fine' by following 'prop' edges.
  def subsumes(self, store, prop, coarse, fine):
    coarse = self.kb.resolve(coarse)
    fine = self.kb.resolve(fine)
    if coarse == fine:
      return True
    closure_property = self.closure_properties.get(prop, None)

    if closure_property is not None:
      return self.extractor.in_closure(store, closure_property, coarse, fine)
    else:
      return self.extractor.in_closure(store, self.p_subclass, coarse, fine) \
          or self.extractor.in_closure(store, self.p_parent_org, coarse, fine)


  # Reports match type for the proposed fact (item, prop, value) against
  # any existing fact for the same item and property.
  # 'value' should be a sling.Frame object.
  # 'prop' could be a property (sling.Frame) or a pid-path represented either
  # as a sling.Array or a list.
  #
  # Returns the type of the match.
  def for_item(self, item, prop, value, store=None):
    assert isinstance(value, sling.Frame)
    if isinstance(prop, sling.Frame):
      prop = [prop]
    else:
      prop = list(prop)

    if store is None:
      store = sling.Store(self.kb)

    # Compute existing facts without any backoff.
    exact_facts = self._existing_facts(store, item, prop, False)
    if len(exact_facts) == 0:
      return FactMatchType.NEW

    if value in exact_facts:
      return FactMatchType.EXACT

    # For date-valued properties, existing dates could be int or string
    # (which won't match 'value', which is a sling.Frame). For them, we do a
    # more elaborate matching procedure.
    if self._date_valued(prop[-1]):
      proposed_date = sling.Date(value)
      existing_dates = [sling.Date(e) for e in exact_facts]
      for e in existing_dates:
        if e.value() == proposed_date.value():
          return FactMatchType.EXACT

    # Check whether the proposed fact subsumes an existing fact.
    # dates require special treatment.
    if self._date_valued(prop[-1]):
      for e in existing_dates:
        if self._finer_date(e, proposed_date):
          return FactMatchType.SUBSUMES_EXISTING
    else:
      for existing in exact_facts:
        if isinstance(existing, sling.Frame):
          if self.subsumes(store, prop[-1], value, existing):
            return FactMatchType.SUBSUMES_EXISTING

    # Check whether the proposed fact is subsumed by an existing fact.
    # Again, dates require special treatment.
    if self._date_valued(prop[-1]):
      for e in existing_dates:
        if self._finer_date(proposed_date, e):
          return FactMatchType.SUBSUMED_BY_EXISTING
    else:
      for existing in exact_facts:
        if isinstance(existing, sling.Frame):
          if self.subsumes(store, prop[-1], existing, value):
            return FactMatchType.SUBSUMED_BY_EXISTING

    # Check for conflicts in case of unique-valued properties.
    if len(prop) == 1 and prop[0] in self.unique_properties:
      return FactMatchType.CONFLICT

    # Proposed fact is an additional one.
    return FactMatchType.ADDITIONAL


  # Same as above, but returns a histogram of match types over multiple items.
  def for_items(self, items, prop, value, store=None):
    if store is None:
      store = sling.Store(self.kb)
    output = FactMatcher.Output()
    for item in items:
      match = self.for_item(item, prop, value, store)
      output.add(match, item)
    return output


  # Same as above, but returns one list of match-type histograms per parse
  # in 'category'. The list of source items is taken to be the members of
  # 'category'.
  #
  # 'category' is a frame that is produced by the initial stages of the category
  # parsing pipeline (cf. parse_generator.py and prelim_ranker.py).
  #
  # Most parses share a lot of common spans, so this method caches and reuses
  # match statistics for such spans.
  def for_parses(self, category, store=None):
    if store is None:
      store = sling.Store(self.kb)

    items = category.members
    output = []    # ith entry = match stats for ith parse
    cache = {}     # (pid, qid) -> match stats
    for parse in category("parse"):
      parse_stats = []
      for span in parse.spans:
        key = (span.pids, span.qid)
        stats = None
        if key in cache:
          stats = cache[key]
        else:
          stats = self.for_items(items, span.pids, span.qid, store)
          cache[key] = stats
        parse_stats.append(stats)
      output.append(parse_stats)
    return output


# Task that adds fact matching statistics to each span in each category parse.
class FactMatcherTask:
  def init(self, task):
    self.kb = load_kb(task)
    self.extractor = sling.FactExtractor(self.kb)
    self.matcher = FactMatcher(self.kb, self.extractor)


  # Runs the task over a recordio of category parses.
  def run(self, task):
    self.init(task)
    reader = sling.RecordReader(task.input("parses").name)
    writer = sling.RecordWriter(task.output("output").name)
    for key, value in reader:
      store = sling.Store(self.kb)
      category = store.parse(value)
      matches = self.matcher.for_parses(category, store)
      frame_cache = {}   # (pid, qid) -> frame containing their match statistics
      for parse, parse_match in zip(category("parse"), matches):
        for span, span_match in zip(parse.spans, parse_match):
          span_key = (span.pids, span.qid)
          if span_key not in frame_cache:
            match_frame = span_match.as_frame(store)
            frame_cache[span_key] = match_frame
          span["fact_matches"] = frame_cache[span_key]
      writer.write(key, category.data(binary=True))
      task.increment("fact-matcher/categories-processed")
    reader.close()
    writer.close()

register_task("category-parse-fact-matcher", FactMatcherTask)


# Loads a KB and brings up a shell to compute and debug match statistics.
def shell():
  kb = load_kb("local/data/e/wiki/kb.sling")
  extractor = sling.api.FactExtractor(kb)
  matcher = FactMatcher(kb, extractor)

  parses = "local/data/e/wikicat/filtered-parses.rec"
  db = sling.RecordDatabase(parses)

  while True:
    item = raw_input("Enter item or category QID:")

    # See if a category QID was entered, if so, compute and output match
    # statistics for all its parses.
    value = db.lookup(item)
    if value is not None:
      store = sling.Store(kb)
      category = store.parse(value)
      output = matcher.for_parses(category, store)
      print "%s = %s (%d members)" % \
        (item, category.name, len(category.members))
      for idx, (parse, match) in enumerate(zip(category("parse"), output)):
        print "%d. %s" % (idx, ' '.join(parse.signature))
        for span, span_match in zip(parse.spans, match):
          print "  %s = (%s=%s) : %s" % \
            (span.signature, str(list(span.pids)), span.qid, \
             str(span_match))
        print ""
      print ""
      continue

    item = kb[item]

    pids = raw_input("Enter [comma-separated] pid(s):")
    pids = filter(None, pids.replace(' ', '').split(','))
    for pid in pids:
      assert pid in kb, pid
    pids = [kb[p] for p in pids]

    qid = raw_input("Enter qid:")
    assert qid in kb, qid
    qid = kb[qid]

    output = matcher.for_item(item, pids, qid)
    print item, "(" + item.name + ") :", output.name
    print ""
