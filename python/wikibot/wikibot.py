import pywikibot
import sling
import json
import sys
import datetime

precision_map = {
  sling.MILLENNIUM: pywikibot.WbTime.PRECISION['millenia'],
  sling.CENTURY: pywikibot.WbTime.PRECISION['century'],
  sling.DECADE: pywikibot.WbTime.PRECISION['decade'],
  sling.YEAR: pywikibot.WbTime.PRECISION['year'],
  sling.MONTH: pywikibot.WbTime.PRECISION['month'],
  sling.DAY: pywikibot.WbTime.PRECISION['day']
}

class StoreFactsBot:
  def __init__(self, test=True):
    self.site = pywikibot.Site("wikidata", "wikidata")
    self.repo = self.site.data_repository()

    time_str = datetime.datetime.now().isoformat("-")[:19].replace(":","-")
    if test:
      record_file_name = "local/data/e/wikibot/test-birth-dates.rec"
      time_str = "test-" + time_str
    else:
      record_file_name = "local/data/e/wikibot/birth-dates.rec"
    status_file_name = "local/data/e/wikibot/wikibotlog-"+time_str+".rec"

    try:
      self.record_file = sling.RecordReader(record_file_name)
      self.status_file = sling.RecordWriter(status_file_name)
    except :
      print "Can't open Sling record files", record_file_name, status_file_name
      sys.exit()

    self.store = sling.Store()
    self.n_item = self.store["item"]
    self.n_facts = self.store["facts"]
    self.n_date_of_birth = self.store["P569"]
    self.n_provenance = self.store["provenance"]
    self.n_category = self.store["category"]
    self.n_method = self.store["method"]
    self.n_status = self.store["status"]
    self.n_stored = self.store["stored"]
    self.n_skipped = self.store["skipped"]
    self.n_error = self.store["error"]
    self.n_has_prop = self.store["already has property"]
    self.n_had_prop = self.store["already had property"]
    self.n_incons_input = self.store["inconsistent input"]
    self.n_redirect = self.store["is a redirect page"]
    self.store.freeze()

    self.source_claim = pywikibot.Claim(self.repo, "P3452") # Inferred from
    self.time_claim = pywikibot.Claim(self.repo, "P813") # Referenced (on)
    today = datetime.date.today()
    time_target = pywikibot.WbTime(year=today.year,
                                   month=today.month,
                                   day=today.day)
    self.time_claim.setTarget(time_target)

  def getSources(self, category):
    source_target = pywikibot.ItemPage(self.repo, category)
    self.source_claim.setTarget(source_target)
    return [self.source_claim, self.time_claim]

  def everHadProp(self, wd_item, prop):
    # up to 150 revisions covers the full history of 99% of e.g. human items
    revisions = wd_item.revisions(total=150, content=True)
    for revision in revisions:
      try:
        revision_text = json.loads(revision.text)
        claims = revision_text['claims']
      except:
        pass # unable to extract claims - move to next revision
      else:
        if prop in claims: return True
    return False

  def storeRecords(self, records, batch_size=3):
    rs = sling.Store(self.store)
    updated = 0
    for item, record in records:
      if updated >= batch_size:
        print "Hit batch size of", batch_size
        break
      print "Processing", item
      wd_item = pywikibot.ItemPage(self.repo, item)
      if wd_item.isRedirectPage():
        print item, " is a redirect page"
        status_record = rs.frame({
          self.n_item: fact_record[self.n_item],
          self.n_facts: fact_record[self.n_facts],
          self.n_status: rs.frame({self.n_skipped: self.n_redirect})
        })
        self.status_file.write(item, status_record.data(binary=True))
        continue
      wd_claims = wd_item.get().get('claims')
      fact_record = rs.parse(record)
      if rs[item] != fact_record[self.n_item]:
        print "Inconsistent key & item input: ", item, fact_record[self.n_item]
        status_record = rs.frame({
          self.n_item: fact_record[self.n_item],
          self.n_facts: fact_record[self.n_facts],
          self.n_status: rs.frame({self.n_skipped: self.n_incons_input})
        })
        self.status_file.write(item, status_record.data(binary=True))
        continue # read next record in the file
      provenance = fact_record[self.n_provenance]
      # Process facts / claims
      facts = fact_record[self.n_facts]
      for prop, val in facts:
        prop_str = str(prop)
        if prop_str in wd_claims:
          print item, "already has property", prop
          status_record = rs.frame({
            self.n_item: fact_record[self.n_item],
            self.n_facts: rs.frame({prop: val}),
            self.n_status: rs.frame({self.n_skipped: self.n_has_prop})
          })
          self.status_file.write(item, status_record.data(binary=True))
          continue
        if self.everHadProp(wd_item, prop_str):
          print item, "previously had property", prop
          status_record = rs.frame({
            self.n_item: fact_record[self.n_item],
            self.n_facts: rs.frame({prop: val}),
            self.n_status: rs.frame({self.n_skipped: self.n_had_prop})
          })
          self.status_file.write(item, status_record.data(binary=True))
          continue
        claim = pywikibot.Claim(self.repo, prop_str)
        if claim.type == "time":
          date = sling.Date(val) #parse date from record
          precision = precision_map[date.precision] # sling to wikidata
          target = pywikibot.WbTime(year=date.year, precision=precision)
        elif claim.type == 'wikibase-item':
          target = pywikibot.ItemPage(self.repo, val)
        else:
          # TODO add location and possibly other types
          print "Error: Unknown claim type", claim.type
          continue
        claim.setTarget(target)
        cat_str = str(provenance[self.n_category])
        summary = provenance[self.n_method] + " " + cat_str
        wd_item.addClaim(claim, summary=summary)
        claim.addSources(self.getSources(cat_str))
        rev_id = wd_item.latest_revision_id
        status_record = rs.frame({
          self.n_item: fact_record[self.n_item],
          self.n_facts: rs.frame({prop: val}),
          self.n_status: rs.frame({self.n_stored: str(rev_id)})
        })
        self.status_file.write(item, status_record.data(binary=True))

      updated += 1
      print item
    print "Last record.", updated, "records updated."
    self.status_file.close()
    self.record_file.close()

  def run(self):
    self.storeRecords(self.record_file, batch_size=2)

sfb = StoreFactsBot(test=True)
sfb.run()

