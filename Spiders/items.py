# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class articleItem(Item):
    title = Field()
    updated = Field()
    content = Field()
    link = Field()
    
    def to_dict(self):
        return {
            'updated': self['updated'].strftime("%d/%m/%Y"),
            'title': self['title'],
            'content': self['content'],
            'link': self['link'],
        }
        
        
class cabinetItem(Item):
    politicianId = Field()
    year = Field()
    name = Field()
    group = Field()
    role = Field()
    period = Field()
    monthly = Field()
    
    def to_dict(self):
        return {
            'politicianId': self['politicianId'],
            'year': self['year'],
            'name': self['name'],
            'group': self['group'],
            'role': self['role'],
            'period': self['period'],
            'monthly': self['monthly'],
        }

class generalItem(Item):
    politicianId = Field()
    year = Field()
    usedParliamentaryQuota = Field()
    unusedParliamentaryQuota = Field()
    usedCabinetQuota = Field()
    unusedCabinetQuota = Field()
    janUsedParliamentaryQuota = Field()
    janCabinetQuota = Field()
    febUsedParliamentaryQuota = Field()
    febCabinetQuota = Field()
    marUsedParliamentaryQuota = Field()
    marCabinetQuota = Field()
    aprUsedParliamentaryQuota = Field()
    aprCabinetQuota = Field()
    mayUsedParliamentaryQuota = Field()
    mayCabinetQuota = Field()
    junUsedParliamentaryQuota = Field()
    junCabinetQuota = Field()
    julUsedParliamentaryQuota = Field()
    julCabinetQuota = Field()
    augUsedParliamentaryQuota = Field()
    augCabinetQuota = Field()
    sepUsedParliamentaryQuota = Field()
    sepCabinetQuota = Field()
    octUsedParliamentaryQuota = Field()
    octCabinetQuota = Field()
    novUsedParliamentaryQuota = Field()
    novCabinetQuota = Field()
    decUsedParliamentaryQuota = Field()
    decCabinetQuota = Field()
    def to_dict(self):
        return {
            'politicianId': self['politicianId'],
            'year': self['year'],
            'usedParliamentaryQuota': self['usedParliamentaryQuota'],
            'unusedParliamentaryQuota': self['unusedParliamentaryQuota'],
            'usedCabinetQuota': self['usedCabinetQuota'],
            'unusedCabinetQuota': self['unusedCabinetQuota'],
            'janUsedParliamentaryQuota': self['janUsedParliamentaryQuota'],
            'janCabinetQuota': self['janCabinetQuota'],
            'febUsedParliamentaryQuota': self['febUsedParliamentaryQuota'],
            'febCabinetQuota': self['febCabinetQuota'],
            'marUsedParliamentaryQuota': self['marUsedParliamentaryQuota'],
            'marCabinetQuota': self['marCabinetQuota'],
            'aprUsedParliamentaryQuota': self['aprUsedParliamentaryQuota'],
            'aprCabinetQuota': self['aprCabinetQuota'],
            'mayUsedParliamentaryQuota': self['mayUsedParliamentaryQuota'],
            'mayCabinetQuota': self['mayCabinetQuota'],
            'junUsedParliamentaryQuota': self['junUsedParliamentaryQuota'],
            'junCabinetQuota': self['junCabinetQuota'],
            'julUsedParliamentaryQuota': self['julUsedParliamentaryQuota'],
            'julCabinetQuota': self['julCabinetQuota'],
            'augUsedParliamentaryQuota': self['augUsedParliamentaryQuota'],
            'augCabinetQuota': self['augCabinetQuota'],
            'sepUsedParliamentaryQuota': self['sepUsedParliamentaryQuota'],
            'sepCabinetQuota': self['sepCabinetQuota'],
            'octUsedParliamentaryQuota': self['octUsedParliamentaryQuota'],
            'octCabinetQuota': self['octCabinetQuota'],
            'novUsedParliamentaryQuota': self['novUsedParliamentaryQuota'],
            'novCabinetQuota': self['novCabinetQuota'],
            'decUsedParliamentaryQuota': self['decUsedParliamentaryQuota'],
            'decCabinetQuota': self['decCabinetQuota'],
        }

class otherItem(Item):
    politicianId = Field()
    year = Field()
    contractedPeople = Field()
    grossSalary = Field()
    functionalPropertyUsage = Field()
    housingAssistant = Field()
    trips = Field()
    diplomaticPassport = Field()

    def to_dict(self):
        return {
            'politicianId': self['politicianId'],
            'year': self['year'],
            'contractedPeople': self['contractedPeople'],
            'grossSalary': self['grossSalary'],
            'functionalPropertyUsage': self['functionalPropertyUsage'],
            'housingAssistant': self['housingAssistant'],
            'trips': self['trips'],
            'diplomaticPassport': self['diplomaticPassport']
            }

class politicsItem(Item):
    politicianId = Field()
    year = Field()
    createdProposals = Field()
    createdProposalsUrl = Field()
    relatedProposals = Field()
    relatedProposalsUrl = Field()
    rollCallVotes = Field()
    rollCallVotesUrl = Field()
    speeches = Field()
    speechesUrl = Field()
    speechesVideosUrl = Field()
    speechesAudiosUrl = Field()
    plenaryPresence = Field()
    plenaryJustifiedAbsences = Field()
    plenaryUnjustifiedAbsences = Field()
    committeesPresence = Field()
    committeesJustifiedAbsences = Field()
    committeesUnjustifiedAbsences = Field()
    commissions = Field()

    def to_dict(self):
        return {
            'politicianId': self['politicianId'],
            'year': self['year'],
            'createdProposals': self['createdProposals'],
            'createdProposalsUrl': self['createdProposalsUrl'],
            'relatedProposals': self['relatedProposals'],
            'relatedProposalsUrl': self['relatedProposalsUrl'],
            'rollCallVotes': self['rollCallVotes'],
            'rollCallVotesUrl': self['rollCallVotesUrl'],
            'speeches': self['speeches'],
            'speeches': self['speeches'],
            'speechesVideosUrl': self['speechesVideosUrl'],
            'speechesAudiosUrl': self['speechesAudiosUrl'],
            'plenaryPresence': self['plenaryPresence'],
            'plenaryJustifiedAbsences': self['plenaryJustifiedAbsences'],
            'plenaryUnjustifiedAbsences': self['plenaryUnjustifiedAbsences'],
            'committeesPresence': self['committeesPresence'],
            'committeesJustifiedAbsences': self['committeesJustifiedAbsences'],
            'committeesUnjustifiedAbsences': self['committeesUnjustifiedAbsences'],
            'commissions': self['commissions']
            }

class roleItem(Item):
    politicianId = Field()
    year = Field()
    name = Field()
    description = Field()
    date = Field()
    
    def to_dict(self):
        return {
            'politicianId': self['politicianId'],
            'year': self['year'],
            'name': self['name'],
            'description': self['description'],
            'date': self['date']
            }
