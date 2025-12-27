from scrapy.signals import spider_closed
from ...items import politicsItem
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import scrapy
import json
import os

now = datetime.now()
timestamp = datetime.timestamp(now)

year = os.environ['YEAR']

class CamaraPoliticsSpider(scrapy.Spider):
    name = "CamaraPolitics"
    allowed_domains = ["camara.leg.br"]
    data = []

    def start_requests(self):
        # Make the API request here
        self.logger.info(f"Fetching politician IDs from {os.environ['API_URL']}/politician/ids")
        request = requests.get(f"{os.environ['API_URL']}/politician/ids")
        response_data = request.json()
        ids = response_data['ids']  # Extract the array from the 'ids' key
        self.logger.info(f"Successfully fetched {len(ids)} politician IDs")

        # Generate URLs and create requests
        for politician_id in ids:
            url = f"https://www.camara.leg.br/deputados/{politician_id}?ano={year}"
            yield scrapy.Request(url=url, callback=self.parse, meta={'politician_id': politician_id})

    def parse(self, response):
        politician_id = response.meta['politician_id']   # Get all table rows
        # self.logger.info(f"Parsing profile for Politician ID: {politician_id}")
        info = {
            'politicianId': None,
            'year': None,
            'createdProposals': None,
            'createdProposalsUrl': None,
            'relatedProposals': None,
            'relatedProposalsUrl': None,
            'rollCallVotes': None,
            'rollCallVotesUrl': None,
            'speeches': None,
            'speechesVideosUrl': None,
            'speechesAudiosUrl': None,
            'plenaryPresence': None,
            'plenaryJustifiedAbsences': None,
            'plenaryUnjustifiedAbsences': None,
            'committeesPresence': None,
            'committeesJustifiedAbsences': None,
            'committeesUnjustifiedAbsences': None,
            'commissions': None
        }

        cards = response.css("div.l-cards-atuacao__item").getall()
        if len(cards) == 0:
            self.logger.warning(f"No activity cards found for Politician ID: {politician_id}. Saving empty profile.")
            info['politicianId'] = politician_id
            info['year'] = year
            item = politicsItem(**info)
            yield item
            self.data.append(item)
            return 

        card1 = cards[0]
        soup1 = BeautifulSoup(card1, "html.parser")
        proposals = soup1.findAll('li', class_='atuacao__item')
        createdProposals = proposals[0]
        createdProposalsUrl = createdProposals.find('a')
        if createdProposalsUrl is not None:
            createdProposalsUrl = createdProposalsUrl['href']
        createdProposals = createdProposals.text.strip()
        createdProposals = createdProposals.split("\n")[1]
        relatedProposals = proposals[1]
        relatedProposalsUrl = relatedProposals.find('a')
        if relatedProposalsUrl is not None:
            relatedProposalsUrl = relatedProposalsUrl['href']
        relatedProposals = relatedProposals.text.strip()
        relatedProposals = relatedProposals.split("\n")[1]

        card2 = cards[1]
        soup2 = BeautifulSoup(card2, "html.parser")
        rollCallVotes = soup2.find('div', class_='atuacao__item')
        rollCallVotesUrl = rollCallVotes.find('a')
        if rollCallVotesUrl is not None:
            rollCallVotesUrl = rollCallVotesUrl['href']
        rollCallVotes = rollCallVotes.text.strip()
        rollCallVotes = rollCallVotes.split("\n")[1]

        card3 = cards[2]
        soup3 = BeautifulSoup(card3, "html.parser")
        speeches = soup3.find('div', class_='atuacao__item')
        speechesUrl = speeches.find('a')
        if speechesUrl is not None:
            speechesUrl = speechesUrl['href']
        additionalLinks = speeches.find('ul', class_='atuacao__links-adicionais')
        if additionalLinks is not None:
            speechesUrl = additionalLinks.findAll('a')
            speechesUrl = [a.get('href') for a in speechesUrl]
            for url in speechesUrl:
                if url is not None:
                    if 'videos' in url:
                        speechesVideosUrl = url
                    elif 'audio' in url:
                        speechesAudiosUrl = url
        speeches = speeches.text.strip()
        speeches = speeches.split("\n")[1]

        presences = response.css("div.presencas__content section ul.presencas__subsection-content").getall()
        plenaryPresences = presences[0]
        plenaryPresencesSoup = BeautifulSoup(plenaryPresences, "html.parser")
        plenaryPresences = plenaryPresencesSoup.findAll('li')
        plenaryPresence = plenaryPresences[0]
        plenaryPresence = plenaryPresence.text.strip()
        plenaryPresence = plenaryPresence.split("\n")[1]
        plenaryPresence = plenaryPresence.strip()
        plenaryJustifiedAbsences = plenaryPresences[1]
        plenaryJustifiedAbsences = plenaryJustifiedAbsences.text.strip()
        plenaryJustifiedAbsences = plenaryJustifiedAbsences.split("\n")[1]
        plenaryJustifiedAbsences = plenaryJustifiedAbsences.strip()
        plenaryUnjustifiedAbsences = plenaryPresences[2]
        plenaryUnjustifiedAbsences = plenaryUnjustifiedAbsences.text.strip()
        plenaryUnjustifiedAbsences = plenaryUnjustifiedAbsences.split("\n")[1]
        plenaryUnjustifiedAbsences = plenaryUnjustifiedAbsences.strip()

        committeesPresences = presences[1]
        committeesPresencesSoup = BeautifulSoup(committeesPresences, "html.parser")
        committeesPresences = committeesPresencesSoup.findAll('li')
        committeesPresence = committeesPresences[0]
        committeesPresence = committeesPresence.text.strip()
        committeesPresence = committeesPresence.split("\n")[1]
        committeesPresence = committeesPresence.strip()
        committeesJustifiedAbsences = committeesPresences[1]
        committeesJustifiedAbsences = committeesJustifiedAbsences.text.strip()
        committeesJustifiedAbsences = committeesJustifiedAbsences.split("\n")[1]
        committeesJustifiedAbsences = committeesJustifiedAbsences.strip()
        committeesUnjustifiedAbsences = committeesPresences[2]
        committeesUnjustifiedAbsences = committeesUnjustifiedAbsences.text.strip()
        committeesUnjustifiedAbsences = committeesUnjustifiedAbsences.split("\n")[1]
        committeesUnjustifiedAbsences = committeesUnjustifiedAbsences.strip()

        commissions = response.css('ul.titular-comissoes__lista').get()
        commissions = BeautifulSoup(commissions, "html.parser")
        commissions = BeautifulSoup(" ".join(commissions.findAll(text=True)), "html.parser").text
        commissions = commissions.replace('\n', '')
        commissions = commissions.strip()

        info['politicianId'] = politician_id
        info['year'] = year
        info['createdProposals'] = createdProposals
        info['createdProposalsUrl'] = createdProposalsUrl
        info['relatedProposals'] = relatedProposals
        info['relatedProposalsUrl'] = relatedProposalsUrl
        info['rollCallVotes'] = rollCallVotes
        info['rollCallVotesUrl'] = rollCallVotesUrl
        info['speeches'] = speeches
        info['speechesUrl'] = speechesUrl
        info['speechesVideosUrl'] = speechesVideosUrl
        info['speechesAudiosUrl'] = speechesAudiosUrl
        info['plenaryPresence'] = plenaryPresence
        info['plenaryJustifiedAbsences'] = plenaryJustifiedAbsences
        info['plenaryUnjustifiedAbsences'] = plenaryUnjustifiedAbsences
        info['committeesPresence'] = committeesPresence
        info['committeesJustifiedAbsences'] = committeesJustifiedAbsences
        info['committeesUnjustifiedAbsences'] = committeesUnjustifiedAbsences
        info['commissions'] = commissions

        item = politicsItem(**info)
        yield item
        self.data.append(item)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraPoliticsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider

    def upload_data(self, spider):
        file_path = f"/home/scrapeops/legisai-scrape/Spiders/Results/{self.name}_{timestamp}.json"
        if not os.path.isfile(file_path):
            with open(file_path, "w") as f:
                json.dump([], f)

        with open(file_path, "r") as f:
            file_data = json.load(f)

        data_dicts = [item.to_dict() for item in self.data]
        file_data.extend(data_dicts)

        with open(file_path, "w") as f:
            json.dump(file_data, f, ensure_ascii=False)

        spider.logger.info(f"Uploading {len(file_data)} records to API at {os.environ['API_URL']}/politician-profile")
        response = requests.post(f"{os.environ['API_URL']}/politician-profile", json={"records": file_data})
        if response.status_code >= 200 and response.status_code < 300:
            spider.logger.info(f"Upload successful: {response.status_code} - {response.text}")
        else:
            spider.logger.error(f"Upload failed: {response.status_code} - {response.text}")