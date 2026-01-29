# Spider de notícias de comissões temporárias (Comissões Especiais, Externas, CPIs).
# Usa a mesma lógica de parse de notícias que CamaraNoticiasComissoes (duplicada).
from datetime import date, datetime, timedelta
from scrapy.signals import spider_closed
from ...items import articleItem
from scrapy.http import Request
from bs4 import BeautifulSoup
import requests
import locale
import scrapy
import json
import os
from dotenv import load_dotenv
import re
import unicodedata

load_dotenv()

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

now = datetime.now()
timestamp = datetime.timestamp(now)

today = datetime.strptime(date.today().strftime("%d/%m/%Y"), "%d/%m/%Y")
search_limit = datetime.strptime((date.today() - timedelta(days=90)).strftime("%d/%m/%Y"), "%d/%m/%Y")

current_dir = os.path.dirname(os.path.abspath(__file__))
selectors_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "CSS_Selectors", "CamaraNoticiasComissoes.json")

with open(selectors_path) as f:
    search_terms = json.load(f)

site_id = "ab9a6448-3471-491f-9009-d7ec57daba54"

LISTA_COMISSOES_TEMPORARIAS_URL = "https://www.camara.leg.br/comissoes/comissoes-temporarias"


class CamaraNoticiasComissoesTemporariasSpider(scrapy.Spider):
    name = "CamaraNoticiasComissoesTemporarias"
    allowed_domains = ["camara.leg.br", "www2.camara.leg.br"]
    start_urls = []

    data = []
    article_count = 0
    found_old_articles = False
    MAX_ARTICLES_PER_COMMISSION = 50
    # Limite global de notícias (aumentar para deploy oficial; 50 para testes)
    MAX_TOTAL_ARTICLES = 50
    old_articles_count = 0
    MAX_OLD_ARTICLES_BEFORE_STOP = 10
    processed_commissions = set()

    def __init__(self, *args, **kwargs):
        super(CamaraNoticiasComissoesTemporariasSpider, self).__init__(*args, **kwargs)
        self.dept_map = {}
        self.dept_url_map = {}
        self.visited_pagination_urls = {}

    def normalize_text(self, text):
        if not text:
            return ''
        text = unicodedata.normalize('NFKD', text)
        text = ''.join(c for c in text if not unicodedata.combining(c))
        text = text.lower().strip().replace('-', ' ').replace('_', ' ')
        text = re.sub(r'\s+', ' ', text)
        return text

    def create_slug_from_name(self, name):
        if not name:
            return ''
        slug = self.normalize_text(name)
        prefixes = [
            'comissao especial sobre ', 'comissao especial da ', 'comissao especial ',
            'comissao externa sobre ', 'comissao externa ', 'cpi sobre ', 'cpi ',
            'comissao parlamentar de inquérito sobre ', 'comissao parlamentar de inquérito ',
        ]
        for prefix in prefixes:
            if slug.startswith(prefix):
                slug = slug[len(prefix):].strip()
                break
        stop_words = ['a', 'o', 'e', 'de', 'da', 'do', 'em', 'no', 'na', 'para', 'com', 'sobre']
        words = slug.split()
        words = [w for w in words if w not in stop_words and len(w) > 2]
        return ' '.join(words[:7])

    def extract_slug_from_url(self, url):
        if not url:
            return ''
        parts = url.rstrip('/').split('/')
        if parts:
            slug = parts[-1].replace('-', ' ')
            return self.normalize_text(slug)
        return ''

    def find_department_id(self, commission_name, commission_url):
        dept_id = None
        if commission_name:
            name_key = self.normalize_text(commission_name)
            dept_id = self.dept_map.get(name_key)
            if dept_id:
                return dept_id
        if commission_name and not dept_id:
            name_key = self.normalize_text(commission_name)
            for key, d_id in self.dept_map.items():
                if len(name_key) > 10 and len(key) > 10 and (name_key in key or key in name_key):
                    overlap = min(len(name_key), len(key))
                    if overlap > max(len(name_key), len(key)) * 0.7:
                        dept_id = d_id
                        break
        if commission_url and not dept_id:
            url_slug = self.extract_slug_from_url(commission_url)
            if url_slug:
                dept_id = self.dept_url_map.get(url_slug)
                if not dept_id:
                    for slug_key, d_id in self.dept_url_map.items():
                        url_words = set(url_slug.split())
                        slug_words = set(slug_key.split())
                        common = url_words.intersection(slug_words)
                        if len(common) >= 3 or (len(common) > 0 and len(common) >= min(len(url_words), len(slug_words)) * 0.5):
                            dept_id = d_id
                            break
        if commission_name and not dept_id:
            name_slug = self.create_slug_from_name(commission_name)
            if name_slug:
                dept_id = self.dept_url_map.get(name_slug)
        return dept_id

    def extract_b_start(self, url):
        if not url:
            return 0
        match = re.search(r'b_start:int=(\d+)', url)
        return int(match.group(1)) if match else 0

    def get_base_url(self, url):
        if not url:
            return ''
        base = re.sub(r'[?&]b_start:int=\d+', '', url)
        return base.rstrip('?&')

    def start_requests(self):
        yield Request(
            LISTA_COMISSOES_TEMPORARIAS_URL,
            callback=self.map_temporary_commissions,
            priority=10,
            errback=self.handle_error
        )

    def map_temporary_commissions(self, response):
        self.logger.info("Mapeando comissões temporárias...")
        api_url = os.environ.get('API_URL', 'http://localhost:3333')
        if not self.dept_map:
            try:
                all_temp_departments = []
                page = 1
                while True:
                    api_response = requests.get(f"{api_url}/department?type=TEMPORARY&page={page}")
                    if api_response.status_code != 200:
                        break
                    api_data = api_response.json()
                    departments = api_data.get('departments', [])
                    if not departments:
                        break
                    all_temp_departments.extend(departments)
                    total_pages = api_data.get('pages', 1)
                    if page >= total_pages:
                        break
                    page += 1
                for dept in all_temp_departments:
                    dept_id = dept.get('id')
                    dept_name = dept.get('name', '').strip()
                    dept_acronym = dept.get('acronym', '').strip()
                    dept_surname = dept.get('surname', '').strip()
                    if not dept_id:
                        continue
                    if dept_name:
                        name_key = self.normalize_text(dept_name)
                        self.dept_map[name_key] = dept_id
                        for variation in [
                            dept_name.replace('Comissão Especial sobre ', ''),
                            dept_name.replace('Comissão Especial da ', ''),
                            dept_name.replace('Comissão Especial ', ''),
                            dept_name.replace('Comissão Externa sobre ', ''),
                            dept_name.replace('Comissão Externa ', ''),
                            dept_name.replace('CPI - ', ''),
                            dept_name.replace('CPI ', ''),
                        ]:
                            if variation and variation != dept_name:
                                var_key = self.normalize_text(variation)
                                if var_key not in self.dept_map:
                                    self.dept_map[var_key] = dept_id
                    if dept_acronym:
                        self.dept_map[dept_acronym.lower()] = dept_id
                    if dept_surname:
                        self.dept_map[self.normalize_text(dept_surname)] = dept_id
                    if dept_name:
                        slug = self.create_slug_from_name(dept_name)
                        if slug:
                            self.dept_url_map[slug] = dept_id
            except Exception as e:
                self.logger.warning(f"Erro ao buscar comissões da API: {e}")
                self.dept_map = {}
                self.dept_url_map = {}

        selector = search_terms.get('temporary_commission_list', 'ul.l-lista-comissoes li a')
        commission_links = response.css(f'{selector}::attr(href)').getall()
        commission_names = response.css(f'{selector}::text').getall()
        if len(commission_links) == 0:
            xpath_links = response.xpath('//a[contains(@href, "/comissoes/")]/@href').getall()
            xpath_names = response.xpath('//a[contains(@href, "/comissoes/")]/text()').getall()
            commission_links = []
            commission_names = []
            for i, link in enumerate(xpath_links):
                if link:
                    link = link.strip()
                    is_list_page = (
                        link.endswith('/comissoes/comissoes-temporarias') or
                        link.endswith('/comissoes/comissoes-temporarias/') or
                        link == '/comissoes/comissoes-temporarias'
                    )
                    is_valid = (
                        '/comissoes/' in link and not is_list_page and
                        link not in ['/comissoes/', '/comissoes', '#', ''] and
                        len(link.split('/')) > 3
                    )
                    if is_valid and link not in commission_links:
                        commission_links.append(link)
                        commission_names.append(xpath_names[i].strip() if i < len(xpath_names) and xpath_names[i] else '')

        for i, link in enumerate(commission_links):
            if not link:
                continue
            if link.startswith('/'):
                full_url = f"https://www2.camara.leg.br{link}"
            elif link.startswith('http'):
                full_url = link
            else:
                full_url = f"https://www2.camara.leg.br/{link}"
            commission_name = commission_names[i] if i < len(commission_names) else ''
            dept_id = self.find_department_id(commission_name, full_url)
            yield Request(
                full_url,
                callback=self.find_news_link,
                meta={
                    'commission_url': full_url,
                    'commission_name': commission_name,
                    'department_id': dept_id
                },
                priority=8,
                errback=self.handle_error
            )

    def find_news_link(self, response):
        commission_url = response.meta.get('commission_url', '')
        dept_id = response.meta.get('department_id')
        commission_name = response.meta.get('commission_name', '')
        all_news_links = response.xpath('//a[contains(@href, "/noticias")]/@href').getall()
        generic_patterns = ['/noticias', '/noticias/', 'https://www.camara.leg.br/noticias', 'https://www.camara.leg.br/noticias/', 'https://www2.camara.leg.br/noticias', 'https://www2.camara.leg.br/noticias/']
        valid_news_links = [l.strip() for l in all_news_links if l and not (any(p in l for p in generic_patterns) and l.count('/') <= 3) and ('/noticias/' in l or (l.count('/') > 3 and 'comissoes' in l.lower()))]
        news_link = valid_news_links[0] if valid_news_links else None
        if not news_link:
            main_content_links = response.xpath('//main//a[contains(@href, "/noticias")]/@href | //div[@id="main-content"]//a[contains(@href, "/noticias")]/@href').getall()
            for link in main_content_links:
                if link and link.strip() not in ['/noticias', '/noticias/']:
                    news_link = link.strip()
                    break
        if not news_link and '/comissoes-temporarias/' in commission_url:
            news_link = commission_url.rstrip('/') + '/noticias'
        if not news_link:
            self.logger.warning(f"Não foi possível encontrar link de notícias para: {commission_name} ({commission_url})")
            return
        news_link = news_link.strip()
        if news_link.startswith('/'):
            news_url = f"https://www2.camara.leg.br{news_link}"
        elif news_link.startswith('http'):
            news_url = news_link
        else:
            news_url = response.urljoin(news_link)
        if dept_id:
            self.processed_commissions.add(dept_id)
        yield Request(
            news_url,
            callback=self.parse_news_list,
            meta={
                'department_id': dept_id,
                'news_url': news_url,
                'is_temporary': True,
                'commission_name': commission_name,
                'commission_url': commission_url
            },
            priority=6,
            errback=self.handle_error
        )

    def parse_news_list(self, response):
        department_id = response.meta.get('department_id')
        news_url = response.meta.get('news_url', response.url)
        commission_name = response.meta.get('commission_name', '')
        if 'require_login' in response.url or response.status == 302:
            return
        if response.status == 404:
            return
        if not department_id:
            self.logger.info(f"Processando comissão temporária sem department_id: {commission_name}")
        article_selector = search_terms.get('article', 'li.l-lista-noticias__item')
        articles_found = response.css(article_selector)
        articles_count = len(articles_found)
        if articles_count == 0:
            alt_selectors = ['article', '.l-lista-noticias__item', 'ul.l-lista-noticias li', '.noticia-item', 'li[class*="noticia"]', 'a[href*="/noticias/"]']
            for alt_selector in alt_selectors:
                alt_articles = response.css(alt_selector)
                if len(alt_articles) > 0:
                    articles_found = alt_articles
                    articles_count = len(alt_articles)
                    article_selector = alt_selector
                    break
            if articles_count == 0:
                articles_found = response.xpath('//a[contains(@href, "/noticias/") or contains(@href, "/noticia/")]')
                articles_count = len(articles_found)
                article_selector = 'xpath_links'
        for article in articles_found:
            if self.article_count >= self.MAX_TOTAL_ARTICLES:
                return
            if article_selector.startswith('a[') or article_selector == 'xpath_links':
                link = article.css('::attr(href)').get() or article.xpath('./@href').get()
            else:
                link_selector = search_terms.get('link', 'a::attr(href)')
                link = article.css(link_selector).get() or article.xpath('.//a/@href').get()
            if link:
                if link.startswith('/'):
                    link = f"https://www2.camara.leg.br{link}"
                elif not link.startswith('http'):
                    link = response.urljoin(link)
                if '/noticias/' in link:
                    yield Request(link, callback=self.parse_article, meta={'department_id': department_id}, priority=1, errback=self.handle_error)
        next_page = response.css('a[rel="next"]::attr(href)').get()
        if not next_page:
            next_page = response.css('.pagination a.next::attr(href)').get() or response.xpath('//a[contains(text(), "Próxima") or contains(text(), "Próximo")]/@href').get()
        if next_page:
            next_url = response.urljoin(next_page)
            current_b_start = self.extract_b_start(response.url)
            next_b_start = self.extract_b_start(next_url)
            if next_url != response.url and next_b_start > current_b_start:
                commission_key = department_id if department_id else self.get_base_url(response.url)
                if commission_key not in self.visited_pagination_urls:
                    self.visited_pagination_urls[commission_key] = set()
                if next_url not in self.visited_pagination_urls[commission_key]:
                    self.visited_pagination_urls[commission_key].add(next_url)
                    yield Request(next_url, callback=self.parse_news_list, meta=response.meta, priority=5, errback=self.handle_error)

    def handle_error(self, failure):
        request = failure.request
        url = request.url if request else "URL desconhecida"
        if hasattr(failure.value, 'response'):
            response = failure.value.response
            self.logger.error(f"Erro HTTP {response.status if response else 'N/A'} ao processar: {url}")
        else:
            self.logger.error(f"Erro ao processar: {url} - {failure.value}")

    def parse_article(self, response):
        department_id = response.meta.get('department_id')
        if response.status != 200:
            return
        updated_selector = search_terms.get('updated', 'p.g-artigo__data-hora::text')
        updated = response.css(updated_selector).get()
        if not updated:
            for alt in ['p.g-artigo__data-hora::text', '.g-artigo__data-hora::text', '[class*="data-hora"]::text', 'time::attr(datetime)', '.data::text']:
                candidate = response.css(alt).get()
                if candidate and (re.search(r'\d{2}/\d{2}/\d{4}', candidate) or re.search(r'\d{4}-\d{2}-\d{2}', candidate)):
                    updated = candidate
                    break
        if not updated and response.text:
            date_match = re.search(r'\b(\d{2})/(\d{2})/(\d{4})\b', response.text)
            if date_match:
                updated = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            if not updated:
                iso_match = re.search(r'\b(\d{4})-(\d{2})-(\d{2})\b', response.text)
                if iso_match:
                    updated = f"{iso_match.group(3)}-{iso_match.group(2)}-{iso_match.group(1)}"
        if not updated:
            return
        try:
            updated = updated.strip()
            first_part = updated.split(" ")[0].split("T")[0]
            if '/' in first_part:
                date_str = first_part.replace("/", "-")
                updated = datetime.strptime(date_str, "%d-%m-%Y")
            elif re.match(r'\d{4}-\d{2}-\d{2}', first_part):
                updated = datetime.strptime(first_part[:10], "%Y-%m-%d")
            else:
                updated = datetime.strptime(first_part, "%d-%m-%Y")
        except (ValueError, Exception):
            return
        title_selector = search_terms.get('title', 'h1.g-artigo__titulo::text')
        title = response.css(title_selector).get()
        if not title:
            title = response.css('h1::text').get() or response.css('title::text').get()
        if not title:
            title = "Sem título"
        content_selector = search_terms.get('content', 'div.js-article-read-more')
        content = response.css(content_selector).getall()
        if not content:
            for alt in ['article p', 'main p', 'main article p', '[class*="artigo"] p', '[class*="conteudo"]', '.g-artigo p', 'main', 'article']:
                content = response.css(alt).getall()
                if content:
                    break
        if not content:
            content = [""]
        try:
            content = BeautifulSoup(" ".join(content), "html.parser").text.replace("\n", " ").strip()
        except Exception:
            content = ""
        if self.article_count >= self.MAX_TOTAL_ARTICLES:
            return
        if search_limit <= updated <= today:
            self.old_articles_count = 0
            item = articleItem(updated=updated, title=title, content=content, link=response.url, departmentId=department_id)
            yield item
            self.data.append(item)
            self.article_count += 1
        else:
            if updated < search_limit:
                self.old_articles_count += 1

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraNoticiasComissoesTemporariasSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider

    def upload_data(self, spider):
        results_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "Results")
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
        file_path = os.path.join(results_dir, f"{self.name}_{timestamp}.json")
        if not os.path.isfile(file_path):
            with open(file_path, "w") as f:
                json.dump([], f)
        with open(file_path, "r") as f:
            file_data = json.load(f)
        file_data.extend([item.to_dict() for item in self.data])
        with open(file_path, "w", encoding='utf-8') as f:
            json.dump(file_data, f, ensure_ascii=False)
        api_url = os.environ.get('API_URL')
        if not api_url:
            spider.logger.error("API_URL environment variable is not set. Skipping upload.")
            return
        try:
            spider.logger.info(f"Uploading {len(file_data)} records to {api_url}/news/scrape?type=PARLIAMENT")
            upload = requests.post(f"{api_url}/news/scrape?type=PARLIAMENT", json={"records": file_data})
            if upload.status_code >= 200 and upload.status_code < 300:
                print("upload success: ", upload.text)
            else:
                spider.logger.error(f"Upload failed: {upload.status_code} - {upload.text}")
        except Exception as e:
            spider.logger.error(f"Error during upload: {str(e)}")
