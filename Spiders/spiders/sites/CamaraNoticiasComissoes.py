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

load_dotenv()

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

now = datetime.now()
timestamp = datetime.timestamp(now)

today = datetime.strptime(date.today().strftime("%d/%m/%Y"), "%d/%m/%Y")
search_limit = datetime.strptime((date.today() - timedelta(days=1)).strftime("%d/%m/%Y"), "%d/%m/%Y")

# Resolving relative path for CSS Selectors
current_dir = os.path.dirname(os.path.abspath(__file__))
selectors_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "CSS_Selectors", "CamaraNoticiasComissoes.json")

with open(selectors_path) as f:
    search_terms = json.load(f)

site_id = "ab9a6448-3471-491f-9009-d7ec57daba54"

class CamaraNoticiasComissoesSpider(scrapy.Spider):
    name = "CamaraNoticiasComissoes"
    allowed_domains = ["camara.leg.br", "www2.camara.leg.br"]
    
    # URLs serão definidas dinamicamente
    start_urls = []
    
    INCREMENT = 1
    data = []
    article_count = 0
    found_old_articles = False
    MAX_ARTICLES_PER_COMMISSION = 50
    
    # Mapeamento de comissões temporárias (ID -> URL de notícias)
    temporary_commissions_map = {}
    
    def __init__(self, department_ids=None, commission_type=None, *args, **kwargs):
        super(CamaraNoticiasComissoesSpider, self).__init__(*args, **kwargs)
        self.department_ids = department_ids.split(',') if department_ids else None
        self.commission_type = commission_type
        self.processed_commissions = set()
        
    def start_requests(self):
        """
        Inicia o processo de scraping:
        1. Se há comissões temporárias, primeiro mapeia suas URLs
        2. Depois processa todas as comissões (permanentes e temporárias)
        """
        # Primeiro, buscar comissões temporárias se necessário
        if not self.commission_type or self.commission_type == 'TEMPORARY':
            yield Request(
                'https://www.camara.leg.br/comissoes/comissoes-temporarias',
                callback=self.map_temporary_commissions,
                priority=10
            )
        
        # Buscar comissões do banco via API ou processar diretamente
        # Por enquanto, vamos processar via parâmetros ou buscar todas
        api_url = os.environ.get('API_URL', 'http://localhost:3333')
        
        # Buscar comissões da API
        try:
            params = {}
            if self.commission_type:
                params['type'] = self.commission_type
            if self.department_ids:
                # Se IDs específicos foram fornecidos, buscar cada um
                for dept_id in self.department_ids:
                    dept_url = f"{api_url}/department/{dept_id}"
                    yield Request(
                        dept_url,
                        callback=self.process_department,
                        meta={'department_id': dept_id},
                        priority=5
                    )
            else:
                # Buscar todas as comissões do tipo especificado
                type_param = f"?type={self.commission_type}" if self.commission_type else ""
                yield Request(
                    f"{api_url}/department{type_param}",
                    callback=self.fetch_all_departments,
                    priority=5
                )
        except Exception as e:
            self.logger.error(f"Erro ao buscar comissões: {e}")
    
    def fetch_all_departments(self, response):
        """Busca todas as comissões e processa cada uma"""
        try:
            data = json.loads(response.text)
            departments = data.get('departments', [])
            
            for dept in departments:
                dept_id = dept.get('id')
                if dept_id and dept_id not in self.processed_commissions:
                    self.processed_commissions.add(dept_id)
                    yield Request(
                        response.url.replace('/department', f'/department/{dept_id}'),
                        callback=self.process_department,
                        meta={'department_id': dept_id, 'department': dept},
                        priority=5
                    )
        except Exception as e:
            self.logger.error(f"Erro ao processar lista de comissões: {e}")
    
    def map_temporary_commissions(self, response):
        """
        Mapeia comissões temporárias da página de lista
        Extrai links e tenta encontrar seção de notícias para cada uma
        """
        self.logger.info("Mapeando comissões temporárias...")
        
        # Extrair links das comissões (especiais, externas, CPIs)
        commission_links = response.css(search_terms.get('temporary_commission_list', 'a::attr(href)')).getall()
        
        for link in commission_links:
            if link and link.startswith('/'):
                full_url = f"https://www2.camara.leg.br{link}"
                # Navegar até a página da comissão para encontrar link de notícias
                yield Request(
                    full_url,
                    callback=self.find_news_link,
                    meta={'commission_url': full_url},
                    priority=8
                )
    
    def find_news_link(self, response):
        """
        Encontra o link para a seção de notícias de uma comissão temporária
        """
        commission_url = response.meta.get('commission_url', '')
        
        # Procurar link para /noticias na página
        news_link = response.css(search_terms.get('commission_news_link', 'a[href*="/noticias"]::attr(href)')).get()
        
        if news_link:
            if news_link.startswith('/'):
                news_url = f"https://www2.camara.leg.br{news_link}"
            else:
                news_url = news_link
            
            # Tentar extrair department_id da URL ou nome da comissão
            # Por enquanto, vamos usar a URL como identificador temporário
            # O department_id será passado via meta quando disponível
            
            self.logger.info(f"Encontrado link de notícias: {news_url}")
            
            # Processar notícias desta comissão
            yield Request(
                news_url,
                callback=self.parse_news_list,
                meta={
                    'department_id': response.meta.get('department_id'),
                    'news_url': news_url,
                    'is_temporary': True
                },
                priority=6
            )
        else:
            self.logger.warning(f"Não foi encontrado link de notícias para: {commission_url}")
    
    def process_department(self, response):
        """
        Processa uma comissão específica
        Determina se é permanente ou temporária e constrói URL apropriada
        """
        try:
            dept_data = json.loads(response.text)
            dept_id = response.meta.get('department_id') or dept_data.get('id')
            dept_type = dept_data.get('type', '')
            acronym = dept_data.get('acronym', '')
            
            if not dept_id:
                self.logger.warning("Department ID não encontrado")
                return
            
            if dept_id in self.processed_commissions:
                return
            self.processed_commissions.add(dept_id)
            
            # Determinar URL de notícias baseado no tipo
            if dept_type == 'Comissão Permanente' and acronym:
                # Comissão permanente: URL fixa
                news_url = f"https://www2.camara.leg.br/atividade-legislativa/comissoes/comissoes-permanentes/{acronym.lower()}/noticias"
                self.logger.info(f"Processando comissão permanente {acronym}: {news_url}")
                
                yield Request(
                    news_url,
                    callback=self.parse_news_list,
                    meta={
                        'department_id': dept_id,
                        'news_url': news_url,
                        'is_temporary': False
                    },
                    priority=7
                )
            elif dept_type in ['Comissão Especial', 'Comissão Externa', 'Comissão Parlamentar de Inquérito']:
                # Comissão temporária: precisa mapear primeiro
                # Se já temos o mapeamento, usar; senão, pular (será processado no map_temporary_commissions)
                self.logger.info(f"Comissão temporária {dept_id} - será processada via mapeamento")
            else:
                self.logger.warning(f"Tipo de comissão não suportado: {dept_type}")
                
        except Exception as e:
            self.logger.error(f"Erro ao processar comissão: {e}")
    
    def parse_news_list(self, response):
        """
        Parseia a lista de notícias de uma comissão
        Similar ao parse do scraper original, mas com departmentId
        """
        department_id = response.meta.get('department_id')
        if not department_id:
            self.logger.warning("Department ID não encontrado no meta")
            return
        
        articles_in_timeframe = 0
        
        for article in response.css(search_terms['article']):
            if self.article_count >= self.MAX_ARTICLES_PER_COMMISSION * len(self.processed_commissions):
                break
            
            link = article.css(search_terms['link']).get()
            if link:
                # Garantir URL absoluta
                if link.startswith('/'):
                    link = f"https://www2.camara.leg.br{link}"
                elif not link.startswith('http'):
                    link = response.urljoin(link)
                
                articles_in_timeframe += 1
                yield Request(
                    link,
                    callback=self.parse_article,
                    meta={'department_id': department_id},
                    priority=1
                )
        
        if articles_in_timeframe == 0:
            self.found_old_articles = True
            return
        
        # Tentar próxima página se existir
        next_page = response.css('a[rel="next"]::attr(href)').get()
        if next_page:
            next_url = response.urljoin(next_page)
            yield Request(
                next_url,
                callback=self.parse_news_list,
                meta=response.meta,
                priority=5
            )
    
    def parse_article(self, response):
        """
        Parseia um artigo individual
        Similar ao scraper original, mas inclui departmentId
        """
        department_id = response.meta.get('department_id')
        
        updated = response.css(search_terms['updated']).get()
        if not updated:
            return
        
        updated = updated.strip()
        updated = updated.split(" ")[0]
        updated = updated.replace("/", "-")
        
        try:
            updated = datetime.strptime(updated, "%d-%m-%Y")
        except ValueError:
            self.logger.warning(f"Formato de data inválido: {updated}")
            return
        
        title = response.css(search_terms['title']).get()
        content = response.css(search_terms['content']).getall()
        content = BeautifulSoup(" ".join(content), "html.parser").text
        content = content.replace("\n", " ")
        
        if search_limit <= updated <= today:
            item = articleItem(
                updated=updated,
                title=title,
                content=content,
                link=response.url,
                departmentId=department_id,  # Novo campo
            )
            yield item
            self.data.append(item)
            self.article_count += 1
        else:
            self.found_old_articles = True
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraNoticiasComissoesSpider, cls).from_crawler(crawler, *args, **kwargs)
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
        
        data_dicts = [item.to_dict() for item in self.data]
        
        file_data.extend(data_dicts)
        
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
