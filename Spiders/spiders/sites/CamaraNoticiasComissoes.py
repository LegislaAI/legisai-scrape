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
# Ampliar timerange para 90 dias (comissões não são tão frequentes)
# Isso permite coletar notícias mais antigas que ainda são relevantes
search_limit = datetime.strptime((date.today() - timedelta(days=90)).strftime("%d/%m/%Y"), "%d/%m/%Y")

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
    old_articles_count = 0  # Contador de artigos antigos consecutivos
    MAX_OLD_ARTICLES_BEFORE_STOP = 10  # Parar após 10 artigos antigos consecutivos
    
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
        1. Busca comissões da API usando requests (não Scrapy)
        2. Se há comissões temporárias, primeiro mapeia suas URLs
        3. Depois processa todas as comissões (permanentes e temporárias)
        """
        api_url = os.environ.get('API_URL', 'http://localhost:3333')
        
        # Buscar comissões da API usando requests (não Scrapy)
        try:
            if self.department_ids:
                # Se IDs específicos foram fornecidos, buscar cada um
                for dept_id in self.department_ids:
                    dept_url = f"{api_url}/department/{dept_id}"
                    try:
                        response = requests.get(dept_url)
                        if response.status_code == 200:
                            dept_data = response.json()
                            # Processar e criar requisição Scrapy
                            req = self.process_department_from_data(dept_data, dept_id)
                            if req:
                                yield req
                    except Exception as e:
                        self.logger.error(f"Erro ao buscar comissão {dept_id}: {e}")
            else:
                # Buscar todas as comissões do tipo especificado
                type_param = f"?type={self.commission_type}" if self.commission_type else ""
                api_endpoint = f"{api_url}/department{type_param}"
                
                self.logger.info(f"Buscando comissões de: {api_endpoint}")
                response = requests.get(api_endpoint)
                
                if response.status_code == 200:
                    data = response.json()
                    departments = data.get('departments', [])
                    self.logger.info(f"Encontradas {len(departments)} comissões")
                    
                    for dept in departments:
                        dept_id = dept.get('id')
                        if dept_id and dept_id not in self.processed_commissions:
                            self.processed_commissions.add(dept_id)
                            # Processar e criar requisição Scrapy
                            req = self.process_department_from_data(dept, dept_id)
                            if req:
                                yield req
                else:
                    self.logger.error(f"Erro ao buscar comissões: {response.status_code}")
        except Exception as e:
            self.logger.error(f"Erro ao buscar comissões: {e}")
        
        # Se há comissões temporárias, mapear suas URLs
        if not self.commission_type or self.commission_type == 'TEMPORARY':
            yield Request(
                'https://www.camara.leg.br/comissoes/comissoes-temporarias',
                callback=self.map_temporary_commissions,
                priority=10
            )
    
    def process_department_from_data(self, dept_data, dept_id):
        """
        Processa uma comissão a partir dos dados já obtidos
        Retorna uma Request do Scrapy apenas para páginas da Câmara
        """
        dept_type = dept_data.get('type', '')
        acronym = dept_data.get('acronym', '')
        
        if not dept_id:
            self.logger.warning("Department ID não encontrado")
            return None
        
        # Determinar URL de notícias baseado no tipo
        if dept_type == 'Comissão Permanente' and acronym:
            # Comissão permanente: URL fixa
            news_url = f"https://www2.camara.leg.br/atividade-legislativa/comissoes/comissoes-permanentes/{acronym.lower()}/noticias"
            self.logger.info(f"Processando comissão permanente {acronym} (ID: {dept_id}): {news_url}")
            
            # Retornar requisição Scrapy para a URL da Câmara
            return Request(
                news_url,
                callback=self.parse_news_list,
                meta={
                    'department_id': dept_id,
                    'news_url': news_url,
                    'is_temporary': False
                },
                priority=7,
                dont_filter=True,
                errback=self.handle_error
            )
        elif dept_type in ['Comissão Especial', 'Comissão Externa', 'Comissão Parlamentar de Inquérito']:
            # Comissão temporária: será processada via map_temporary_commissions
            self.logger.info(f"Comissão temporária {dept_id} - será processada via mapeamento")
        else:
            self.logger.warning(f"Tipo de comissão não suportado: {dept_type}")
        
        return None
    
    def map_temporary_commissions(self, response):
        """
        Mapeia comissões temporárias da página de lista
        Extrai links e tenta encontrar seção de notícias para cada uma
        Também tenta mapear com IDs do banco via API
        """
        self.logger.info("Mapeando comissões temporárias...")
        
        # Primeiro, buscar comissões temporárias da API para ter os IDs
        api_url = os.environ.get('API_URL', 'http://localhost:3333')
        try:
            api_response = requests.get(f"{api_url}/department?type=TEMPORARY")
            if api_response.status_code == 200:
                api_data = api_response.json()
                temp_departments = api_data.get('departments', [])
                # Criar mapa de nome/URL para ID
                dept_map = {}
                for dept in temp_departments:
                    # Usar nome ou sigla como chave
                    key = dept.get('name', '').lower()
                    dept_map[key] = dept.get('id')
                    if dept.get('acronym'):
                        dept_map[dept.get('acronym').lower()] = dept.get('id')
                self.logger.info(f"Mapeadas {len(dept_map)} comissões temporárias da API")
        except Exception as e:
            self.logger.warning(f"Erro ao buscar comissões da API: {e}")
            dept_map = {}
        
        # Extrair links das comissões (especiais, externas, CPIs)
        # Tentar múltiplos seletores para encontrar os links
        commission_links = []
        commission_names = []
        
        # Seletor original do JSON
        selector = search_terms.get('temporary_commission_list', 'ul.l-lista-comissoes li a')
        commission_links = response.css(f'{selector}::attr(href)').getall()
        commission_names = response.css(f'{selector}::text').getall()
        
        # Se não encontrou, tentar seletores alternativos
        if len(commission_links) == 0:
            self.logger.warning(f"Seletor original '{selector}' não encontrou links. Tentando alternativos...")
            
            # Logar um trecho do HTML para debug (procurar pelo conteúdo principal)
            html_full = response.text
            
            # Procurar pelo conteúdo principal (main-content)
            main_start = html_full.find('main-content') or html_full.find('id="main"') or html_full.find('class="main"')
            if main_start > 0:
                # Logar 10000 chars a partir do conteúdo principal
                html_sample = html_full[main_start:main_start+10000] if len(html_full) > main_start+10000 else html_full[main_start:]
                self.logger.info(f"Trecho do HTML (a partir do conteúdo principal, 10000 chars):\n{html_sample}")
            else:
                # Procurar pelo body
                body_start = html_full.find('<body')
                if body_start > 0:
                    # Logar 10000 chars a partir do body
                    html_sample = html_full[body_start:body_start+10000] if len(html_full) > body_start+10000 else html_full[body_start:]
                    self.logger.info(f"Trecho do HTML (a partir do body, 10000 chars):\n{html_sample}")
                else:
                    # Se não encontrou, logar uma parte maior do início
                    html_sample = html_full[:10000] if len(html_full) > 10000 else html_full
                    self.logger.info(f"Trecho do HTML (primeiros 10000 chars):\n{html_sample}")
            
            # Contar quantos links com /comissoes/ existem na página inteira
            all_comissoes_links = response.xpath('//a[contains(@href, "/comissoes/")]/@href').getall()
            self.logger.info(f"Total de links com '/comissoes/' encontrados na página (via XPath): {len(all_comissoes_links)}")
            if len(all_comissoes_links) > 0:
                # Mostrar alguns exemplos
                sample_links = all_comissoes_links[:10]
                self.logger.info(f"Exemplos de links encontrados: {sample_links}")
            
            # Tentar seletores alternativos mais abrangentes
            alt_selectors = [
                'ul.l-lista-comissoes li a',
                'ul.lista-comissoes li a',
                '.lista-comissoes li a',
                'ul li a[href*="/comissoes/"]',
                'a[href*="/comissoes/"]',
                'section a[href*="/comissoes/"]',
                'div.comissoes a',
                'article a[href*="/comissoes/"]',
                'a[href*="comissao"]',
                'li a[href*="/comissoes/"]',
                '.comissoes-temporarias a',
                'div[class*="comissoes"] a',
                'ul[class*="lista"] a',
            ]
            
            for alt_selector in alt_selectors:
                try:
                    links = response.css(f'{alt_selector}::attr(href)').getall()
                    names = response.css(f'{alt_selector}::text').getall()
                    
                    # Filtrar links válidos (que apontam para comissões temporárias)
                    valid_links = []
                    valid_names = []
                    for i, link in enumerate(links):
                        if link:
                            # Normalizar link (remover espaços, etc)
                            link = link.strip()
                            
                            # Verificar se é um link válido de comissão
                            # Deve conter /comissoes/ mas não deve ser a própria página de lista
                            is_valid = (
                                '/comissoes/' in link and 
                                'comissoes-temporarias' not in link and
                                link not in ['/comissoes/', '/comissoes']
                            )
                            
                            if is_valid:
                                # Evitar links duplicados
                                if link not in valid_links:
                                    valid_links.append(link)
                                    # Limpar nome (remover espaços extras)
                                    name = names[i].strip() if i < len(names) and names[i] else ''
                                    valid_names.append(name)
                    
                    if len(valid_links) > 0:
                        self.logger.info(f"Seletor alternativo '{alt_selector}' encontrou {len(valid_links)} links válidos")
                        commission_links = valid_links
                        commission_names = valid_names
                        break
                except Exception as e:
                    self.logger.debug(f"Erro ao testar seletor '{alt_selector}': {e}")
                    continue
            
            # Se ainda não encontrou, tentar XPath como último recurso
            if len(commission_links) == 0:
                self.logger.warning("Nenhum seletor CSS funcionou. Tentando XPath...")
                try:
                    # XPath mais abrangente: encontrar todos os links que contêm /comissoes/ mas não são a página de lista
                    xpath_links = response.xpath('//a[contains(@href, "/comissoes/")]/@href').getall()
                    xpath_names = response.xpath('//a[contains(@href, "/comissoes/")]/text()').getall()
                    
                    self.logger.info(f"XPath encontrou {len(xpath_links)} links totais com '/comissoes/'")
                    
                    # Filtrar links válidos
                    for i, link in enumerate(xpath_links):
                        if link:
                            link = link.strip()
                            
                            # Verificar se é um link válido de comissão temporária
                            # Deve conter /comissoes/ mas não deve ser:
                            # - A página de lista (comissoes-temporarias)
                            # - Links genéricos (/comissoes/, /comissoes)
                            # - Links de outras seções
                            is_valid = (
                                '/comissoes/' in link and 
                                'comissoes-temporarias' not in link and
                                link not in ['/comissoes/', '/comissoes', '#', ''] and
                                not link.startswith('http') or 'camara.leg.br' in link
                            )
                            
                            if is_valid:
                                # Evitar links duplicados
                                if link not in commission_links:
                                    commission_links.append(link)
                                    name = xpath_names[i].strip() if i < len(xpath_names) and xpath_names[i] else ''
                                    commission_names.append(name)
                    
                    if len(commission_links) > 0:
                        self.logger.info(f"XPath encontrou {len(commission_links)} links válidos após filtragem")
                        # Mostrar alguns exemplos
                        self.logger.info(f"Exemplos de links válidos encontrados: {commission_links[:5]}")
                    else:
                        self.logger.warning(f"XPath encontrou {len(xpath_links)} links mas nenhum passou na filtragem")
                        # Mostrar alguns exemplos dos links que foram filtrados
                        if len(xpath_links) > 0:
                            self.logger.info(f"Exemplos de links filtrados: {xpath_links[:10]}")
                except Exception as e:
                    self.logger.error(f"Erro ao usar XPath: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
        
        self.logger.info(f"Encontrados {len(commission_links)} links de comissões temporárias")
        
        for i, link in enumerate(commission_links):
            if link and link.startswith('/'):
                full_url = f"https://www2.camara.leg.br{link}"
                commission_name = commission_names[i] if i < len(commission_names) else ''
                
                # Tentar encontrar department_id pelo nome
                dept_id = None
                if commission_name:
                    name_key = commission_name.lower().strip()
                    dept_id = dept_map.get(name_key)
                    if not dept_id:
                        # Tentar match parcial
                        for key, d_id in dept_map.items():
                            if name_key in key or key in name_key:
                                dept_id = d_id
                                break
                
                # Navegar até a página da comissão para encontrar link de notícias
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
        """
        Encontra o link para a seção de notícias de uma comissão temporária
        """
        commission_url = response.meta.get('commission_url', '')
        dept_id = response.meta.get('department_id')
        commission_name = response.meta.get('commission_name', '')
        
        # Procurar link para /noticias na página
        news_link = response.css(search_terms.get('commission_news_link', 'a[href*="/noticias"]::attr(href)')).get()
        
        if news_link:
            if news_link.startswith('/'):
                news_url = f"https://www2.camara.leg.br{news_link}"
            else:
                news_url = news_link if news_link.startswith('http') else response.urljoin(news_link)
            
            if dept_id:
                self.logger.info(f"Encontrado link de notícias para comissão {dept_id}: {news_url}")
            else:
                self.logger.warning(f"Link de notícias encontrado mas sem department_id: {commission_name} - {news_url}")
            
            # Processar notícias desta comissão
            yield Request(
                news_url,
                callback=self.parse_news_list,
                meta={
                    'department_id': dept_id,
                    'news_url': news_url,
                    'is_temporary': True
                },
                priority=6,
                errback=self.handle_error
            )
        else:
            self.logger.warning(f"Não foi encontrado link de notícias para: {commission_url} (Comissão: {commission_name})")
    
    
    def parse_news_list(self, response):
        """
        Parseia a lista de notícias de uma comissão
        Similar ao parse do scraper original, mas com departmentId
        """
        department_id = response.meta.get('department_id')
        news_url = response.meta.get('news_url', response.url)
        
        self.logger.info(f"Parsing news list - URL: {response.url}, Department ID: {department_id}, Status: {response.status}")
        
        # Verificar se a página foi redirecionada para login
        if 'require_login' in response.url or response.status == 302:
            self.logger.warning(f"Página redirecionada para login: {response.url}")
            return
        
        # Verificar se a página retornou 404
        if response.status == 404:
            self.logger.warning(f"Página não encontrada (404): {response.url}")
            return
        
        if not department_id:
            self.logger.warning(f"Department ID não encontrado no meta para URL: {response.url}")
            return
        
        # Verificar se o seletor CSS está encontrando elementos
        article_selector = search_terms.get('article', 'li.l-lista-noticias__item')
        articles_found = response.css(article_selector)
        articles_count = len(articles_found)
        
        self.logger.info(f"Seletor CSS usado: '{article_selector}' - Encontrados {articles_count} elementos")
        
        # Se não encontrou artigos, tentar seletores alternativos
        if articles_count == 0:
            # Logar um trecho do HTML para debug (apenas uma vez por comissão)
            if not hasattr(self, '_html_logged'):
                self._html_logged = set()
            
            if department_id not in self._html_logged:
                html_sample = response.text[:2000] if len(response.text) > 2000 else response.text
                self.logger.info(f"Trecho do HTML da página (primeiros 2000 chars) para debug:\n{html_sample}")
                self._html_logged.add(department_id)
            
            # Tentar seletores alternativos mais abrangentes
            alt_selectors = [
                'article',
                '.l-lista-noticias__item',
                'ul.l-lista-noticias li',
                '.noticia-item',
                'li[class*="noticia"]',
                'li[class*="lista"]',
                'div[class*="noticia"]',
                'ul li a',
                '.lista-noticias li',
                'section article',
                'div.noticia',
                'li.item',
            ]
            
            for alt_selector in alt_selectors:
                try:
                    alt_articles = response.css(alt_selector)
                    if len(alt_articles) > 0:
                        self.logger.warning(f"Seletor original não funcionou, mas seletor alternativo '{alt_selector}' encontrou {len(alt_articles)} elementos")
                        articles_found = alt_articles
                        articles_count = len(alt_articles)
                        # Atualizar o seletor para usar nos links também
                        article_selector = alt_selector
                        break
                except Exception as e:
                    self.logger.debug(f"Erro ao testar seletor '{alt_selector}': {e}")
                    continue
            
            # Se ainda não encontrou, tentar buscar por links que contenham "/noticias/"
            if articles_count == 0:
                news_links = response.css('a[href*="/noticias/"]')
                news_links_count = len(news_links)
                if news_links_count > 0:
                    self.logger.info(f"Encontrados {news_links_count} links que contêm '/noticias/' na URL - usando esses links diretamente")
                    articles_found = news_links
                    articles_count = news_links_count
                    article_selector = 'a[href*="/noticias/"]'
                else:
                    # Última tentativa: buscar qualquer link que possa ser uma notícia
                    all_links = response.css('a::attr(href)').getall()
                    news_links_filtered = [link for link in all_links if link and ('/noticias/' in link or '/noticia/' in link)]
                    if len(news_links_filtered) > 0:
                        self.logger.info(f"Encontrados {len(news_links_filtered)} links potenciais de notícias via filtro")
                        # Usar XPath para pegar os elementos <a> completos
                        articles_found = response.xpath('//a[contains(@href, "/noticias/") or contains(@href, "/noticia/")]')
                        articles_count = len(articles_found)
                        article_selector = 'xpath_links'
        
        articles_in_timeframe = 0
        
        for article in articles_found:
            if self.article_count >= self.MAX_ARTICLES_PER_COMMISSION * len(self.processed_commissions):
                self.logger.info(f"Limite de artigos atingido: {self.article_count}")
                break
            
            # Se o artigo já é um link (a tag), usar diretamente
            if article_selector.startswith('a[') or article_selector == 'xpath_links':
                link = article.css('::attr(href)').get()
                if not link:
                    # Tentar XPath se CSS não funcionou
                    link = article.xpath('./@href').get()
            else:
                link_selector = search_terms.get('link', 'a::attr(href)')
                link = article.css(link_selector).get()
                if not link:
                    # Tentar encontrar qualquer link dentro do elemento
                    link = article.xpath('.//a/@href').get()
            
            if link:
                # Garantir URL absoluta
                if link.startswith('/'):
                    link = f"https://www2.camara.leg.br{link}"
                elif not link.startswith('http'):
                    link = response.urljoin(link)
                
                # Verificar se o link é realmente uma notícia (contém /noticias/)
                if '/noticias/' in link:
                    self.logger.debug(f"Encontrado link de artigo: {link}")
                    articles_in_timeframe += 1
                    yield Request(
                        link,
                        callback=self.parse_article,
                        meta={'department_id': department_id},
                        priority=1,
                        errback=self.handle_error
                    )
                else:
                    self.logger.debug(f"Link ignorado (não é notícia): {link}")
            else:
                # Tentar extrair link de outras formas
                link = article.css('::attr(href)').get()
                if not link:
                    # Tentar encontrar qualquer link dentro do elemento
                    link = article.xpath('.//a/@href').get()
                
                if link:
                    if link.startswith('/'):
                        link = f"https://www2.camara.leg.br{link}"
                    elif not link.startswith('http'):
                        link = response.urljoin(link)
                    
                    if '/noticias/' in link:
                        self.logger.debug(f"Encontrado link de artigo (método alternativo): {link}")
                        articles_in_timeframe += 1
                        yield Request(
                            link,
                            callback=self.parse_article,
                            meta={'department_id': department_id},
                            priority=1,
                            errback=self.handle_error
                        )
                else:
                    self.logger.debug(f"Link não encontrado no artigo usando seletor '{link_selector}'")
        
        self.logger.info(f"Total de artigos encontrados nesta página: {articles_in_timeframe} de {articles_count}")
        
        if articles_in_timeframe == 0:
            self.logger.warning(f"Nenhum artigo encontrado na página: {response.url}")
            # Não marcar found_old_articles imediatamente - pode ser que a página esteja vazia
            # mas ainda há outras páginas com conteúdo
            return
        
        # Tentar próxima página se existir
        next_page = response.css('a[rel="next"]::attr(href)').get()
        if not next_page:
            # Tentar outros seletores para próxima página
            next_page = response.css('.pagination a.next::attr(href)').get()
            if not next_page:
                next_page = response.css('a:contains("Próxima")::attr(href)').get()
        
        if next_page:
            next_url = response.urljoin(next_page)
            self.logger.info(f"Encontrada próxima página: {next_url}")
            yield Request(
                next_url,
                callback=self.parse_news_list,
                meta=response.meta,
                priority=5,
                errback=self.handle_error
            )
        else:
            self.logger.debug("Nenhuma próxima página encontrada")
    
    def handle_error(self, failure):
        """Trata erros nas requisições"""
        request = failure.request
        url = request.url if request else "URL desconhecida"
        
        # Verificar tipo de erro
        if hasattr(failure.value, 'response'):
            response = failure.value.response
            status = response.status if response else "N/A"
            self.logger.error(f"Erro HTTP {status} ao processar requisição: {url}")
            
            # Se for 404, apenas logar como warning
            if response and response.status == 404:
                self.logger.warning(f"Página não encontrada (404): {url}")
            # Se for redirecionamento para login
            elif response and response.status in [302, 301]:
                if 'require_login' in url or 'login' in url.lower():
                    self.logger.warning(f"Página requer autenticação: {url}")
        else:
            self.logger.error(f"Erro ao processar requisição: {url} - {type(failure.value).__name__}: {failure.value}")
    
    def parse_article(self, response):
        """
        Parseia um artigo individual
        Similar ao scraper original, mas inclui departmentId
        """
        department_id = response.meta.get('department_id')
        
        self.logger.debug(f"Parsing article - URL: {response.url}, Department ID: {department_id}")
        
        # Verificar status da resposta
        if response.status != 200:
            self.logger.warning(f"Artigo retornou status {response.status}: {response.url}")
            return
        
        updated_selector = search_terms.get('updated', 'p.g-artigo__data-hora::text')
        updated = response.css(updated_selector).get()
        
        if not updated:
            # Tentar seletores alternativos para data
            alt_date_selectors = [
                'p.g-artigo__data-hora::text',
                '.g-artigo__data-hora::text',
                '.data-hora::text',
                'time::attr(datetime)',
                '.data::text',
            ]
            for alt_selector in alt_date_selectors:
                updated = response.css(alt_selector).get()
                if updated:
                    self.logger.debug(f"Data encontrada com seletor alternativo: '{alt_selector}'")
                    break
            
            if not updated:
                self.logger.warning(f"Data não encontrada no artigo: {response.url} (seletor usado: '{updated_selector}')")
                return
        
        try:
            updated = updated.strip()
            updated = updated.split(" ")[0]
            updated = updated.replace("/", "-")
            updated = datetime.strptime(updated, "%d-%m-%Y")
            self.logger.debug(f"Data parseada: {updated}")
        except ValueError as e:
            self.logger.warning(f"Formato de data inválido: '{updated}' no artigo {response.url} - Erro: {e}")
            return
        except Exception as e:
            self.logger.error(f"Erro ao processar data: {e} - Artigo: {response.url}")
            return
        
        title_selector = search_terms.get('title', 'h1.g-artigo__titulo::text')
        title = response.css(title_selector).get()
        
        if not title:
            # Tentar seletores alternativos para título
            alt_title_selectors = [
                'h1.g-artigo__titulo::text',
                'h1::text',
                '.g-artigo__titulo::text',
                'title::text',
            ]
            for alt_selector in alt_title_selectors:
                title = response.css(alt_selector).get()
                if title:
                    break
        
        if not title:
            self.logger.warning(f"Título não encontrado no artigo: {response.url}")
            title = "Sem título"
        
        content_selector = search_terms.get('content', 'div.js-article-read-more')
        content = response.css(content_selector).getall()
        
        if not content:
            # Tentar seletores alternativos para conteúdo
            alt_content_selectors = [
                'div.js-article-read-more',
                '.js-article-read-more',
                'article p',
                '.conteudo',
                '.artigo-conteudo',
            ]
            for alt_selector in alt_content_selectors:
                content = response.css(alt_selector).getall()
                if content:
                    break
        
        if not content:
            self.logger.warning(f"Conteúdo não encontrado no artigo: {response.url}")
            content = [""]
        
        try:
            content = BeautifulSoup(" ".join(content), "html.parser").text
            content = content.replace("\n", " ")
            content = content.strip()
        except Exception as e:
            self.logger.error(f"Erro ao processar conteúdo: {e} - Artigo: {response.url}")
            content = ""
        
        # Verificar se o artigo está no período válido (90 dias)
        if search_limit <= updated <= today:
            # Resetar contador de artigos antigos quando encontrar um artigo válido
            self.old_articles_count = 0
            
            item = articleItem(
                updated=updated,
                title=title,
                content=content,
                link=response.url,
                departmentId=department_id,
            )
            yield item
            self.data.append(item)
            self.article_count += 1
            self.logger.info(f"Artigo coletado com sucesso: {title[:50]}... (Data: {updated.strftime('%d/%m/%Y')}, Total: {self.article_count})")
        else:
            if updated < search_limit:
                self.old_articles_count += 1
                self.logger.debug(f"Artigo muito antigo (data: {updated.strftime('%d/%m/%Y')}, limite: {search_limit.strftime('%d/%m/%Y')}): {response.url} (Antigos consecutivos: {self.old_articles_count})")
                
                # Parar apenas se encontrar muitos artigos antigos consecutivos
                if self.old_articles_count >= self.MAX_OLD_ARTICLES_BEFORE_STOP:
                    self.logger.info(f"Encontrados {self.old_articles_count} artigos antigos consecutivos. Parando coleta para esta comissão.")
                    self.found_old_articles = True
            else:
                self.logger.debug(f"Artigo no futuro (data: {updated.strftime('%d/%m/%Y')}): {response.url}")
    
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
