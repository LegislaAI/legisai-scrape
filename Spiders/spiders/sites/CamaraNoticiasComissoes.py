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

# Janela padrão: hoje e até 90 dias atrás (comissões não são tão frequentes)
_today = datetime.strptime(date.today().strftime("%d/%m/%Y"), "%d/%m/%Y")
_default_search_limit = datetime.strptime((date.today() - timedelta(days=90)).strftime("%d/%m/%Y"), "%d/%m/%Y")

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
    found_old_articles = False
    MAX_ARTICLES_PER_COMMISSION = 50
    MAX_TOTAL_ARTICLES = 1000  # Teto de segurança na coleta diária
    MAX_OLD_ARTICLES_BEFORE_STOP = 10  # Parar paginação após N artigos antigos consecutivos
    BATCH_SIZE = 100  # Envio em batch para a API (evita payload >5MB)

    # Instância (definidos em __init__)
    # search_limit, today, backfill_mode, articles_per_commission, batch_data, old_articles_count

    def __init__(self, department_ids=None, start_date=None, end_date=None, backfill_mode=None, *args, **kwargs):
        super(CamaraNoticiasComissoesSpider, self).__init__(*args, **kwargs)
        self.department_ids = department_ids.split(',') if department_ids else None
        self.processed_commissions = set()
        self.visited_pagination_urls = {}
        self.batch_data = []
        self.data = []
        self.article_count = 0
        self.old_articles_count = 0
        self.articles_per_commission = {}

        # Backfill: -a start_date=01/01/2025 -a end_date=31/12/2025 -a backfill_mode=1
        self.backfill_mode = str(backfill_mode or kwargs.get('backfill_mode') or '').lower() in ('1', 'true', 'yes')
        start_date = start_date or kwargs.get('start_date')
        end_date = end_date or kwargs.get('end_date')

        def parse_date(s):
            if not s:
                return None
            s = str(s).strip()[:10]
            for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s, fmt)
                except (ValueError, TypeError):
                    continue
            return None

        if self.backfill_mode and start_date:
            self.search_limit = parse_date(start_date) or _default_search_limit
            self.today = parse_date(end_date) or _today
            if self.today < self.search_limit:
                self.today, self.search_limit = self.search_limit, self.today
            self.effective_max_total = 100000
            self.effective_max_old_before_stop = 999999
            self.logger.info(f"Modo backfill: janela {self.search_limit.strftime('%d/%m/%Y')} a {self.today.strftime('%d/%m/%Y')}")
        else:
            self.search_limit = _default_search_limit
            self.today = _today
            self.effective_max_total = self.MAX_TOTAL_ARTICLES
            self.effective_max_old_before_stop = self.MAX_OLD_ARTICLES_BEFORE_STOP
    
    def extract_b_start(self, url):
        """
        Extrai o valor de b_start:int da URL
        Retorna 0 se não encontrar o parâmetro
        """
        if not url:
            return 0
        import re
        match = re.search(r'b_start:int=(\d+)', url)
        return int(match.group(1)) if match else 0
    
    def get_base_url(self, url):
        """
        Extrai URL base sem parâmetros de paginação
        Usado para identificar a comissão quando não há department_id
        """
        if not url:
            return ''
        import re
        # Remover parâmetros de query relacionados à paginação
        base = re.sub(r'[?&]b_start:int=\d+', '', url)
        return base.rstrip('?&')
    
    def start_requests(self):
        """
        Inicia o processo de scraping de comissões permanentes:
        Busca comissões permanentes na API e processa a URL de notícias de cada uma.
        """
        api_url = os.environ.get('API_URL', 'http://localhost:3333')

        try:
            if self.department_ids:
                for dept_id in self.department_ids:
                    dept_url = f"{api_url}/department/{dept_id}"
                    try:
                        response = requests.get(dept_url)
                        if response.status_code == 200:
                            dept_data = response.json()
                            req = self.process_department_from_data(dept_data, dept_id)
                            if req:
                                yield req
                    except Exception as e:
                        self.logger.error(f"Erro ao buscar comissão {dept_id}: {e}")
            else:
                api_endpoint = f"{api_url}/department?type=PERMANENT"
                self.logger.info(f"Buscando comissões permanentes: {api_endpoint}")
                response = requests.get(api_endpoint)

                if response.status_code == 200:
                    data = response.json()
                    departments = data.get('departments', [])
                    self.logger.info(f"Encontradas {len(departments)} comissões permanentes")

                    for dept in departments:
                        dept_id = dept.get('id')
                        if dept_id and dept_id not in self.processed_commissions:
                            self.processed_commissions.add(dept_id)
                            req = self.process_department_from_data(dept, dept_id)
                            if req:
                                yield req
                else:
                    self.logger.error(f"Erro ao buscar comissões: {response.status_code}")
        except Exception as e:
            self.logger.error(f"Erro ao buscar comissões: {e}")

    def process_department_from_data(self, dept_data, dept_id):
        """
        Processa uma comissão permanente: monta URL de notícias e retorna Request.
        """
        dept_type = dept_data.get('type', '')
        acronym = dept_data.get('acronym', '')

        if not dept_id:
            self.logger.warning("Department ID não encontrado")
            return None

        if dept_type == 'Comissão Permanente' and acronym:
            news_url = f"https://www2.camara.leg.br/atividade-legislativa/comissoes/comissoes-permanentes/{acronym.lower()}/noticias"
            self.logger.info(f"Processando comissão permanente {acronym} (ID: {dept_id}): {news_url}")

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

        self.logger.warning(f"Tipo de comissão não é permanente (ignorado): {dept_type}")
        return None

    def parse_news_list(self, response):
        """
        Parseia a lista de notícias de uma comissão
        Similar ao parse do scraper original, mas com departmentId
        """
        if self.article_count >= self.effective_max_total:
            self.crawler.engine.close_spider(self, "Limite global de artigos atingido.")
            return
        department_id = response.meta.get('department_id')
        news_url = response.meta.get('news_url', response.url)
        is_temporary = response.meta.get('is_temporary', False)
        commission_name = response.meta.get('commission_name', '')
        
        self.logger.info(f"Parsing news list - URL: {response.url}, Department ID: {department_id}, Status: {response.status}, Temporária: {is_temporary}")
        
        # Verificar se a página foi redirecionada para login
        if 'require_login' in response.url or response.status == 302:
            self.logger.warning(f"Página redirecionada para login: {response.url}")
            return
        
        # Verificar se a página retornou 404
        if response.status == 404:
            self.logger.warning(f"Página não encontrada (404): {response.url}")
            return
        
        # Para comissões temporárias, department_id pode ser None
        # Continuar processamento mesmo sem department_id
        if not department_id:
            if is_temporary:
                self.logger.info(f"Processando notícias de comissão temporária sem department_id: {commission_name}")
            else:
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
            
            # Usar uma chave única para logging (department_id ou URL)
            log_key = department_id if department_id else response.url
            if log_key not in self._html_logged:
                html_sample = response.text[:2000] if len(response.text) > 2000 else response.text
                self.logger.info(f"Trecho do HTML da página (primeiros 2000 chars) para debug:\n{html_sample}")
                self._html_logged.add(log_key)
            
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
            if self.article_count >= self.effective_max_total:
                self.logger.info(f"Limite global de artigos atingido: {self.article_count}")
                return
            # Limite por comissão (em modo normal): não solicitar mais artigos desta comissão
            if not self.backfill_mode and self.articles_per_commission.get(department_id, 0) >= self.MAX_ARTICLES_PER_COMMISSION:
                continue
            
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
        next_page = None
        
        # Estratégia 1: Procurar por rel="next"
        next_page = response.css('a[rel="next"]::attr(href)').get()
        
        # Estratégia 2: Procurar por classes de paginação
        if not next_page:
            next_page = response.css('.pagination a.next::attr(href)').get()
            if not next_page:
                next_page = response.css('.pagination a[aria-label*="próxima"]::attr(href)').get()
            if not next_page:
                next_page = response.css('.pagination a[aria-label*="Próxima"]::attr(href)').get()
        
        # Estratégia 3: Procurar por texto "Próxima" ou "Próximo"
        if not next_page:
            # XPath para encontrar link com texto contendo "Próxima" ou "Próximo"
            next_page = response.xpath('//a[contains(text(), "Próxima") or contains(text(), "Próximo")]/@href').get()
        
        # Estratégia 4: Para página geral de notícias, pode ter paginação numérica
        if not next_page and 'camara.leg.br/noticias' in response.url and 'pagina' not in response.url:
            # Tentar encontrar link para página 2
            page2_link = response.xpath('//a[contains(@href, "pagina=2") or contains(@href, "pagina=1")]/@href').get()
            if page2_link:
                # Se encontrou página 1 ou 2, construir link para próxima página
                if 'pagina=1' in page2_link:
                    next_page = page2_link.replace('pagina=1', 'pagina=2')
                elif 'pagina=2' in page2_link:
                    next_page = page2_link.replace('pagina=2', 'pagina=3')
        
        # Estratégia 5: Buscar próxima página por incremento de b_start
        if not next_page:
            # Contar quantos links de paginação existem
            pagination_links = response.css('.pagination a::attr(href)').getall()
            if len(pagination_links) > 0:
                self.logger.debug(f"Encontrados {len(pagination_links)} links de paginação, mas nenhum identificado como 'próxima'")
                
                # Extrair b_start da URL atual
                current_b_start = self.extract_b_start(response.url)
                next_b_start = current_b_start + 20  # Incremento padrão do site da Câmara
                
                # Procurar link com b_start igual ao próximo esperado
                for link in pagination_links:
                    if link:
                        link_b_start = self.extract_b_start(link)
                        if link_b_start == next_b_start:
                            next_page = link
                            self.logger.debug(f"Encontrada próxima página via Estratégia 5: b_start {current_b_start} → {next_b_start}")
                            break
                
                # Se não encontrou o próximo exato, procurar qualquer link com b_start maior
                if not next_page:
                    for link in pagination_links:
                        if link:
                            link_b_start = self.extract_b_start(link)
                            if link_b_start > current_b_start:
                                next_page = link
                                self.logger.debug(f"Encontrada próxima página via Estratégia 5 (fallback): b_start {current_b_start} → {link_b_start}")
                                break
        
        if next_page:
            next_url = response.urljoin(next_page)
            
            # Validar que não é a mesma página
            if next_url == response.url:
                self.logger.debug(f"Próxima página é a mesma que a atual. Parando paginação.")
                return
            
            # Validar que b_start está aumentando
            current_b_start = self.extract_b_start(response.url)
            next_b_start = self.extract_b_start(next_url)
            
            if next_b_start <= current_b_start:
                self.logger.debug(f"Próxima página tem b_start menor ou igual ({next_b_start} <= {current_b_start}). Parando paginação.")
                return
            
            # Identificar chave da comissão (para controle por comissão)
            # Usar department_id se disponível, senão usar URL base (sem parâmetros)
            commission_key = department_id if department_id else self.get_base_url(response.url)
            
            # Inicializar set de URLs visitadas para esta comissão se não existir
            if commission_key not in self.visited_pagination_urls:
                self.visited_pagination_urls[commission_key] = set()
            
            # Verificar se já foi visitada para esta comissão
            if next_url in self.visited_pagination_urls[commission_key]:
                self.logger.debug(f"URL de paginação já visitada para esta comissão: {next_url}. Parando para evitar loop.")
                return
            
            # Marcar como visitada para esta comissão
            self.visited_pagination_urls[commission_key].add(next_url)
            
            self.logger.info(f"Encontrada próxima página: {next_url} (b_start: {current_b_start} → {next_b_start}, Comissão: {commission_key})")
            yield Request(
                next_url,
                callback=self.parse_news_list,
                meta=response.meta,
                priority=5,
                errback=self.handle_error
                # Removido dont_filter=True - não é necessário com controle de URLs visitadas
            )
        else:
            self.logger.debug("Nenhuma próxima página encontrada")
            # Logar informações sobre a página para debug
            pagination_elements = response.css('.pagination, .paginacao, [class*="pagin"]').getall()
            if len(pagination_elements) > 0:
                self.logger.debug(f"Elementos de paginação encontrados na página: {len(pagination_elements)}")
            else:
                self.logger.debug("Nenhum elemento de paginação encontrado na página")
    
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
                '[class*="data-hora"]::text',
                '[class*="data"]::text',
                '.data-hora::text',
                'time::attr(datetime)',
                '.data::text',
                'p::text',
            ]
            for alt_selector in alt_date_selectors:
                candidate = response.css(alt_selector).get()
                if candidate and (re.search(r'\d{2}/\d{2}/\d{4}', candidate) or re.search(r'\d{4}-\d{2}-\d{2}', candidate)):
                    updated = candidate
                    self.logger.debug(f"Data encontrada com seletor alternativo: '{alt_selector}'")
                    break

        # Fallback: buscar primeiro DD/MM/YYYY ou YYYY-MM-DD no HTML da página
        if not updated and response.text:
            date_match = re.search(r'\b(\d{2})/(\d{2})/(\d{4})\b', response.text)
            if date_match:
                updated = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                self.logger.debug(f"Data encontrada via regex no HTML (DD/MM/YYYY)")
            if not updated:
                iso_match = re.search(r'\b(\d{4})-(\d{2})-(\d{2})\b', response.text)
                if iso_match:
                    updated = f"{iso_match.group(3)}-{iso_match.group(2)}-{iso_match.group(1)}"
                    self.logger.debug(f"Data encontrada via regex no HTML (ISO)")

        if not updated:
            self.logger.warning(f"Data não encontrada no artigo: {response.url} (seletor usado: '{updated_selector}')")
            return

        try:
            updated = updated.strip()
            # Aceitar formato DD/MM/YYYY, DD-MM-YYYY ou YYYY-MM-DD (com ou sem hora)
            first_part = updated.split(" ")[0].split("T")[0]
            if '/' in first_part:
                date_str = first_part.replace("/", "-")
                updated = datetime.strptime(date_str, "%d-%m-%Y")
            elif re.match(r'\d{4}-\d{2}-\d{2}', first_part):
                updated = datetime.strptime(first_part[:10], "%Y-%m-%d")
            else:
                updated = datetime.strptime(first_part, "%d-%m-%Y")
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
            # Tentar seletores alternativos (estrutura do site da Câmara pode variar)
            alt_content_selectors = [
                'div.js-article-read-more',
                '.js-article-read-more',
                'article p',
                'main p',
                'main article p',
                '[class*="artigo"] p',
                '[class*="conteudo"]',
                '.g-artigo p',
                '.conteudo',
                '.artigo-conteudo',
                'main div[class*="conteudo"]',
                'main div[class*="artigo"]',
                'article',
                'main',
            ]
            for alt_selector in alt_content_selectors:
                content = response.css(alt_selector).getall()
                if content:
                    self.logger.debug(f"Conteúdo encontrado com seletor alternativo: '{alt_selector}'")
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
        
        if self.article_count >= self.effective_max_total:
            self.logger.info(f"Limite global de artigos atingido: {self.article_count}. Parando coleta.")
            return

        # Verificar se o artigo está na janela de datas (search_limit até today)
        if self.search_limit <= updated <= self.today:
            self.old_articles_count = 0
            self.articles_per_commission[department_id] = self.articles_per_commission.get(department_id, 0) + 1

            item = articleItem(
                updated=updated,
                title=title,
                content=content,
                link=response.url,
                departmentId=department_id,
            )
            yield item
            self.data.append(item)
            self.batch_data.append(item)
            self.article_count += 1
            if len(self.batch_data) >= self.BATCH_SIZE:
                self._upload_batch()
            if self.article_count >= self.effective_max_total:
                self.crawler.engine.close_spider(self, "Limite global de artigos atingido.")
            self.logger.info(f"Artigo coletado: {title[:50]}... (Data: {updated.strftime('%d/%m/%Y')}, Total: {self.article_count})")
        else:
            if updated < self.search_limit:
                self.old_articles_count += 1
                self.logger.debug(f"Artigo fora da janela (data: {updated.strftime('%d/%m/%Y')}, janela: {self.search_limit.strftime('%d/%m/%Y')} a {self.today.strftime('%d/%m/%Y')}): {response.url} (antigos consecutivos: {self.old_articles_count})")
                if self.old_articles_count >= self.effective_max_old_before_stop:
                    self.logger.info(f"Encontrados {self.old_articles_count} artigos antigos consecutivos. Parando paginação desta comissão.")
                    self.found_old_articles = True
            else:
                self.logger.debug(f"Artigo no futuro (data: {updated.strftime('%d/%m/%Y')}): {response.url}")
    
    def _upload_batch(self):
        """Envia o batch atual para a API (evita payload único muito grande)."""
        if not self.batch_data:
            return
        api_url = os.environ.get('API_URL')
        if not api_url:
            self.logger.error("API_URL não definido. Ignorando envio do batch.")
            return
        try:
            data_dicts = [item.to_dict() for item in self.batch_data]
            self.logger.info(f"Enviando batch de {len(data_dicts)} registros para {api_url}/news/scrape?type=PARLIAMENT")
            response = requests.post(f"{api_url}/news/scrape?type=PARLIAMENT", json={"records": data_dicts})
            if response.status_code >= 200 and response.status_code < 300:
                self.logger.info(f"Batch enviado com sucesso: {len(data_dicts)} registros")
            else:
                self.logger.error(f"Falha no envio do batch: {response.status_code} - {response.text}")
        except Exception as e:
            self.logger.error(f"Erro ao enviar batch: {e}")
        finally:
            self.batch_data = []
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraNoticiasComissoesSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider
    
    def upload_data(self, spider):
        # Enviar o que restar do batch
        self._upload_batch()
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
