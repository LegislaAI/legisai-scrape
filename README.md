# legisai-scrape

## Coleta de notícias de comissões

Existem dois spiders separados:

- **Comissões permanentes**: `scrapy crawl CamaraNoticiasComissoes` (opcional: `-a department_ids=id1,id2`)
- **Comissões temporárias**: `scrapy crawl CamaraNoticiasComissoesTemporarias`

O limite global de notícias coletadas é controlado pela constante `MAX_TOTAL_ARTICLES` em cada spider (valor padrão: 50 para testes; aumentar para o deploy oficial).
