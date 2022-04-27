Construa um código que permita processar uma página do INEP e obter os 
links das bases de dados que podem ser baixadas.

Esse tipo de processamento exigirá que você conheça sobre Web Scraping,
para isso sugerimos que você veja:
- https://medium.com/pyladiesbh/beautiful-soup-parseamento-de-html-337197a7d4b9
- https://www.codingem.com/python-download-file-from-url/
- https://www.analyticsvidhya.com/blog/2021/05/how-to-use-progress-bars-in-python/

Quanto as páginas, você deverá acessar:
- https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados
- https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar

Para isso sugerimos que você:
1. Adicione ao utils uma biblioteca web
2. Crie um função para baixar uma página dada uma URL
3. Escreve uma função para baixar os dados de um link
4. Crie um objeto ETL que permita acessar os dados de uma base de maneira estruturada
   1. Reflita: Você vai pegar mais de um dado do INEP?
   2. Reflita: Há um formato padrão da URL do INEP?
   3. Reflita: Há uma formatação padrão nas páginas do INEP?
