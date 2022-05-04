Crie os testes unitários referentes as funções do bloco de utils.

Adicione uma pasta de tests e dentro dela uma pasta para o módulo utils.
Depois disso crie um arquivo test_[modulo].py para cada módulo a ser testado com funções test_[função]
para cada um dos itens abaixo:
1. info
   1. carrega_yaml: Teste se os dados são carregados como dicionário com o conteúdo esperado
   2. carrega_excel: Teste se os dados são carregados como pd.DataFrame com o conteúdo esperado
2. logs
   1. configura_logs: Teste se o arquivo de logs é gerado corretamente, se se novos logs são adicionados ao arquivo
   2. log_erros: Teste se o erro aparece na saída de logs corretamente
3. web
   1. obtem_pagina: Teste se a página é processada como BeautifulSoup e se o conteúdo da página está correto 
   (sugestão: veja uma página estável como o www.google.com)
   2. download_dados_web: Teste se um arquivo é gerado ao fazer o download e também se ao passar um buffer
   o conteúdo do buffer é armazenado corretamente