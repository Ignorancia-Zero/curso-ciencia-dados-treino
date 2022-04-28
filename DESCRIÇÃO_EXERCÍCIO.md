Realize o processamento da base de Escolas para extrair os dados

Pense nas seguintes operações que terão de ser feitos:

1. Renomear os campos: Os nomes dos campos variam ao longo do tempo
2. Remover colunas: Algumas colunas tem informações redundantes com outras bases ou são de pouca relevância
3. Converter colunas de tempo: Há algumas datas na base, seria melhor modifica-las
4. Colunas com 88888: Algumas colunas estão com valor 88888 para indicar "não reportado", será que faz sentido?
5. Colunas IN: 
   1. Algumas colunas IN_ aparecem e desaparecem da base, será que podemos reproduzi-las?
   2. É possível reproduzir algumas colunas IN_ a partir de outras colunas IN_?
   3. É possível reproduzir algumas colunas IN_ a partir de outras colunas TP_?
   4. As colunas IN_ tem valor 9 para indicar não reportado. Será que isso faz sentido?
6. Como podemos preencher as colunas TP_?
   1. Devemos mante-las como texto ou há algum tipo melhor?
   2. Como nós podemos preenche-las?
7. Como nós podemos lidar com valores nulos em geral?