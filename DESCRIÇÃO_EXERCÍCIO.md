Construa um objeto ETL base que tenha todos os métodos de extrair, 
transformar e carregar, além de atributos de pastas de entrada e saída

Sugestão de estrutura
1. Coloque todo o código dentro de src/
2. Adicione um pacote chamado aquisicao dentro de src
3. Construa um objeto BaseETL com as seguintes características:
- Atributos
  - caminho_entrada: Objeto path para dados de entrada
  - caminho_saida: Objeto path para dados de saida
  - _dados_entrada: Dicionário com os dados carregados de entrada
  - _dados_saida: Dicionário com os dados de saída
- Métodos
  - _extract: Método a ser preenchido pela classe filha para extrair os dados
  - _transform: Método a ser preenchido pela classe filha para transformar os dados
  - extract: Faz um print e chama o método _extract
  - transform: Faz um print e chama o método _transform
  - load: Extraí os dados transformados dos _dados_saida como arquivos .parquet com o nome das chaves para o caminho_saida
  - pipeline: Executa o extract, transform, load
