# Curso de Ciência de Dados - Treino

---
## Sobre o Projeto

Este repositório é utilizado para acompanhar os exercícios do curso de ciência de dados
do canal de youtube Ignorância Zero (https://www.youtube.com/channel/UCmjj41YfcaCpZIkU-oqVIIw)

Cada ramo deste repositório conterá exercícios e resoluções dos exercícios
passados em aula, na qual temos como objetivo desenvolver uma ferramenta que
consolide os dados de educação do país e gere modelos e interfaces gráficas
que permitam interpretar e melhorar a educação do país

---
## Requisitos
* Python 3.9 64-bit
  * Windows: https://www.python.org/downloads/windows/ ou https://docs.anaconda.com/anaconda/install/windows/
  * Mac: https://www.python.org/downloads/mac-osx/ ou https://docs.anaconda.com/anaconda/install/mac-os/ ou https://formulae.brew.sh/formula/python@3.9
  * Linux:
---
  
## Instalação do Ambiente Python

---
### Windows

#### Configuração do Ambiente Python

##### Opção 1: Anaconda/Miniconda
* Execute no terminal
```
conda create -n curso-ciencia-dados python=3.9
```
* Em seguida execute `conda activate curso-ciencia-dados`. 
* Você pode adicionar esse ambiente ao Pycharm:
File -> Settings -> Project Interpreter -> Add -> Conda Environment ->
Existing Environment.


##### Opção 2: PyCharm com ambiente Conda
* Faça o download do PyCharm (https://www.jetbrains.com/pycharm/)
* Pycharm -> Settings -> Project Interpreter -> Add -> Conda Enviroment
-> [Selecione o conda apropriado] -> Python Version 3.9

#### Instalação Pacotes

##### !!! IMPORTANTE !!! #####
Garanta que você possuí a última versão do Visual C++ runtime instalada 
(https://aka.ms/vs/16/release/vc_redist.x64.exe)

No Windows há um sério problema na configuração de ambiente
que ao ser criado pelo processo acima pode acarretar na geração
de problemas de compatibilidade principalmente com a biblioteca 
do geopandas. 

Desta forma, caso haja algum problema recomendamos a instalação 
manual do ambiente executando os comandos descritos abaixo a 
partir da raíz do projeto:
```
conda create -n curso-ciencia-dados python=3.9
conda activate curso-ciencia-dados
cd '.\suporte\Pacotes Windows\'
conda update -n base -c defaults conda
conda install -c anaconda numpy==1.21.2
conda install -c conda-forge pandas==1.3.4
conda install -c anaconda jupyter==1.0.0
conda install -c conda-forge jupyter_contrib_nbextensions==0.5.1
conda install -c conda-forge pyarrow==6.0.0
conda install -c anaconda beautifulsoup4==4.9.3
conda install -c conda-forge click==8.0.3
conda install -c anaconda lxml==4.6.3
conda install -c conda-forge openpyxl==3.0.9
conda install -c anaconda yaml==5.4.1
conda install -c anaconda requests==2.26.0
conda install -c conda-forge tqdm==4.62.3
conda install -c conda-forge mypy==0.942
pip install types-beautifulsoup4==4.11.1
```

### Mac

#### virtualenv
1. Instale a biblioteca `virtualenv` no Python.
1. Crie um ambiente virtual na raiz do repositório
1. Instale os pacotes requeridos
```
env $ pip3 install virtualenv
env $ python3 -m venv venv
env $ source ./venv/bin/activate
env $ pip install -r requirements.txt
```
*Após ativar o ambiente você pode utilizar 'python' e 'pip' sem o 3*

#### anaconda
Siga os passos descritos em https://www.youtube.com/watch?v=KJ1tajePMd4&list=PLfCKf0-awunPFkWOKWNaXB_ndaHBlJ4QQ&index=7&ab_channel=Ignor%C3%A2nciaZero

### Linux

#### virtualenv
1. Instale a biblioteca `virtualenv` no Python.
1. Crie um ambiente virtual na raiz do repositório
1. Instale os pacotes requeridos
```
> pip3 install virtualenv
> python3 -m venv venv
> source ./venv/bin/activate
> pip install -r requirements.txt
```

#### anaconda
Siga os passos descritos em https://www.youtube.com/watch?v=e47Fcao8xUM&list=PLfCKf0-awunPFkWOKWNaXB_ndaHBlJ4QQ&index=6&ab_channel=Ignor%C3%A2nciaZero

---
  
## Pessoas
Esse curso é desenvolvido para o canal do Youtube por Pedro Forli.
Você pode entrar em contato com os desenvolvedores através de:
- Discord: https://discord.gg/3NA8ubw5pG
- Facebook: https://bit.ly/3J1PKB9
- LinkeDin: https://bit.ly/3rqb5yl
- Instragram: https://bit.ly/3GxELOw
- Twitter: https://bit.ly/3uFjJv5
- E-mail: ignorancia_zero@hotmail.com 
- Skype: ignorancia_zero

---

## Como contribuir
O código disponibilizado aqui é aberto e pode ser utilizado sem custo.

Se você quiser contribuir com o conteúdo desenvolvido entre em contato por
alguma das vias descritas acima e poderemos adiciona-lo ao Github e ao projeto
colocado no Atlassian