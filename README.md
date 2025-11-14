
# Curso Apache Airflow

Aprendendo a orquestrar primeiro pipeline de dados!


## Environment Variables

Para rodar esse projeto você precisa criar um arquivo `.env` com as seguintes variáveis:

``` bash
API_KEY = 
API_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
CITY = "London" # ou outra de sua escolha
FILE_PATH = "~data/"
```
`API_KEY` = Chave de usuário gerada no site da api do [Visual Crossing](https://www.visualcrossing.com/)

`FILE_PATH` = Onde os dados serão salvos na máquina


## Rodar Localmente

Clone o projeto

```bash
  git clone https://github.com/laryyssa/alura-apache-airflow.git
```

Vá para o diretório

```bash
  cd alura-apache-airflow
```

Crie um ambiente virtual

```bash
  python3 -m venv venv
  
  source venv/bin/activate

  pip install -r requirements.txt
```

Rode o Apache airflow

```bash
  export AIRFLOW_HOME= ...

  airflow standalone
```
`AIRFLOW_HOME` = Diretório para o projeto