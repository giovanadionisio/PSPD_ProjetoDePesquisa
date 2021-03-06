# PSPD - Projeto de Pesquisa
**Matéria**: Programação para Sistemas Paralelos e Distribuídos  
**Semestre**: 2021/2  
**Aluna**: Giovana Vitor Dionísio Santana  
**Matrícula**: 18/0017659
## Parte 2
### Requerimentos
- [Spark](https://www.vultr.com/docs/install-apache-spark-on-ubuntu-20-04/)
- [Kafka](https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-20-04)
- Python
  - [Jupyter](https://pypi.org/project/jupyter/)
  - [Pandas](https://pypi.org/project/pandas/)
  - [Numpy](https://pypi.org/project/numpy/)
  - [Matplotlib](https://pypi.org/project/matplotlib/)
  - [Kafka-Python](https://pypi.org/project/kafka-python/)
  

### Execução
Primeiramente crie os seguintes tópicos Kafka:  
- topico
- PalavrasComS
- PalavrasComP
- PalavrasComR
- PalavrasCom6
- PalavrasCom8
- PalavrasCom11

Em um terminal execute o comando:  
```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 word_count.py localhost 9999```    
para a execução do servidor Spark.

Em outros dois terminais, execute:  
```python consumer.py```  
e  
```python producer.py```  
para iniciar o consumer e o producer, respectivamente.

Por fim, para a visualização do gráfico interativo de número de palavras (iniciadas pelas letras S, P e R e com 6, 8 e 11 caracteres) por intervalo, inicie o servidor do Jupyter Notebook com o comando:  
```jupyter notebook```  
abra o notebook ```gera_grafico.ipynb``` e execute a primeira (e única) célula. 

### Saída
| spark(_wordcount.py_) | gráfico |
| --------------------- | ---------------- |
|![spark](https://i.ibb.co/3fzDLVz/Captura-de-tela-de-2022-05-02-19-19-43.png)|![python](https://i.ibb.co/xhYJ3jv/Captura-de-tela-de-2022-05-02-19-18-25.png)|

[Vídeo de execução](https://www.loom.com/share/327baaae8e9e4889901a1d82943116ff)  
[Vídeo de execução - Gráfico](https://www.loom.com/share/c150187260014f888bef8562044ac7b3)
