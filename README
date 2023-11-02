### Configurando o ambiente local

Para começar, inicialize os containers docker com o comando
```bash
$ docker-compose -f spark-kafka-composer.yaml up
```
Veja se todos os containers estão rodando executando o comando:
```bash
$ docker ps
```

Depois, entre no container kafka e crie os topicos pedidos no trabalho:
```bash
$ docker exec -it kafka_streaming_class-kafka-1 bash
$ kafka-topics --create --replication-factor 1 --bootstrap-server localhost:9092 --topic <nome do topico>
```

Depois dos topicos criados, você pode entrar no container spark e executar seus scripts
```bash
$ docker exec -it kafka_streaming_class-spark-1 bash
$ spark-submit <script x>
$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 <script que fala com kafka>
```

### Executando o projeto

Faça download do [arquivo zip](http://servicosbhtrans.pbh.gov.br/Bhtrans/webservice/AGOSTO_2022.zip)
 e faça unzip na pasta data.

A pasta `/data` deve ter a pasta `AGOSTO_2022`

Rode o comando para transformar os arquivos JSON para parquet no container spark.
```
spark-submit /src/transform_json_to_parquet.py
```

Depois faça rode o join de parquet
```bash
spark-submit /src/join_parquet_files.py
```

Vamos agora carregar os dados no kafka:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/insert_data_kafka.py
```

Agora com dados carregados, podemos consumi-los:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/group_by_vehicle_type.py
```

### Comandos uteis do kafka

Conectar num topico e produzir mensagens com formato "chave:valor":
```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test_topic --property "parse.key=true" --property "key.separator=:"
```

Verificar informações do topico:
```bash
kaflog-dirs --describe --topic-list traffic_sensor --bootstrap-server localhost:9092
```

Deletar um topico:
```bash
kafka-topics --delete --topic random_traffic_sensor --bootstrap-server localhost:9092
```