import json
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer

consumer = KafkaConsumer('PalavrasComP','PalavrasComS','PalavrasComR', 'PalavrasCom6','PalavrasCom8','PalavrasCom11', bootstrap_servers='localhost:9092')

p_x_vals = []
p_y_vals = []
p_batch = 0
p_y = 0

s_x_vals = []
s_y_vals = []
s_batch = 0
s_y = 0

r_x_vals = []
r_y_vals = []
r_batch = 0
r_y = 0

seis_x_vals = []
seis_y_vals = []
seis_batch = 0
seis_y = 0

oito_x_vals = []
oito_y_vals = []
oito_batch = 0
oito_y = 0

onze_x_vals = []
onze_y_vals = []
onze_batch = 0
onze_y = 0


while(True):
    for message in consumer:
        if (message.topic == 'PalavrasComP'):
            print(message.topic)
            y = int(message.value)
            p_y_vals.append(y - p_y)
            p_y = y

            p_batch = p_batch + 1
            p_x_vals.append(p_batch)

            df_test = pd.DataFrame({'palavrasComP': p_y_vals, 'batch': p_x_vals})
            print(df_test.to_string())
            df_test.to_csv('palavrasComP.csv', index=False)

        if (message.topic == 'PalavrasComS'):
            print(message.topic)
            y = int(message.value)
            s_y_vals.append(y - s_y)
            s_y = y

            s_batch = s_batch + 1
            s_x_vals.append(s_batch)

            df_test = pd.DataFrame({'palavrasComS': s_y_vals, 'batch': s_x_vals})
            print(df_test.to_string())
            df_test.to_csv('palavrasComS.csv', index=False)

        if (message.topic == 'PalavrasComR'):
            print(message.topic)
            y = int(message.value)
            r_y_vals.append(y - r_y)
            r_y = y

            r_batch = r_batch + 1
            r_x_vals.append(r_batch)

            df_test = pd.DataFrame({'palavrasComR': r_y_vals, 'batch': r_x_vals})
            print(df_test.to_string())
            df_test.to_csv('palavrasComR.csv', index=False)




        if (message.topic == 'PalavrasCom6'):
            print(message.topic)
            y = int(message.value)
            seis_y_vals.append(y - seis_y)
            seis_y = y

            seis_batch = seis_batch + 1
            seis_x_vals.append(seis_batch)

            df_test = pd.DataFrame({'palavrasCom6': seis_y_vals, 'batch': seis_x_vals})
            print(df_test.to_string())
            df_test.to_csv('palavrasCom6.csv', index=False)

        if (message.topic == 'PalavrasCom8'):
            print(message.topic)
            y = int(message.value)
            oito_y_vals.append(y - oito_y)
            oito_y = y

            oito_batch = oito_batch + 1
            oito_x_vals.append(oito_batch)

            df_test = pd.DataFrame({'palavrasCom8': oito_y_vals, 'batch': oito_x_vals})
            print(df_test.to_string())
            df_test.to_csv('palavrasCom8.csv', index=False)

        if (message.topic == 'PalavrasCom11'):
            print(message.topic)
            y = int(message.value)
            onze_y_vals.append(y - onze_y)
            onze_y = y

            onze_batch = onze_batch + 1
            onze_x_vals.append(onze_batch)

            df_test = pd.DataFrame({'palavrasCom11': onze_y_vals, 'batch': onze_x_vals})
            print(df_test.to_string())
            df_test.to_csv('palavrasCom11.csv', index=False)
