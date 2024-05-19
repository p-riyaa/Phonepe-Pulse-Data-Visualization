import os
import json
import pandas as pd
from pprint import pprint
import psycopg2

# Postgresql connection
try:
    conn = psycopg2.connect(
    host="localhost",
    database="phonepe_db",
    user="postgres",
    password="MyDB123")
    curr = conn.cursor()
    # Table Creation
    curr.execute('''create table if not exists agg_insr(State varchar(255), 
                                                        Year int,
                                                        Quarter int,
                                                        Transaction_type varchar(255),
                                                        Transaction_count bigint,
                                                        Transaction_amount bigint )''')
    curr.execute('''create table if not exists agg_tran(State varchar(255),
                                                                     Year int,
                                                                     Quarter int,
                                                                     Transaction_type varchar(255),
                                                                     Transaction_count bigint,
                                                                     Transaction_amount bigint )'''
                 )
    curr.execute('''create table if not exists agg_user(State varchar(255),
                                                    Year int,
                                                    Quarter int,
                                                    Brand varchar(255),
                                                    Transaction_count bigint,
                                                    Percentage float )'''
                 )
    curr.execute('''create table if not exists map_tran(State varchar(255),
                                                    Year int,
                                                    Quarter int,
                                                    District varchar(255),
                                                    Transaction_count bigint,
                                                    Transaction_amount bigint )'''
                 )
    curr.execute('''create table if not exists map_user(State varchar(255),
                                                    Year int,
                                                    Quarter int,
                                                    District varchar(255),
                                                    RegisteredUsers bigint,
                                                    AppOpens bigint )'''
                 )
    curr.execute('''create table if not exists map_insr(State varchar(255),
                                                        Year int,
                                                        Quarter int,
                                                        District varchar(255),
                                                        Transaction_count bigint,
                                                        Transaction_amount bigint )'''
                 )
    curr.execute('''create table if not exists top_tran(State varchar(255),
                                                        Year int,
                                                        Quarter int,
                                                        Pincodes varchar(255),
                                                        Transaction_count bigint,
                                                        Transaction_amount bigint )'''
                 )
    curr.execute('''create table if not exists top_insr(State varchar(255),
                                                        Year int,
                                                        Quarter int,
                                                        Pincodes varchar(255),
                                                        Transaction_count bigint,
                                                        Transaction_amount bigint )'''
                 )
    curr.execute('''create table if not exists top_user(State varchar(255),
                                                        Year int,
                                                        Quarter int,
                                                        Pincodes varchar(255),
                                                        registeredUsers bigint)'''
                 )
    conn.commit()
    # print("DB's creation are successfull")
    #Agg_Insur
    agg_insur_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/aggregated/insurance/country/india/state/"
    agg_insur_list = os.listdir(agg_insur_path)
    # print(agg_insur_list)

    clm_agginsr = {'State':[],'Year':[],'Quarter':[],'Name':[],'Transaction_count':[],'Transaction_amount':[]}
    for state in agg_insur_list:
        state_path = agg_insur_path+state+"/"
        years = os.listdir(state_path)
        for year in years:
            year_path = state_path+year+"/"
            data_file = os.listdir(year_path)
            for k in data_file:
                data_path = year_path+k
                data = open(data_path,'r')
                d = json.load(data)
    # pprint(d)
                for z in d['data']['transactionData']:
                  clm_agginsr['Name'].append(z['name'])
                  clm_agginsr['Transaction_count'].append(z['paymentInstruments'][0]['count'])
                  clm_agginsr['Transaction_amount'].append(z['paymentInstruments'][0]['amount'])
                  clm_agginsr['State'].append(state)
                  clm_agginsr['Year'].append(year)
                  clm_agginsr['Quarter'].append(int(k.strip('.json')))

    Agg_Insr=pd.DataFrame(clm_agginsr)
    Agg_Insr["State"] = Agg_Insr["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Agg_Insr["State"] = Agg_Insr["State"].str.replace("-"," ")
    Agg_Insr["State"] = Agg_Insr["State"].str.title()
    Agg_Insr['State'] = Agg_Insr['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Agg_Insr)
    #Insert Agg Insr list into backend
    insert = '''INSERT INTO agg_insr(State,Year,Quarter,Transaction_type,Transaction_count,Transaction_amount)
                                      VALUES(%s,%s,%s,%s,%s,%s)'''
    data = Agg_Insr.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("Agg_Insr data are inserted to table")

    #aggregated_transaction
    agg_tran_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/aggregated/transaction/country/india/state/"
    agg_tran_list = os.listdir(agg_tran_path)
    # agg_tran_list

    clm1 = {'State':[],'Year':[],'Quarter':[],'Transaction_type':[],'Transaction_count':[],'Transaction_amount':[]}

    for state in agg_tran_list:
      state_path = agg_tran_path+state+"/"
      years = os.listdir(state_path)
      for year in years:
        year_path = state_path+year+"/"
        data_file = os.listdir(year_path)
        for k in data_file:
          data_path = year_path+k
          data = open(data_path,'r')
          d = json.load(data)
          # pprint(d)
          for z in d['data']['transactionData']:
                  Name=z['name']
                  count=z['paymentInstruments'][0]['count']
                  amount=z['paymentInstruments'][0]['amount']
                  clm1['Transaction_type'].append(Name)
                  clm1['Transaction_count'].append(count)
                  clm1['Transaction_amount'].append(amount)
                  clm1['State'].append(state)
                  clm1['Year'].append(year)
                  clm1['Quarter'].append(int(k.strip('.json')))
    Agg_Trans=pd.DataFrame(clm1)
    Agg_Trans["State"] = Agg_Trans["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Agg_Trans["State"] = Agg_Trans["State"].str.replace("-"," ")
    Agg_Trans["State"] = Agg_Trans["State"].str.title()
    Agg_Trans['State'] = Agg_Trans['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Agg_Trans)

    #Insert Agg_Trans into backend
    insert = '''INSERT INTO agg_tran(State,Year,Quarter,Transaction_type,Transaction_count,Transaction_amount)
                                      VALUES(%s,%s,%s,%s,%s,%s)'''
    data = Agg_Trans.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("Agg_Trans data are inserted to table")
    #aggregated_user
    agg_user_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/aggregated/user/country/india/state/"
    agg_user_list = os.listdir(agg_tran_path)
    # print(agg_user_list)

    clm2 = {'State':[],'Year':[],'Quarter':[],'Brand':[],'Transaction_count':[],'Percentage':[]}
    for state in agg_user_list:
      state_path = agg_user_path+state+"/"
      years = os.listdir(state_path)
      for year in years:
        year_path = state_path+year+"/"
        data_file = os.listdir(year_path)
        for k in data_file:
          data_path = year_path+k
          data = open(data_path,'r')
          d = json.load(data)
          # pprint(d)
          if d['data']['usersByDevice'] != None:
            for z in d['data']['usersByDevice']:
                  clm2['Brand'].append(z['brand'])
                  clm2['Transaction_count'].append(z['count'])
                  clm2['Percentage'].append(z['percentage'])
                  clm2['State'].append(state)
                  clm2['Year'].append(year)
                  clm2['Quarter'].append(int(k.strip('.json')))

    Agg_User=pd.DataFrame(clm2)
    Agg_User["State"] = Agg_User["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Agg_User["State"] = Agg_User["State"].str.replace("-"," ")
    Agg_User["State"] = Agg_User["State"].str.title()
    Agg_User['State'] = Agg_User['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Agg_User)

    #Insert Agg_User into backend

    insert = '''INSERT INTO agg_user(State,Year,Quarter,Brand,Transaction_count,Percentage)
                                      VALUES(%s,%s,%s,%s,%s,%s)'''
    data = Agg_User.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("Agg_ user are inserted to table")
    #map_tran
    map_tran_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/map/transaction/hover/country/india/state/"
    map_tran_list = os.listdir(map_tran_path)
    # print(map_tran_list)

    clm3 = {'State':[],'Year':[],'Quarter':[],'District':[],'Transaction_count':[],'Transaction_amount':[]}
    for state in map_tran_list:
      state_path = map_tran_path+state+"/"
      years = os.listdir(state_path)
      for year in years:
        year_path = state_path+year+"/"
        data_file = os.listdir(year_path)
        for k in data_file:
          data_path = year_path+k
          data = open(data_path,'r')
          d = json.load(data)
          # pprint(d)
          for z in d['data']['hoverDataList']:
                  clm3['District'].append(z['name'])
                  clm3['Transaction_count'].append(z['metric'][0]['count'])
                  clm3['Transaction_amount'].append(z['metric'][0]['amount'])
                  clm3['State'].append(state)
                  clm3['Year'].append(year)
                  clm3['Quarter'].append(int(k.strip('.json')))

    Map_Tran=pd.DataFrame(clm3)
    Map_Tran["State"] = Map_Tran["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Map_Tran["State"] = Map_Tran["State"].str.replace("-"," ")
    Map_Tran["State"] = Map_Tran["State"].str.title()
    Map_Tran['State'] = Map_Tran['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Map_Tran)

    #Insert Map_Tran into backend

    insert = '''INSERT INTO map_tran(State,Year,Quarter,District,Transaction_count,Transaction_amount)
                                      VALUES(%s,%s,%s,%s,%s,%s)'''
    data = Map_Tran.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("Map tran data are inserted to table")
    #map_user
    map_user_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/map/user/hover/country/india/state/"
    map_user_list = os.listdir(map_user_path)
    # print(map_user_list)

    clm4 = {'State':[],'Year':[],'Quarter':[],'District':[],'User_count':[],'appOpens':[]}
    for state in map_user_list:
      state_path = map_user_path+state+"/"
      years = os.listdir(state_path)
      for year in years:
        year_path = state_path+year+"/"
        data_file = os.listdir(year_path)
        for k in data_file:
          data_path = year_path+k
          data = open(data_path,'r')
          d = json.load(data)
          # pprint(d)
          # if d['data']['usersByDevice'] != None:
          for z in d['data']['hoverData'].items():
                  clm4['District'].append(z[0])
                  clm4['User_count'].append(z[1]['registeredUsers'])
                  clm4['appOpens'].append(z[1]['appOpens'])
                  clm4['State'].append(state)
                  clm4['Year'].append(year)
                  clm4['Quarter'].append(int(k.strip('.json')))

    Map_User=pd.DataFrame(clm4)
    Map_User["State"] = Map_User["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Map_User["State"] = Map_User["State"].str.replace("-"," ")
    Map_User["State"] = Map_User["State"].str.title()
    Map_User['State'] = Map_User['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Map_User)

    #Insert Map_User into backend
    insert = '''INSERT INTO map_user(State,Year,Quarter,District,registeredUsers,appOpens)
                                      VALUES(%s,%s,%s,%s,%s,%s)'''
    data = Map_User.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("map user data are inserted to table")

    #map_insr
    map_insr_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/map/insurance/hover/country/india/state/"
    map_insr_list = os.listdir(map_insr_path)
    # print(map_insr_list)

    clm5 = {'State':[],'Year':[],'Quarter':[],'District':[],'Transaction_count':[],'Transaction_amount':[]}
    for state in map_insr_list:
      state_path = map_insr_path+state+"/"
      years = os.listdir(state_path)
      for year in years:
        year_path = state_path+year+"/"
        data_file = os.listdir(year_path)
        for k in data_file:
          data_path = year_path+k
          data = open(data_path,'r')
          d = json.load(data)
          # pprint(d)
          for z in d['data']['hoverDataList']:
                  clm5['District'].append(z['name'])
                  clm5['Transaction_count'].append(z['metric'][0]['count'])
                  clm5['Transaction_amount'].append(z['metric'][0]['amount'])
                  clm5['State'].append(state)
                  clm5['Year'].append(year)
                  clm5['Quarter'].append(int(k.strip('.json')))

    Map_Insr=pd.DataFrame(clm5)
    Map_Insr["State"] = Map_Insr["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Map_Insr["State"] = Map_Insr["State"].str.replace("-"," ")
    Map_Insr["State"] = Map_Insr["State"].str.title()
    Map_Insr['State'] = Map_Insr['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Map_Insr)

    #Insert Map_Insr to backend
    insert = '''INSERT INTO map_insr(State,Year,Quarter,District,Transaction_count,Transaction_amount)
                                      VALUES(%s,%s,%s,%s,%s,%s)'''
    data = Map_Insr.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("map_Insr data are inserted to table")

    #top_tran
    top_tran_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/top/transaction/country/india/state/"
    top_tran_list = os.listdir(top_tran_path)
    # print(top_tran_list)

    clm6 = {'State':[],'Year':[],'Quarter':[],'Pincodes':[],'Transaction_count':[],'Transaction_amount':[]}
    for state in top_tran_list:
      state_path = top_tran_path+state+"/"
      years = os.listdir(state_path)
      for year in years:
        year_path = state_path+year+"/"
        data_file = os.listdir(year_path)
        for k in data_file:
          data_path = year_path+k
          data = open(data_path,'r')
          d = json.load(data)
          # pprint(d)
          for z in d['data']['pincodes']:
                  clm6['Pincodes'].append(z['entityName'])
                  clm6['Transaction_count'].append(z['metric']['count'])
                  clm6['Transaction_amount'].append(z['metric']['amount'])
                  clm6['State'].append(state)
                  clm6['Year'].append(year)
                  clm6['Quarter'].append(int(k.strip('.json')))

    Top_Tran=pd.DataFrame(clm6)
    Top_Tran["State"] = Top_Tran["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Top_Tran["State"] = Top_Tran["State"].str.replace("-"," ")
    Top_Tran["State"] = Top_Tran["State"].str.title()
    Top_Tran['State'] = Top_Tran['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Top_Tran)

    #Insert Top_Tran to backend
    insert = '''INSERT INTO top_tran(State,Year,Quarter,Pincodes,Transaction_count,Transaction_amount)
                                      VALUES(%s,%s,%s,%s,%s,%s)'''
    data = Top_Tran.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("top_tran data are inserted to table")

    #top_insr
    top_insr_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/top/insurance/country/india/state/"
    top_insr_list = os.listdir(top_insr_path)
    # print(top_insr_list)

    clm6 = {'State':[],'Year':[],'Quarter':[],'Pincodes':[],'Transaction_count':[],'Transaction_amount':[]}
    for state in top_insr_list:
      state_path = top_insr_path+state+"/"
      years = os.listdir(state_path)
      for year in years:
        year_path = state_path+year+"/"
        data_file = os.listdir(year_path)
        for k in data_file:
          data_path = year_path+k
          data = open(data_path,'r')
          d = json.load(data)
          # pprint(d)
          for z in d['data']['pincodes']:
                  clm6['Pincodes'].append(z['entityName'])
                  clm6['Transaction_count'].append(z['metric']['count'])
                  clm6['Transaction_amount'].append(z['metric']['amount'])
                  clm6['State'].append(state)
                  clm6['Year'].append(year)
                  clm6['Quarter'].append(int(k.strip('.json')))

    Top_Insr=pd.DataFrame(clm6)
    Top_Insr["State"] = Top_Insr["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Top_Insr["State"] = Top_Insr["State"].str.replace("-"," ")
    Top_Insr["State"] = Top_Insr["State"].str.title()
    Top_Insr['State'] = Top_Insr['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Top_Insr)

    #Insert Top_Insr to backend
    insert = '''INSERT INTO top_insr(State,Year,Quarter,Pincodes,Transaction_count,Transaction_amount)
                                      VALUES(%s,%s,%s,%s,%s,%s)'''
    data = Top_Insr.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("Top_Insr data are inserted to table")

    #top_user
    top_user_path = "C:/Users/Pooja/Desktop/PhonePe Data/pulse/data/top/user/country/india/state/"
    top_user_list = os.listdir(top_user_path)
    # print(top_user_list)

    clm7 = {'State':[],'Year':[],'Quarter':[],'Pincodes':[],'registeredUsers':[]}
    for state in top_user_list:
      state_path = top_user_path+state+"/"
      years = os.listdir(state_path)
      for year in years:
        year_path = state_path+year+"/"
        data_file = os.listdir(year_path)
        for k in data_file:
          data_path = year_path+k
          data = open(data_path,'r')
          d = json.load(data)
          # pprint(d)
          for z in d['data']['pincodes']:
                  clm7['Pincodes'].append(z['name'])
                  clm7['registeredUsers'].append(z['registeredUsers'])
                  clm7['State'].append(state)
                  clm7['Year'].append(year)
                  clm7['Quarter'].append(int(k.strip('.json')))

    Top_User=pd.DataFrame(clm7)
    Top_User["State"] = Top_User["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    Top_User["State"] = Top_User["State"].str.replace("-"," ")
    Top_User["State"] = Top_User["State"].str.title()
    Top_User['State'] = Top_User['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    # print(Top_User)

    #Insert top_user to backend
    insert = '''INSERT INTO top_user(State,Year,Quarter,Pincodes,registeredUsers)
                                      VALUES(%s,%s,%s,%s,%s)'''
    data = Top_User.values.tolist()
    curr.executemany(insert, data)
    conn.commit()
    print("top user data are inserted to table")


except Exception as error:
    print(error)
    print("exception occurred")


