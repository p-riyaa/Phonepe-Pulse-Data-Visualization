import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
import pandas as pd
import requests
import json
import plotly_express as px
from PIL import Image

#Data Frame Creation
#Postgres Connection
conn = psycopg2.connect(
    host="localhost",
    database="phonepe_db",
    user="postgres",
    password="MyDB123")
curr = conn.cursor()

#Aggregated Insurance DF

curr.execute("SELECT * FROM agg_insr")
conn.commit()
table1 = curr.fetchall()
agg_insr_df = pd.DataFrame(table1, columns = ("State","Year","Quarter","Transaction_type","Transaction_count","Transaction_amount"))
# print(agg_insr_df)

#Aggregated Transaction DF

curr.execute("SELECT * FROM agg_tran")
conn.commit()
table2 = curr.fetchall()
agg_tran_df = pd.DataFrame(table2, columns = ("State","Year","Quarter","Transaction_type","Transaction_count","Transaction_amount"))
# print(agg_tran_df)

#Aggregated User DF

curr.execute("SELECT * FROM agg_user")
conn.commit()
table3 = curr.fetchall()
agg_user_df = pd.DataFrame(table3, columns = ("State","Year","Quarter","Brands","Transaction_count","Percentage"))
# print(agg_user_df)

#Map Insurance DF

curr.execute("SELECT * FROM map_insr")
conn.commit()
table4 = curr.fetchall()
map_insr_df = pd.DataFrame(table4, columns = ("State","Year","Quarter","District","Transaction_count","Transaction_amount"))
# print(map_insr_df)

#Map Transaction DF

curr.execute("SELECT * FROM map_tran")
conn.commit()
table5 = curr.fetchall()
map_tran_df = pd.DataFrame(table5, columns = ("State","Year","Quarter","District","Transaction_count","Transaction_amount"))
# print(map_tran_df)

#Map User DF

curr.execute("SELECT * FROM map_user")
conn.commit()
table6 = curr.fetchall()
map_user_df = pd.DataFrame(table6, columns = ("State","Year","Quarter","District","RegisteredUsers","AppOpens"))
# print(map_user_df)

#Top Insurance DF

curr.execute("SELECT * FROM top_insr")
conn.commit()
table7= curr.fetchall()
top_insr_df = pd.DataFrame(table7, columns = ("State","Year","Quarter","Pincodes","Transaction_count","Transaction_amount"))
# print(top_insr_df)

#Top Transaction DF

curr.execute("SELECT * FROM top_tran")
conn.commit()
table8= curr.fetchall()
top_tran_df = pd.DataFrame(table8, columns = ("State","Year","Quarter","Pincodes","Transaction_count","Transaction_amount"))
# print(top_tran_df)

#Top User DF

curr.execute("SELECT * FROM top_user")
conn.commit()
table9= curr.fetchall()
top_user_df = pd.DataFrame(table9, columns = ("State","Year","Quarter","Pincodes","RegisteredUsers"))
# print(top_user_df)

#Analysis for Transaction amount/count
def transaction_amount_count_in_year(df,years):
    sep1 = (df[df["Year"] == years])
    sep1.reset_index(drop=True,inplace=True)
    # print(sep)

    #Grouping data based on state and summing up the tran count & count for that state per year
    sepgroup1 = sep1.groupby("State")[["Transaction_count","Transaction_amount"]].sum()
    sepgroup1.reset_index(inplace=True)
    # print(sepgroup1)
    col1,col2 = st.columns(2)
    #Bar Plot for tran amt and count
    plot_amt = px.bar(sepgroup1, x="State",y="Transaction_amount", title=f"{years} TRANSACTION AMOUNT",
                      color_discrete_sequence=px.colors.sequential.Cividis_r,height=600,width=650)
    plot_count = px.bar(sepgroup1, x="State",y="Transaction_count", title=f"{years} TRANSACTION COUNT",
                      color_discrete_sequence=px.colors.sequential.Magenta_r,height=600,width=650)
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    # print(response)
    data1 = json.loads(response.content)
    state_name = []
    for feature in data1["features"]:
        state_name.append(feature["properties"]["ST_NM"])
    # print(state_name)

    fig_india1 = px.choropleth(sepgroup1, geojson=data1, locations="State", featureidkey="properties.ST_NM",
                               color="Transaction_amount", color_continuous_scale="Twilight",
                               range_color=(
                               sepgroup1["Transaction_amount"].min(), sepgroup1["Transaction_amount"].min()),
                               hover_name="State", title=f"{years} TRANSACTION AMOUNT", fitbounds="locations",
                               height=600, width=650)
    fig_india1.update_geos(visible=False)
    fig_india2 = px.choropleth(sepgroup1, geojson=data1, locations="State", featureidkey="properties.ST_NM",
                               color="Transaction_count", color_continuous_scale="Twilight",
                               range_color=(
                                   sepgroup1["Transaction_count"].min(), sepgroup1["Transaction_count"].min()),
                               hover_name="State", title=f"{years} TRANSACTION COUNT", fitbounds="locations",
                               height=600, width=650)
    fig_india2.update_geos(visible=False)
    with col1:
        st.plotly_chart(plot_amt)
        st.plotly_chart(fig_india1)
    with col2:
        st.plotly_chart(plot_count)
        st.plotly_chart(fig_india2)
    return (sep1)

#Analysis for Transaction amount/count per quarter
def transaction_amount_count_in_quarter(df,quarter):
    sep1 = (df[df["Quarter"] == quarter])
    sep1.reset_index(drop=True,inplace=True)
    # print(sep)

    #Grouping data based on state and summing up the tran count & count for that state per year
    sepgroup1 = sep1.groupby("State")[["Transaction_count","Transaction_amount"]].sum()
    sepgroup1.reset_index(inplace=True)
    # print(sepgroup1)

    #Bar Plot for tran amt and count
    plot_amt = px.bar(sepgroup1, x="State",y="Transaction_amount",
                      title=f"{forquarter["Year"].min()} Quarter {quarter} STATES TRANSACTION AMOUNT",
                      color_discrete_sequence=px.colors.sequential.Cividis_r,height=600, width=600)
    plot_count = px.bar(sepgroup1, x="State",y="Transaction_count",
                        title=f"{forquarter["Year"].min()} Quarter {quarter} STATES TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Magenta_r,height=600, width=600)

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    # print(response)
    data1 = json.loads(response.content)
    state_name =[]
    for feature in data1["features"]:
        state_name.append(feature["properties"]["ST_NM"])
    # print(state_name)

    fig_india1 = px.choropleth(sepgroup1, geojson=data1, locations="State", featureidkey="properties.ST_NM",
                               color="Transaction_amount",color_continuous_scale="Twilight",
                               range_color=(sepgroup1["Transaction_amount"].min(),sepgroup1["Transaction_amount"].min()),
                               hover_name="State", title=f"{forquarter["Year"].min()} Quarter {quarter} TRANSACTION AMOUNT",
                               fitbounds="locations",
                               height=600, width=600)
    fig_india1.update_geos(visible=False)
    fig_india2 = px.choropleth(sepgroup1, geojson=data1, locations="State", featureidkey="properties.ST_NM",
                               color="Transaction_count", color_continuous_scale="Twilight",
                               range_color=(
                               sepgroup1["Transaction_count"].min(), sepgroup1["Transaction_count"].min()),
                               hover_name="State", title=f"{forquarter["Year"].min()} Quarter {quarter} TRANSACTION COUNT",
                               fitbounds="locations",
                               height=600, width=600)
    fig_india2.update_geos(visible=False)
    col1,col2 = st.columns(2)
    with col1:
        st.plotly_chart(plot_amt)
        st.plotly_chart(fig_india1)
    with col2:
        print("\n")
        st.plotly_chart(plot_count)
        st.plotly_chart(fig_india2)
    return sep1

def agg_tran_for_tran_type(df, state):
    sep1 = (df[df["State"] == state])
    sep1.reset_index(drop=True, inplace=True)
    # print(sep1)
    # Grouping data based on Transaction_count and Transaction_amount for tran type
    sepgroup1 = sep1.groupby("Transaction_type")[["Transaction_count", "Transaction_amount"]].sum()
    sepgroup1.reset_index(inplace=True)
    # print(sepgroup1)
    col1,col2 = st.columns(2)
    with col1:
        agg_tran_pie_amt = px.pie(data_frame = sep1, names = "Transaction_type", values = "Transaction_amount",
                              width = 600, title = f"{state.upper()} TRANSACTION AMOUNT per TYPE", hole = 0.65 )
        st.plotly_chart(agg_tran_pie_amt)
    with col2:
        agg_tran_pie_cnt = px.pie(data_frame = sep1, names = "Transaction_type", values = "Transaction_count",
                              width = 600, title = f"{state.upper()} TRANSACTION COUNT per TYPE", hole = 0.65 )
        st.plotly_chart(agg_tran_pie_cnt)
    return sep1
def agg_user_plt1(df,year):
    sep3 = df[df["Year"]==year]
    sep3.reset_index(drop=True,inplace=True)
    # print(sep3)
    sep3_group = pd.DataFrame(sep3.groupby("Brands")["Transaction_count"].sum())
    sep3_group.reset_index(inplace=True)
    # print(sep3_group)
    plot_bar_1 = px.bar(sep3_group,x="Brands",y="Transaction_count", title = f"{year} Transaction Count based on Brands",
                        width= 800, color_discrete_sequence=px.colors.sequential.haline, hover_name="Brands")
    st.plotly_chart(plot_bar_1)
    return sep3
def agg_user_plt1_for_quarter(df,quarter):
    sep3 = df[df["Quarter"]==quarter]
    sep3.reset_index(drop=True,inplace=True)
    # print(sep3)
    sep3_group = pd.DataFrame(sep3.groupby("Brands")["Transaction_count"].sum())
    sep3_group.reset_index(inplace=True)
    # print(sep3_group)
    plot_bar_1 = px.bar(sep3_group,x="Brands",y="Transaction_count", title = f"Quarter {quarter} Transaction Count based on Brands",
                        width= 800, color_discrete_sequence=px.colors.sequential.haline, hover_name="Brands")
    st.plotly_chart(plot_bar_1)
    return sep3
def agg_user_plt1_for_state(df,state):
    sep3 = df[df["State"] == state]
    sep3.reset_index(drop=True, inplace=True)
    print(sep3)
    plot_line_1 = px.line(sep3, x="Brands", y="Transaction_count", hover_data="Percentage",
                        title=f"{state.upper()} STATE TRANSACTION COUNT FOR EACH BRAND", markers= True,
                        width=800, color_discrete_sequence=px.colors.sequential.haline, hover_name="Brands")
    st.plotly_chart(plot_line_1)
    return sep3

#Map Transaction Analysis
#map_transaction_amount_count_in_year
def map_insr_district(df, state):
    sep1 = (df[df["State"] == state])
    sep1.reset_index(drop=True, inplace=True)

    # Grouping data based on Transaction_count and Transaction_amount for tran type
    sepgroup1 = sep1.groupby("District")[["Transaction_count", "Transaction_amount"]].sum()
    sepgroup1.reset_index(inplace=True)
    col1,col2 = st.columns(2)
    with col1:
        plt_bar_amt = px.bar(data_frame = sepgroup1, x = "Transaction_amount", y = "District", orientation= "h",
                              title = f"{state.upper()} DISTRICT WISE TRANSACTION AMOUNT", height= 600,
                             color_discrete_sequence=px.colors.sequential.ice)
        st.plotly_chart(plt_bar_amt)
    with col2:
        plt_bar_cnt = px.bar(data_frame = sepgroup1, x = "Transaction_count", y = "District", orientation= "h",
                            title = f"{state.upper()} DISTRICT WISE TRANSACTION COUNT", height= 600,
                             color_discrete_sequence=px.colors.sequential.Brwnyl_r)
        st.plotly_chart(plt_bar_cnt)
    return sep1

#Map User Analysis
def map_user_analysis(df,year):
    sep3 = df[df["Year"] == year]
    sep3.reset_index(drop=True, inplace=True)
    # print(sep3)

    sepgrp = sep3.groupby("State")[["RegisteredUsers","AppOpens"]].sum()
    sepgrp.reset_index(inplace=True)
    # print(sepgrp)

    plot_line_1 = px.line(sepgrp, x="State", y=["RegisteredUsers","AppOpens"],
                          title=f"{year} REGISTERED USERS & APP OPENS", markers=True,
                          width=800, color_discrete_sequence=px.colors.sequential.haline)
    st.plotly_chart(plot_line_1)
    return sep3

def map_user_analysis_for_quarter(df,quarters):
    sep3 = df[df["Quarter"] == quarters]
    sep3.reset_index(drop=True, inplace=True)
    # print(sep3)

    sepgrp = sep3.groupby("State")[["RegisteredUsers","AppOpens"]].sum()
    sepgrp.reset_index(inplace=True)
    # print(sepgrp)

    plot_line_1 = px.line(sepgrp, x="State", y=["RegisteredUsers","AppOpens"],
                          title=f"{years} {quarters} REGISTERED USERS & APP OPENS", markers=True,
                          width=800, color_discrete_sequence=px.colors.sequential.haline)
    st.plotly_chart(plot_line_1)
    return sep3

def map_user_analysis_statewise(df,state):
    sep3 = df[df["State"] == state]
    sep3.reset_index(drop=True, inplace=True)
    print(sep3)

    col1,col2 = st.columns(2)
    with col1:
        plot_line_1 = px.bar(sep3, x="RegisteredUsers", y="District", orientation= "h",
                              title=f"{state.upper()} STATE REGISTERED USERS", height= 800,
                              color_discrete_sequence=px.colors.sequential.Rainbow)
        st.plotly_chart(plot_line_1)
    with col2:
        plot_line_2 = px.bar(sep3, x="AppOpens", y="District", orientation="h",
                             title=f"{state.upper()} STATE APPOPENS", height=800,
                             color_discrete_sequence=px.colors.sequential.Rainbow)
        st.plotly_chart(plot_line_2)

def top_insr_analysis_for_state(df,state):
    sep3 = df[df["State"] == state]
    sep3.reset_index(drop=True, inplace=True)

    colx,coly = st.columns(2)
    with colx:
        plot_line_1 = px.bar(sep3, x="Quarter", y="Transaction_amount", hover_data="Pincodes",
                              title=f"{state.upper()} STATE TRANSACTION AMOUNT",
                              height=650, width= 400, color_discrete_sequence=px.colors.sequential.Rainbow)
        st.plotly_chart(plot_line_1)
    with coly:
        plot_line_2 = px.bar(sep3, x="Quarter", y="Transaction_count", hover_data="Pincodes",
                             title=f"{state.upper()} STATE TRANSACTION COUNT",
                             height=650, width= 400,  color_discrete_sequence=px.colors.sequential.haline)
        st.plotly_chart(plot_line_2)

#TOP USER
def top_user_analysis(df,year):
    sep3 = df[df["Year"] == year]
    sep3.reset_index(drop=True, inplace=True)
    # print(sep3)

    sepgrp = pd.DataFrame(sep3.groupby(["State","Quarter"])["RegisteredUsers"].sum())
    sepgrp.reset_index(inplace=True)
    # print(sepgrp)

    plot_line_1 = px.bar(sepgrp, x="State", y="RegisteredUsers", color="Quarter",
                          title=f"{year} YEAR REGISTERED USERS", hover_name="State",
                          height=800, width=1000, color_discrete_sequence=px.colors.sequential.Rainbow)
    st.plotly_chart(plot_line_1)
    return sep3

def top_user_analysis_pincode(df,state):
    sep3 = df[df["State"] == state]
    sep3.reset_index(drop=True, inplace=True)

    plot_line_2 = px.bar(sep3, x="Quarter", y="RegisteredUsers",
                          title=f"{state} STATE INFORMATION", hover_name="State", color="RegisteredUsers",
                          height=800, width=1000, color_discrete_sequence=px.colors.sequential.Magenta)
    st.plotly_chart(plot_line_2)

def top_chart_transaction_amount(table):
    query1 = f'''select state, SUM(transaction_amount) as transaction_amount
    from {table}
    group by state
    order by transaction_amount desc
    Limit 10;'''

    curr.execute(query1)
    tab1 =curr.fetchall()
    conn.commit()

    col1, col2 = st.columns(2)
    with col1:
        df1 = pd.DataFrame(tab1,columns=("State","transaction_amount"))
        plot_line_1 = px.bar(df1, x="State", y="transaction_amount",
                                  title="TOP 10 States Transaction Amount", hover_name="State",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta)
        st.plotly_chart(plot_line_1)

    query2 = f'''select state, SUM(transaction_amount) as transaction_amount
    from {table}
    group by state
    order by transaction_amount
    Limit 10;'''

    curr.execute(query2)
    tab2 =curr.fetchall()
    conn.commit()
    with col2:
        df2 = pd.DataFrame(tab2,columns=("State","transaction_amount"))
        plot_line_2 = px.bar(df2, x="State", y="transaction_amount",
                                  title="LEAST 10 States Transaction Amount", hover_name="State",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta_r)
        st.plotly_chart(plot_line_2)

    query3 = f'''select state, AVG(transaction_amount) as transaction_amount
    from {table}
    group by state
    order by transaction_amount;'''

    curr.execute(query3)
    tab3 =curr.fetchall()
    conn.commit()

    df3 = pd.DataFrame(tab3,columns=("State","transaction_amount"))
    plot_line_3 = px.bar(df3, x="transaction_amount", y="State",
                              title="States Average Transaction Amount", hover_name="State", orientation= "h",
                              height=650, width=800, color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(plot_line_3 )

def top_chart_transaction_count(table):
    query1 = f'''select state, SUM(transaction_count) as transaction_count
    from {table}
    group by state
    order by transaction_count desc
    Limit 10;'''

    curr.execute(query1)
    tab1 =curr.fetchall()
    conn.commit()

    col1, col2 = st.columns(2)
    with col1:
        df1 = pd.DataFrame(tab1,columns=("State","transaction_count"))
        plot_line_1 = px.bar(df1, x="State", y="transaction_count",
                                  title="TOP 10 States Transaction Count", hover_name="State",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta)
        st.plotly_chart(plot_line_1)

    query2 = f'''select state, SUM(transaction_count) as transaction_count
    from {table}
    group by state
    order by transaction_count
    Limit 10;'''

    curr.execute(query2)
    tab2 =curr.fetchall()
    conn.commit()
    with col2:
        df2 = pd.DataFrame(tab2,columns=("State","transaction_count"))
        plot_line_2 = px.bar(df2, x="State", y="transaction_count",
                                  title="LEAST 10 States Transaction Count", hover_name="State",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta_r)
        st.plotly_chart(plot_line_2)

    query3 = f'''select state, AVG(transaction_count) as transaction_count
    from {table}
    group by state
    order by transaction_count;'''

    curr.execute(query3)
    tab3 =curr.fetchall()
    conn.commit()

    df3 = pd.DataFrame(tab3,columns=("State","transaction_count"))
    plot_line_3 = px.bar(df3, x="transaction_count", y="State",
                              title="States Average Transaction Count", hover_name="State", orientation= "h",
                              height=650, width=800, color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(plot_line_3 )

def top_chart_registered_user(table,state):
    query1 = f'''select district,SUM(registeredusers) AS registeredusers 
    from {table}
    where state = '{state}'
    group by district
    order by registeredusers DESC
    Limit 10;'''

    curr.execute(query1)
    tab1 =curr.fetchall()
    conn.commit()

    col1, col2 = st.columns(2)
    with col1:
        df1 = pd.DataFrame(tab1,columns=("district","registeredusers"))
        plot_line_1 = px.bar(df1, x="district", y="registeredusers",
                                  title="TOP 10 REGISTERED USER", hover_name="district",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta)
        st.plotly_chart(plot_line_1)

    query2 = f'''select district,SUM(registeredusers) AS registeredusers 
    from {table}
    where state = '{state}'
    group by district
    order by registeredusers
    Limit 10;'''

    curr.execute(query2)
    tab2 =curr.fetchall()
    conn.commit()

    with col2:
        df2 = pd.DataFrame(tab2,columns=("district","registeredusers"))
        plot_line_2 = px.bar(df2, x="district", y="registeredusers",
                                  title="LEAST 10 REGISTERED USERS", hover_name="district",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta_r)
        st.plotly_chart(plot_line_2)

    query3 = f'''select district,AVG(registeredusers) AS registeredusers 
    from {table}
    where state = '{state}'
    group by district
    order by registeredusers'''

    curr.execute(query3)
    tab3 =curr.fetchall()
    conn.commit()

    df3 = pd.DataFrame(tab3,columns=("district","registeredusers"))
    plot_line_3 = px.bar(df3, x="registeredusers", y="district",
                              title="STATES AVERAGE REGISTERED USER", hover_name="district", orientation= "h",
                              height=650, width=800, color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(plot_line_3)

def top_chart_appopens(table,state):
    query1 = f'''select district,SUM(appopens) AS appopens 
    from {table}
    where state = '{state}'
    group by district
    order by appopens DESC
    Limit 10;'''

    curr.execute(query1)
    tab1 =curr.fetchall()
    conn.commit()

    col1, col2 = st.columns(2)
    with col1:
        df1 = pd.DataFrame(tab1,columns=("district","appopens"))
        plot_line_1 = px.bar(df1, x="district", y="appopens",
                                  title="TOP 10 APPOPENS", hover_name="district",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta)
        st.plotly_chart(plot_line_1)

    query2 = f'''select district,SUM(appopens) AS appopens 
    from {table}
    where state = '{state}'
    group by district
    order by appopens
    Limit 10;'''

    curr.execute(query2)
    tab2 =curr.fetchall()
    conn.commit()

    with col2:
        df2 = pd.DataFrame(tab2,columns=("district","appopens"))
        plot_line_2 = px.bar(df2, x="district", y="appopens",
                                  title="LEAST 10 APPOPENS", hover_name="district",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta_r)
        st.plotly_chart(plot_line_2)

    query3 = f'''select district,AVG(appopens) AS appopens 
    from {table}
    where state = '{state}'
    group by district
    order by appopens'''

    curr.execute(query3)
    tab3 =curr.fetchall()
    conn.commit()

    df3 = pd.DataFrame(tab3,columns=("district","appopens"))
    plot_line_3 = px.bar(df3, x="appopens", y="district",
                              title="STATES AVERAGE APPOPENS", hover_name="district", orientation= "h",
                              height=650, width=800, color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(plot_line_3)

def top_chart_topuser(table):
    query1 = f'''select state, SUM(registeredusers) AS registeredusers
    from {table}
    group by state
    order by registeredusers DESC
    LIMIT 10;'''

    curr.execute(query1)
    tab1 =curr.fetchall()
    conn.commit()
    col1,col2 = st.columns(2)
    with col1:
        df1 = pd.DataFrame(tab1,columns=("state","registeredusers"))
        plot_line_1 = px.bar(df1, x="state", y="registeredusers",
                                  title="TOP 10 REGISTERED USER", hover_name="state",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta)
        st.plotly_chart(plot_line_1)

    query2 = f'''select state, SUM(registeredusers) AS registeredusers
    from {table}
    group by state
    order by registeredusers 
    LIMIT 10;'''

    curr.execute(query2)
    tab2 =curr.fetchall()
    conn.commit()

    with col2:
        df2 = pd.DataFrame(tab2,columns=("state","registeredusers"))
        plot_line_2 = px.bar(df2, x="state", y="registeredusers",
                                  title="LEAST 10 REGISTERED USERS", hover_name="state",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta_r)
        st.plotly_chart(plot_line_2)

    query3 = f'''select state, AVG(registeredusers) AS registeredusers
    from top_user
    group by state
    order by registeredusers;'''

    curr.execute(query3)
    tab3 =curr.fetchall()
    conn.commit()

    df3 = pd.DataFrame(tab3,columns=("state","registeredusers"))
    plot_line_3 = px.bar(df3, x="registeredusers", y="state",
                              title="STATES AVERAGE REGISTERED USER", hover_name="state", orientation= "h",
                              height=650, width=800, color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(plot_line_3)

def agg_user_brand(table,state):
    query1 = f'''select brand, SUM(transaction_count) AS transaction_count
    from {table}
    group by brand
    order by transaction_count DESC
    LIMIT 10;'''

    curr.execute(query1)
    tab1 =curr.fetchall()
    conn.commit()
    col1,col2 = st.columns(2)
    with col1:
        df1 = pd.DataFrame(tab1,columns=("brand","transaction_count"))
        plot_line_1 = px.bar(df1, x="brand", y="transaction_count",
                                  title="TOP 10 BRANDS", hover_name="brand",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta)
        st.plotly_chart(plot_line_1)

    query2 = f'''select brand, SUM(transaction_count) AS transaction_count
    from {table}
    group by brand
    order by transaction_count 
    LIMIT 10;'''

    curr.execute(query2)
    tab2 =curr.fetchall()
    conn.commit()

    with col2:
        df2 = pd.DataFrame(tab2,columns=("brand","transaction_count"))
        plot_line_2 = px.bar(df2, x="brand", y="transaction_count",
                                  title="LEAST 10 BRANDS", hover_name="brand",
                                  height=650, width=600, color_discrete_sequence=px.colors.sequential.Magenta_r)
        st.plotly_chart(plot_line_2)

    query3 = f'''select brand, AVG(transaction_count) AS transaction_count
    from {table}
    group by brand
    order by transaction_count;'''

    curr.execute(query3)
    tab3 =curr.fetchall()
    conn.commit()

    df3 = pd.DataFrame(tab3,columns=("brand","transaction_count"))
    plot_line_3 = px.bar(df3, x="transaction_count", y="brand",
                              title="AVERAGE OF BRANDS TRANSACTION COUNT", hover_name="brand", orientation= "h",
                              height=650, width=800, color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(plot_line_3)

def agg_tran_type(table,state):
    query1 = f'''SELECT transaction_type, SUM(transaction_amount) as transaction_count
    FROM {table}
    where state = '{state}'
    group by transaction_type
    order by transaction_count  ;'''


    curr.execute(query1)
    tabn =curr.fetchall()
    conn.commit()

    df1 = pd.DataFrame(tabn,columns=("transaction_type","transaction_amount"))
    plot_line_1 = px.bar(df1, x="transaction_type", y="transaction_amount",
                              title="STATES TRANSACTION TYPE & AMOUNT", hover_name="transaction_type",
                              height=650, width=800, color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(plot_line_1)

#UI Page
st.set_page_config(layout="wide")
st.title("PHONEPE DATA VISUALIZATION AND EXPLORATION")

with st.sidebar:
    select = option_menu("Menu Items",["Home","Data Exploration","Top charts"])
if select == "Home":
    col1,col2 = st.columns(2)
    with col1:
        st.header("PHONEPE")
        st.subheader(" * India's best Money Transaction Platform")
        st.subheader(" * Top Trends & Insights from India's Leading Payment App")
        st.subheader(" * Get the latest data trends and insights on PhonePe Pulse!")
        st.header("IN THIS APP")
        st.subheader(" * Discover the latest trend & data driven insights from Indian Insurance Market")
        st.subheader("check out the report here on what the future holds for digital payments in India")
    with col2:
        st.image(Image.open("C://Users//Pooja//Downloads//phonepeimg.jpeg"), width=500)
        st.image(Image.open("C://Users//Pooja//Downloads//phonepeimg2.jpeg"), width=500)
elif select == "Data Exploration":
    tab1,tab2,tab3 = st.tabs(["Aggregated Analysis","Map Analysis","Top Analysis"])

    with tab1:
        method1 = st.radio("Select the method",["Aggregated Insurance Analysis","Aggregated Transaction Analysis","Aggregated User Analysis"])
        if method1 == "Aggregated Insurance Analysis":
            col1,col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR", agg_insr_df["Year"].unique())
            # Function call  for Transaction amount/count per year
            forquarter = transaction_amount_count_in_year(agg_insr_df, years)
            col1, col2 = st.columns(2)
            with col1:
                quarters = st.selectbox("SELECT THE QUARTER",forquarter["Quarter"].unique())
            # Function call  for Transaction amount/count per Quarter
            quarter_result = transaction_amount_count_in_quarter(forquarter, quarters)

        elif method1 == "Aggregated Transaction Analysis":
            col1, col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR", agg_tran_df["Year"].unique())
            # Function call  for Transaction amount/count per year
            forquarter = transaction_amount_count_in_year(agg_tran_df, years)
            col1, col2 = st.columns(2)
            with col1:
                quarters = st.selectbox("SELECT THE QUARTER", forquarter["Quarter"].unique())
            # Function call  for Transaction amount/count per Quarter
            quarter_result = transaction_amount_count_in_quarter(forquarter, quarters)
            col1, col2 = st.columns(2)
            with col1:
                state_dropdown = st.selectbox("Select the State:",forquarter["State"].unique())
            agg_tran_for_tran_type(forquarter, state_dropdown )

            agg_tran_for_tran_type(quarter_result, state_dropdown)
        elif method1 == "Aggregated User Analysis":
            col1,col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR", agg_user_df["Year"].unique())
            # function call per year
                agg_user_analysis_y = agg_user_plt1(agg_user_df, years)
            col1, col2 = st.columns(2)
            with col1:
                quarter = st.selectbox("SELECT THE QUARTER", agg_user_df["Quarter"].unique())
            # function call per quarter
                agg_user_analysis_q = agg_user_plt1_for_quarter(agg_user_analysis_y, quarter)
            col1, col2 = st.columns(2)
            with col1:
                state_dropdown = st.selectbox("Select the State:", agg_user_df["State"].unique())
                #func call for state analysis
                agg_user_analysis_st = agg_user_plt1_for_state(agg_user_analysis_q, state_dropdown)
    with tab2:
        method2 = st.radio("Select the method", ["Map Insurance Analysis", "Map Transaction Analysis", "Map User Analysis"])
        if method2 == "Map Insurance Analysis":
            col1,col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR ", map_insr_df["Year"].unique())
            forquarter = transaction_amount_count_in_year(map_insr_df, years)
            col1, col2 = st.columns(2)
            with col1:
                state_dropdown = st.selectbox("Select the State:", forquarter["State"].unique())
            district_analysis = map_insr_district(forquarter, state_dropdown)
            col1, col2 = st.columns(2)
            with col1:
                quarters = st.selectbox("SELECT THE QUARTER", map_insr_df["Quarter"].unique())
            quarter_result = transaction_amount_count_in_quarter(forquarter, quarters)
            col1, col2 = st.columns(2)
            with col1:
                state_dropdown = st.selectbox("Select the State", quarter_result["State"].unique())
            district_analysis = map_insr_district(quarter_result, state_dropdown)
        elif method2 == "Map Transaction Analysis":
            col1, col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR ", map_tran_df["Year"].unique())
            forquarter = transaction_amount_count_in_year(map_tran_df, years)
            with col2:
                quarters = st.selectbox("SELECT THE QUARTER FOR MAP TRANSACTION ANALYSIS ", map_tran_df["Quarter"].unique())
            quarter_result = transaction_amount_count_in_quarter(forquarter, quarters)
            col1, col2 = st.columns(2)
            with col1:
                state_dropdown = st.selectbox("Select the State:", forquarter["State"].unique())
            district_analysis = map_insr_district(forquarter, state_dropdown)
            col1, col2 = st.columns(2)
            with col1:
                quarters = st.selectbox("SELECT THE QUARTER FOR MAP TRANSACTION ANALYSIS", map_tran_df["Quarter"].unique())
            quarter_result = transaction_amount_count_in_quarter(forquarter, quarters)
            col1, col2 = st.columns(2)
            with col1:
                state_dropdown = st.selectbox("Select the State", quarter_result["State"].unique())
            district_analysis = map_insr_district(quarter_result, state_dropdown)
        elif method2 == "Map User Analysis":
            col1, col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR ", map_user_df["Year"].unique())
            forquarter = map_user_analysis(map_user_df, years)
            col1, col2 = st.columns(2)
            with col1:
                quarters = st.selectbox("SELECT THE QUARTER ", map_user_df["Quarter"].unique())
            quarter_result = map_user_analysis_for_quarter(forquarter, quarters)
            col1, col2 = st.columns(2)
            with col1:
                stateselect = st.selectbox("SELECT THE STATE",quarter_result["State"].unique())
            map_user_analysis_quarter = map_user_analysis_statewise(quarter_result, stateselect)



    with tab3:
        method3 = st.radio("Select the method", ["Top Insurance Analysis", "Top Transaction Analysis", "Top User Analysis"])
        if method3 == "Top Insurance Analysis":
            col1, col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR   ", top_insr_df["Year"].unique())
            forquarter = transaction_amount_count_in_year(top_insr_df, years)
            col1, col2 = st.columns(2)
            with col1:
                quarters = st.selectbox("SELECT THE QUARTER ", forquarter["Quarter"].unique())
            top_insr_quarter = transaction_amount_count_in_quarter(forquarter, quarters)
            col1, col2 = st.columns(2)
            with col1:
                stateselect1 = st.selectbox("SELECT ANY STATE ", forquarter["State"].unique())
            state_result = top_insr_analysis_for_state(forquarter, stateselect1)

        elif method3 == "Top Transaction Analysis":
            col1, col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR FOR TOP TRANSACTION ANALYSIS", top_insr_df["Year"].unique())
            top_tran_analysis = transaction_amount_count_in_year(top_insr_df,years)
            col1, col2 = st.columns(2)
            with col1:
                stateselect2 = st.selectbox("SELECT ANY STATES  ", top_tran_analysis["State"].unique())
            top_tran_analysis_state = top_insr_analysis_for_state(top_tran_analysis, stateselect2)
            col1, col2 = st.columns(2)
            with col1:
                quarters = st.selectbox("SELECT ANY QUARTER ", top_tran_analysis["Quarter"].unique())
            top_insr_quarter = transaction_amount_count_in_quarter(top_tran_analysis, quarters)
        elif method3 == "Top User Analysis":
            col1, col2 = st.columns(2)
            with col1:
                years = st.selectbox("SELECT THE YEAR FOR TOP USER ANALYSIS", top_insr_df["Year"].unique())
            top_user_analysis=top_user_analysis(top_user_df,years)
            col1, col2 = st.columns(2)
            with col1:
                stateselect2 = st.selectbox("SELECT ANY STATES FOR TOP USER ANALYSIS ", top_user_analysis["State"].unique())
            top_user_analysis_pincode = top_user_analysis_pincode(top_user_analysis, stateselect2)
elif select == "Top charts":
    question = st.selectbox("Select the Question",
                            ["1. Transaction Amount & Count for Aggregated Insurance",
                             "2. Transaction Amount & Count for Map Insurance",
                             "3. Transaction Amount & Count for Top Insurance",
                             "4. Transaction Amount & Count for Aggregated Transaction",
                             "5. Transaction Amount & Count for Map Transaction",
                             "6. Transaction Amount & Count for Top Transaction",
                             "7. Transaction Count for Aggregated User",
                             "8. Registered Users of Map User",
                             "9. App Opens for Map User",
                             "10. Registered Users of Top User",
                             "11. Brands & Transaction Count of Aggregated User",
                             "12. Transaction count per Transaction type for Aggregated Transaction"])

    if (question=="1. Transaction Amount & Count for Aggregated Insurance"):
        st.subheader("TRANSACTION AMOUNT DETAILS")
        top_chart_transaction_amount("agg_insr")
        st.subheader("TRANSACTION COUNT DETAILS")
        top_chart_transaction_count("agg_insr")
    elif (question=="2. Transaction Amount & Count for Map Insurance"):
        st.subheader("TRANSACTION AMOUNT DETAILS")
        top_chart_transaction_amount("map_insr")
        st.subheader("TRANSACTION COUNT DETAILS")
        top_chart_transaction_count("map_insr")
    elif (question=="3. Transaction Amount & Count for Top Insurance"):
        st.subheader("TRANSACTION AMOUNT DETAILS")
        top_chart_transaction_amount("top_insr")
        st.subheader("TRANSACTION COUNT DETAILS")
        top_chart_transaction_count("top_insr")
    elif (question=="4. Transaction Amount & Count for Aggregated Transaction"):
        st.subheader("TRANSACTION AMOUNT DETAILS")
        top_chart_transaction_amount("agg_tran")
        st.subheader("TRANSACTION COUNT DETAILS")
        top_chart_transaction_count("agg_tran")
    elif (question=="5. Transaction Amount & Count for Map Transaction"):
        st.subheader("TRANSACTION AMOUNT DETAILS")
        top_chart_transaction_amount("map_tran")
        st.subheader("TRANSACTION COUNT DETAILS")
        top_chart_transaction_count("map_tran")
    elif (question=="6. Transaction Amount & Count for Top Transaction"):
        st.subheader("TRANSACTION AMOUNT DETAILS")
        top_chart_transaction_amount("top_tran")
        st.subheader("TRANSACTION COUNT DETAILS")
        top_chart_transaction_count("top_tran")
    elif (question=="7. Transaction Count for Aggregated User"):
        st.subheader("TRANSACTION COUNT DETAILS")
        top_chart_transaction_count("agg_user")
    elif (question == "8. Registered Users of Map User"):
        st.subheader("REGISTERED USER DETAILS")
        state = st.selectbox("Select the State", map_user_df["State"].unique())
        top_chart_registered_user("map_user",state)
    elif (question == "9. App Opens for Map User"):
        st.subheader("APPOPENS DETAILS")
        state = st.selectbox("Select the State", map_user_df["State"].unique())
        top_chart_appopens("map_user",state)
    elif (question == "10. Registered Users of Top User"):
        st.subheader("REGISTERED USERS DETAILS")
        top_chart_topuser("top_user")
    elif (question == "10. Registered Users of Top User"):
        st.subheader("REGISTERED USERS DETAILS")
        top_chart_topuser("top_user")
    elif (question == "11. Brands & Transaction Count of Aggregated User"):
        st.subheader("BRANDS & TRANSACTION COUNT DETAILS")
        agg_user_brand("agg_user")
    elif (question == "12. Transaction count per Transaction type for Aggregated Transaction"):
        st.subheader("TRANSACTION COUNT DETAILS")
        state = st.selectbox("Select the State", map_user_df["State"].unique())
        agg_tran_type("agg_tran",state)

