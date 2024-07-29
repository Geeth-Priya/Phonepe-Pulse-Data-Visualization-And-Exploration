import streamlit as st
from streamlit_option_menu import option_menu

import os #used to access file from local system
import json
import pandas as pd
import mysql.connector
import requests

import plotly.express as px
from PIL import Image
from streamlit_lottie import st_lottie
import base64



def connect_to_mysql():
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Vigi@123",
        auth_plugin="mysql_native_password",
        database="Phonepe"
        )
        mycursor=mydb.cursor()
        return mydb, mycursor
        


#aggregated_insurance
def Agg_Insurance():
    mydb, mycursor=connect_to_mysql()
    path1="C:/My Setups/Phonepe Project/pulse/data/aggregated/insurance/country/india/state/" 
    aggregate_insurance_list= os.listdir(path1) 

    #creating a dictionary with empty list
    c1={"States":[],"Years":[],"Quarter":[],"Transaction_Name":[],"Transaction_Count":[],"Transaction_Amount":[]}

    for state in aggregate_insurance_list:
        current_states=path1+state+"/" 
        insurance_year_list=os.listdir(current_states)

        for year in insurance_year_list:
            current_years=current_states+year+"/" 
            insurance_file_list=os.listdir(current_years) 
            
            for file in insurance_file_list:
                current_files=current_years+file
                data=open(current_files,"r") 

                A1=json.load(data)
                
                for x in A1["data"]["transactionData"]:
                    name=x["name"],
                    count=x["paymentInstruments"][0]["count"]
                    amount=x["paymentInstruments"][0]["amount"]
                    c1["Transaction_Name"].append(name)
                    c1["Transaction_Count"].append(count)
                    c1["Transaction_Amount"].append(amount)
                    c1["States"].append(state)
                    c1["Years"].append(year)
                    c1["Quarter"].append(int(file.strip(".json")))

    aggregated_insurance=pd.DataFrame(c1)

    aggregated_insurance["States"].unique()
    aggregated_insurance["States"]=aggregated_insurance["States"].str.replace("-"," ")
    aggregated_insurance["States"]=aggregated_insurance["States"].str.title()
    aggregated_insurance["States"]=aggregated_insurance["States"].str.replace("-&-","&")
    aggregated_insurance["States"]=aggregated_insurance["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

    mycursor.execute('''CREATE TABLE IF NOT EXISTS Aggregate_Insurance(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Transaction_Name VARCHAR(255),
                    Transaction_Count INT,
                    Transaction_Amount INT
                    )''')
    mydb.commit()
    print("Aggregate_Insurance Tables Created Succesfully")

    for index,row in aggregated_insurance.iterrows():
            transaction_name = row["Transaction_Name"]
            if isinstance(transaction_name, tuple):
                transaction_name = transaction_name[0]
            sql='''INSERT INTO Aggregate_Insurance(States,Years,Quarter,Transaction_Name,Transaction_Count,Transaction_Amount)
            VALUES (%s,%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    transaction_name,
                    row["Transaction_Count"],
                    row["Transaction_Amount"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Aggregate_Insurance values inserted successfully")
    return aggregated_insurance



#aggregated Transaction
def Agg_Transaction():
    mydb, mycursor=connect_to_mysql()
    path2="C:/My Setups/Phonepe Project/pulse/data/aggregated/transaction/country/india/state/"
    aggregate_transaction_list=os.listdir(path2)

    c2={"States":[],"Years":[],"Quarter":[],"Transaction_Name":[],"Transaction_Count":[],"Transaction_Amount":[]}

    for state in aggregate_transaction_list:
        current_states=path2+state+"/"
        transaction_year_list=os.listdir(current_states)
        
        for year in transaction_year_list:
            current_years=current_states+year+"/"
            transaction_file_list=os.listdir(current_years)

            for file in transaction_file_list:
                current_files=current_years+file
                data=open(current_files,"r")

                A2=json.load(data)
            
                for x in A2["data"]["transactionData"]:
                    name=x["name"]
                    count=x["paymentInstruments"][0]["count"]
                    amount=x["paymentInstruments"][0]["amount"]
                    c2["Transaction_Name"].append(name)
                    c2["Transaction_Count"].append(count)
                    c2["Transaction_Amount"].append(amount)
                    c2["States"].append(state)
                    c2["Years"].append(year)
                    c2["Quarter"].append(int(file.strip(".json")))

    aggregated_transaction=pd.DataFrame(c2)

    aggregated_transaction["States"].unique()
    aggregated_transaction["States"]=aggregated_transaction["States"].str.replace("-"," ")
    aggregated_transaction["States"]=aggregated_transaction["States"].str.title()
    aggregated_transaction["States"]=aggregated_transaction["States"].str.replace("-&-","&")
    aggregated_transaction["States"]=aggregated_transaction["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

    mycursor.execute('''CREATE TABLE IF NOT EXISTS Aggregate_Transaction(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Transaction_Name VARCHAR(255),
                    Transaction_Count INT,
                    Transaction_Amount BIGINT
                    )''')
    mydb.commit()
    print("Aggregate_Transaction Tables Created Succesfully")

    for index,row in aggregated_transaction.iterrows():
            transaction_name = row["Transaction_Name"]
            if isinstance(transaction_name, tuple):
                transaction_name = transaction_name[0]
            sql='''INSERT INTO Aggregate_Transaction(States,Years,Quarter,Transaction_Name,Transaction_Count,Transaction_Amount)
            VALUES (%s,%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    transaction_name,
                    row["Transaction_Count"],
                    row["Transaction_Amount"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Aggregate_Transaction values inserted successfully")
    return aggregated_transaction



#aggregate user
def Agg_User():
    mydb, mycursor=connect_to_mysql()
    path3="C:/My Setups/Phonepe Project/pulse/data/aggregated/user/country/india/state/"
    aggregated_user_list=os.listdir(path3)

    c3={"States":[],"Years":[],"Quarter":[],"Brands":[],"Transaction_Count":[],"Percentage":[]}

    for state in aggregated_user_list:
        current_states=path3+state+"/"
        user_year_list=os.listdir(current_states)

        for year in user_year_list:
            current_years=current_states+year+"/"
            user_file_list=os.listdir(current_years)

            for file in user_file_list:
                current_files=current_years+file
                data=open(current_files,"r")

                A3=json.load(data)
                try:
                    for x in A3["data"]["usersByDevice"]: 
                        brand=x["brand"]
                        count=x["count"]
                        percentage=x["percentage"]
                        c3["Brands"].append(brand)
                        c3["Transaction_Count"].append(count)
                        c3["Percentage"].append(percentage)
                        c3["States"].append(state)
                        c3["Years"].append(year)
                        c3["Quarter"].append(int(file.strip(".json")))
                except:
                    pass

    aggregated_user=pd.DataFrame(c3)

    aggregated_user["States"].unique()
    aggregated_user["States"]=aggregated_user["States"].str.replace("-"," ")
    aggregated_user["States"]=aggregated_user["States"].str.title()
    aggregated_user["States"]=aggregated_user["States"].str.replace("-&-","&")
    aggregated_user["States"]=aggregated_user["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

    mycursor.execute('''CREATE TABLE IF NOT EXISTS Aggregate_User(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Brands VARCHAR(255),
                    Transaction_Count INT,
                    Percentage FLOAT
                    )''')
    mydb.commit()
    print("Aggregate_User Tables Created Succesfully")

    for index,row in aggregated_user.iterrows():
            sql='''INSERT INTO Aggregate_User(States,Years,Quarter,Brands,Transaction_Count,Percentage)
            VALUES (%s,%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    row["Brands"],
                    row["Transaction_Count"],
                    row["Percentage"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Aggregate_User values inserted successfully")
    return aggregated_user



#map insurance
def map_insuracnce():
    mydb, mycursor=connect_to_mysql()
    path4="C:/My Setups/Phonepe Project/pulse/data/map/insurance/hover/country/india/state/"
    map_insurance_list=os.listdir(path4)

    c4={"States":[],"Years":[],"Quarter":[],"Districts":[],"Transaction_Count":[],"Transaction_Amount":[]}

    for state in map_insurance_list:
        current_states=path4+state+"/"
        insurance_year_list=os.listdir(current_states)

        for year in insurance_year_list:
            current_years=current_states+year+"/"
            insurance_file_list=os.listdir(current_years)

            for file in insurance_file_list:
                current_files=current_years+file
                data=open(current_files,"r")

                A4=json.load(data)

                for x in A4["data"]["hoverDataList"]:
                    name=x["name"]
                    count=x["metric"][0]["count"]
                    amount=x["metric"][0]["amount"]
                    c4["Districts"].append(name)
                    c4["Transaction_Count"].append(count)
                    c4["Transaction_Amount"].append(amount)
                    c4["States"].append(state)
                    c4["Years"].append(year)
                    c4["Quarter"].append(int(file.strip(".json")))

    map_insurance=pd.DataFrame(c4)

    map_insurance["States"].unique()
    map_insurance["States"]=map_insurance["States"].str.replace("-"," ")
    map_insurance["States"]=map_insurance["States"].str.title()
    map_insurance["States"]=map_insurance["States"].str.replace("-&-","&")
    map_insurance["States"]=map_insurance["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")
                
    mycursor.execute('''CREATE TABLE IF NOT EXISTS Map_Insurance_Details(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Districts VARCHAR(255),
                    Transaction_Count INT,
                    Transaction_Amount BIGINT
                    )''')
    mydb.commit()
    print("Map_Insurance_Details Tables Created Succesfully")

    for index,row in map_insurance.iterrows():
            sql='''INSERT INTO Map_Insurance_Details(States,Years,Quarter,Districts,Transaction_Count,Transaction_Amount)
            VALUES (%s,%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    row["Districts"],
                    row["Transaction_Count"],
                    row["Transaction_Amount"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Map_Insurance_Details values inserted successfully")
    return map_insurance



#map transcation
def map_Transaction():
    mydb, mycursor=connect_to_mysql()
    path5="C:/My Setups/Phonepe Project/pulse/data/map/transaction/hover/country/india/state/"
    map_transaction_list=os.listdir(path5)

    c5={"States":[],"Years":[],"Quarter":[],"Districts":[],"Transaction_Count":[],"Transaction_Amount":[]}

    for state in map_transaction_list:
        current_states=path5+state+"/"
        transaction_year_list=os.listdir(current_states)

        for year in transaction_year_list:
            current_years=current_states+year+"/"
            transaction_file_list=os.listdir(current_years)

            for file in transaction_file_list:
                current_files=current_years+file
                data=open(current_files,"r")

                A5=json.load(data)

                for x in A5["data"]["hoverDataList"]:
                    name=x["name"]
                    count=x["metric"][0]["count"]
                    amount=x["metric"][0]["amount"]
                    c5["Districts"].append(name)
                    c5["Transaction_Count"].append(count)
                    c5["Transaction_Amount"].append(amount)
                    c5["States"].append(state)
                    c5["Years"].append(year)
                    c5["Quarter"].append(int(file.strip(".json")))

    map_transaction=pd.DataFrame(c5)

    map_transaction["States"].unique()
    map_transaction["States"]=map_transaction["States"].str.replace("-"," ")
    map_transaction["States"]=map_transaction["States"].str.title()
    map_transaction["States"]=map_transaction["States"].str.replace("-&-","&")
    map_transaction["States"]=map_transaction["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")
                
    mycursor.execute('''CREATE TABLE IF NOT EXISTS Map_Transaction_Details(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Districts VARCHAR(255),
                    Transaction_Count INT,
                    Transaction_Amount BIGINT
                    )''')
    mydb.commit()
    print("Map_Transaction_Details Tables Created Succesfully")

    for index,row in map_transaction.iterrows():
            sql='''INSERT INTO Map_Transaction_Details(States,Years,Quarter,Districts,Transaction_Count,Transaction_Amount)
            VALUES (%s,%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    row["Districts"],
                    row["Transaction_Count"],
                    row["Transaction_Amount"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Map_Transaction_Details values inserted successfully")
    return map_transaction



#map user
def map_user():
    mydb, mycursor=connect_to_mysql()
    path6="C:/My Setups/Phonepe Project/pulse/data/map/user/hover/country/india/state/"
    map_user_list=os.listdir(path6)

    c6={"States":[],"Years":[],"Quarter":[],"Districts":[],"Registered_Users":[],"App_Opens":[]}

    for state in map_user_list:
        current_states=path6+state+"/"
        user_year_list=os.listdir(current_states)

        for year in user_year_list:
            current_years=current_states+year+"/"
            user_file_list=os.listdir(current_years)

            for file in user_file_list:
                current_files=current_years+file
                data=open(current_files,"r")

                A6=json.load(data)
                
                for x in A6["data"]["hoverData"].items():
                    district=x[0]
                    registeredUsers=x[1]["registeredUsers"]
                    appOpens=x[1]["appOpens"]
                    c6["Districts"].append(district)
                    c6["Registered_Users"].append(registeredUsers)
                    c6["App_Opens"].append(appOpens)
                    c6["States"].append(state)
                    c6["Years"].append(year)
                    c6["Quarter"].append(int(file.strip(".json")))
                
    map_user=pd.DataFrame(c6)

    map_user["States"].unique()
    map_user["States"]=map_user["States"].str.replace("-"," ")
    map_user["States"]=map_user["States"].str.title()
    map_user["States"]=map_user["States"].str.replace("-&-","&")
    map_user["States"]=map_user["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

    mycursor.execute('''CREATE TABLE IF NOT EXISTS Map_User_Details(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Districts VARCHAR(255),
                    Registered_Users INT,
                    App_Opens BIGINT
                    )''')
    mydb.commit()
    print("Map_User_Details Tables Created Succesfully")

    for index,row in map_user.iterrows():
            sql='''INSERT INTO Map_User_Details(States,Years,Quarter,Districts,Registered_Users,App_Opens)
            VALUES (%s,%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    row["Districts"],
                    row["Registered_Users"],
                    row["App_Opens"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Map_User_Details values inserted successfully")
    return map_user



#top insurance
def top_insurance():
    mydb, mycursor=connect_to_mysql()
    path7="C:/My Setups/Phonepe Project/pulse/data/top/insurance/country/india/state/"
    top_insurance_list= os.listdir(path7) 

    c7={"States":[],"Years":[],"Quarter":[],"Pincodes":[],"Transaction_Count":[],"Transaction_Amount":[]}

    for state in top_insurance_list:
        current_states=path7+state+"/" 
        insurance_year_list=os.listdir(current_states)

        for year in insurance_year_list:
            current_years=current_states+year+"/" 
            insurance_file_list=os.listdir(current_years) 
            
            for file in insurance_file_list:
                current_files=current_years+file
                data=open(current_files,"r") 

                A7=json.load(data)

                for x in A7["data"]["pincodes"]:
                    entityname=x["entityName"]
                    count=x["metric"]["count"]
                    amount=x["metric"]["amount"]
                    c7["Pincodes"].append(entityname)
                    c7["Transaction_Count"].append(count)
                    c7["Transaction_Amount"].append(amount)
                    c7["States"].append(state)
                    c7["Years"].append(year)
                    c7["Quarter"].append(int(file.strip(".json")))

    top_insurance=pd.DataFrame(c7)

    top_insurance["States"].unique()
    top_insurance["States"]=top_insurance["States"].str.replace("-"," ")
    top_insurance["States"]=top_insurance["States"].str.title()
    top_insurance["States"]=top_insurance["States"].str.replace("-&-","&")
    top_insurance["States"]=top_insurance["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")
   
    mycursor.execute('''CREATE TABLE IF NOT EXISTS Top_Insurance_Details(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Pincodes BIGINT,
                    Transaction_Count INT,
                    Transaction_Amount BIGINT
                    )''')
    mydb.commit()
    print("Top_Insurance_Details Tables Created Succesfully")

    for index,row in top_insurance.iterrows():
            sql='''INSERT INTO Top_Insurance_Details(States,Years,Quarter,Pincodes,Transaction_Count,Transaction_Amount)
            VALUES (%s,%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    row["Pincodes"],
                    row["Transaction_Count"],
                    row["Transaction_Amount"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Top_Insurance_Details values inserted successfully")
    return top_insurance



#top transcation
def top_transaction():
    mydb, mycursor=connect_to_mysql()
    path8="C:/My Setups/Phonepe Project/pulse/data/top/transaction/country/india/state/"
    top_transaction_list=os.listdir(path8)

    c8={"States":[],"Years":[],"Quarter":[],"Pincodes":[],"Transaction_Count":[],"Transaction_Amount":[]}

    for state in top_transaction_list:
        current_states=path8+state+"/"
        transaction_year_list=os.listdir(current_states)

        for year in transaction_year_list:
            current_years=current_states+year+"/"
            transaction_file_list=os.listdir(current_years)

            for file in transaction_file_list:
                current_files=current_years+file
                data=open(current_files,"r")

                A8=json.load(data)

                for x in A8["data"]["pincodes"]:
                    entityname=x["entityName"]
                    count=x["metric"]["count"]
                    amount=x["metric"]["amount"]
                    c8["Pincodes"].append(entityname)
                    c8["Transaction_Count"].append(count)
                    c8["Transaction_Amount"].append(amount)
                    c8["States"].append(state)
                    c8["Years"].append(year)
                    c8["Quarter"].append(int(file.strip(".json")))

    top_transaction=pd.DataFrame(c8)

    top_transaction["States"].unique()
    top_transaction["States"]=top_transaction["States"].str.replace("-"," ")
    top_transaction["States"]=top_transaction["States"].str.title()
    top_transaction["States"]=top_transaction["States"].str.replace("-&-","&")
    top_transaction["States"]=top_transaction["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

    mycursor.execute('''CREATE TABLE IF NOT EXISTS Top_Transaction_Details(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Pincodes BIGINT,
                    Transaction_Count INT,
                    Transaction_Amount BIGINT
                    )''')
    mydb.commit()
    print("Top_Transaction_Details Tables Created Succesfully")

    for index,row in top_transaction.iterrows():
            sql='''INSERT INTO Top_Transaction_Details(States,Years,Quarter,Pincodes,Transaction_Count,Transaction_Amount)
            VALUES (%s,%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    row["Pincodes"],
                    row["Transaction_Count"],
                    row["Transaction_Amount"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Top_Transaction_Details values inserted successfully")
    return top_transaction



# top user
def top_user():
    mydb, mycursor=connect_to_mysql()
    path9="C:/My Setups/Phonepe Project/pulse/data/top/user/country/india/state/"
    top_user_list=os.listdir(path9)

    c9={"States":[],"Years":[],"Quarter":[],"Pincodes":[],"Registered_Users":[]}

    for state in top_user_list:
        current_states=path9+state+"/"
        user_year_list=os.listdir(current_states)

        for year in user_year_list:
            current_years=current_states+year+"/"
            user_file_list=os.listdir(current_years)

            for file in user_file_list:
                current_files=current_years+file
                data=open(current_files,"r")

                A9=json.load(data)
                
                for x in A9["data"]["pincodes"]:
                    name=x["name"]
                    registeredusers=x["registeredUsers"]
                    c9["Pincodes"].append(name)
                    c9["Registered_Users"].append(registeredusers)
                    c9["States"].append(state)
                    c9["Years"].append(year)
                    c9["Quarter"].append(int(file.strip(".json")))

    top_user=pd.DataFrame(c9)

    top_user["States"].unique()
    top_user["States"]=top_user["States"].str.replace("-"," ")
    top_user["States"]=top_user["States"].str.title()
    top_user["States"]=top_user["States"].str.replace("-&-","&")
    top_user["States"]=top_user["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

    mycursor.execute('''CREATE TABLE IF NOT EXISTS Top_User_Details(
                    States VARCHAR(255),
                    Years INT,
                    Quarter INT,
                    Pincodes BIGINT,
                    Registered_Users INT
                    )''')
    mydb.commit()
    print("Top_User_Details Tables Created Succesfully")

    for index,row in top_user.iterrows():
            sql='''INSERT INTO Top_User_Details(States,Years,Quarter,Pincodes,Registered_Users)
            VALUES (%s,%s,%s,%s,%s)'''
            Values=(
                    row["States"],
                    row["Years"],
                    row["Quarter"],
                    row["Pincodes"],
                    row["Registered_Users"])
            mycursor.execute(sql,Values)
            mydb.commit()
    print("Top_User_Details values inserted successfully")
    return top_user


#mysql connection
mydb, mycursor=connect_to_mysql()

#aggregated insurance dataframe
mycursor.execute("SELECT * FROM Aggregate_Insurance")
t1=mycursor.fetchall()
Agg_Insurance_Df=pd.DataFrame(t1,columns= [i[0] for i in mycursor.description])
mydb.commit()


##aggregated transaction dataframe
mycursor.execute("SELECT * FROM Aggregate_Transaction")
t2=mycursor.fetchall()
Agg_Transaction_Df=pd.DataFrame(t2,columns=[i[0] for i in mycursor.description])
mydb.commit()


#aggregate user dataframe
mycursor.execute("SELECT * FROM Aggregate_User")
t3=mycursor.fetchall()
Agg_User_Df=pd.DataFrame(t3,columns=[i[0] for i in mycursor.description])
mydb.commit()


#map insurance dataframe
mycursor.execute("SELECT * FROM Map_Insurance_Details")
t4=mycursor.fetchall()
Map_Insuracnce_Df=pd.DataFrame(t4,columns=[i[0] for i in mycursor.description])
mydb.commit()


#map transaction dataframe
mydb.mycursor=connect_to_mysql()
mycursor.execute("SELECT * FROM Map_Transaction_Details")
t5=mycursor.fetchall()
Map_Transaction_Df=pd.DataFrame(t5,columns=[i[0] for i in mycursor.description])
mydb.commit()


#map user dataframe
mycursor.execute("SELECT * FROM Map_User_Details")
t6=mycursor.fetchall()
Map_User_Df=pd.DataFrame(t6,columns=[i[0] for i in mycursor.description])
mydb.commit()


#top insurance dataframe
mycursor.execute("SELECT * FROM Top_Insurance_Details")
t7=mycursor.fetchall()
Top_Insuracnce_Df=pd.DataFrame(t7,columns=[i[0] for i in mycursor.description])
mydb.commit()


#top transaction dataframe
mycursor.execute("SELECT * FROM Top_Transaction_Details")
t8=mycursor.fetchall()
Top_Transaction_Df=pd.DataFrame(t8,columns=[i[0] for i in mycursor.description])
mydb.commit()


#top user dataframe
mycursor.execute("SELECT * FROM Top_User_Details")
t9=mycursor.fetchall()
Top_User_Df=pd.DataFrame(t9,columns=[i[0] for i in mycursor.description])
mydb.commit()



def yearwise_insurance_amount_count(df,year):

    tacy=df[df["Years"]==year].copy()
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("States")[["Transaction_Count","Transaction_Amount"]].sum().reset_index()
    tacyg.reset_index(drop=True, inplace=True)

    tacyg["Transaction_Amount(Million)"]=tacyg["Transaction_Amount"]/ 1_000_000

    col1, col2 = st.columns(2)

    with col1:
        fig_amount = px.bar(tacyg, x="States", y="Transaction_Amount(Million)", title=f"Transaction Amount {year}", color_discrete_sequence=px.colors.sequential.Cividis,width=600,height=600)
        st.plotly_chart(fig_amount)

    with col2:
        fig_count = px.bar(tacyg, x="States", y="Transaction_Count", title=f"Transaction Count {year}",color_discrete_sequence=px.colors.sequential.Cividis,width=600,height=600)
        st.plotly_chart(fig_count)

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    
    response=requests.get(url)
    d1=json.loads(response.content)
    s_names=[]
    for i in d1["features"]:
        s_names.append(i["properties"]["ST_NM"])
    s_names.sort()

    col3, col4=st.columns(2)

    with col3:
        f_i1=px.choropleth(tacyg, geojson=d1,locations="States",featureidkey="properties.ST_NM",color="Transaction_Amount(Million)",color_continuous_scale="Rainbow",
                        range_color=(tacyg["Transaction_Amount(Million)"].min(),tacyg["Transaction_Amount(Million)"].max()),
                        hover_name="States", title=f" Transaction Amount {year}",fitbounds="locations",height=600,width=600)
        f_i1.update_geos(visible=False)
        st.plotly_chart(f_i1)

    with col4:
        f_i2=px.choropleth(tacyg, geojson=d1,locations="States",featureidkey="properties.ST_NM",color="Transaction_Count",color_continuous_scale="Rainbow",
                        range_color=(tacyg["Transaction_Count"].min(),tacyg["Transaction_Count"].max()),
                        hover_name="States", title=f"Transaction Count {year}",fitbounds="locations",height=600,width=600)
        f_i2.update_geos(visible=False)
        st.plotly_chart(f_i2)

    return tacy



def year_quarter_wise_insurance_amount_count(df,quarter):
    tacy=df[df["Quarter"]==quarter].copy()
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("States")[["Transaction_Count","Transaction_Amount"]].sum().reset_index()
    tacyg.reset_index(drop=True, inplace=True)

    tacyg["Transaction_Amount(Million)"]=tacyg["Transaction_Amount"]/ 1_000_000

    col10, col11 = st.columns(2)

    with col10:
        fig_amount=px.bar(tacyg,x="States",y="Transaction_Amount(Million)",title=f"Transaction Amount Year:{tacy["Years"].max()} Quater:{quarter}",color_discrete_sequence=px.colors.sequential.Cividis,width=600,height=600)
        st.plotly_chart(fig_amount)

    with col11:
        fig_count=px.bar(tacyg,x="States",y="Transaction_Count",title=f"Transaction Count Year:{tacy["Years"].max()} Quater:{quarter}",color_discrete_sequence=px.colors.sequential.Cividis,width=600,height=600)
        st.plotly_chart(fig_count)

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url)
    d1=json.loads(response.content)
    s_names=[]
    for i in d1["features"]:
        s_names.append(i["properties"]["ST_NM"])
    s_names.sort()

    col12,col13=st.columns(2)

    with col12:
        f_i1=px.choropleth(tacyg, geojson=d1,locations="States",featureidkey="properties.ST_NM",color="Transaction_Amount(Million)",color_continuous_scale="Rainbow",
                        range_color=(tacyg["Transaction_Amount(Million)"].min(),tacyg["Transaction_Amount(Million)"].max()),
                        hover_name="States", title=f" Transaction Amount Year:{tacy["Years"].max()} Quater:{quarter}",fitbounds="locations",height=600,width=600)
        f_i1.update_geos(visible=False)
        st.plotly_chart(f_i1)

    with col13:
        f_i2=px.choropleth(tacyg, geojson=d1,locations="States",featureidkey="properties.ST_NM",color="Transaction_Count",color_continuous_scale="Rainbow",
                        range_color=(tacyg["Transaction_Count"].min(),tacyg["Transaction_Count"].max()),
                        hover_name="States", title=f"Transaction Count Year:{tacy["Years"].max()} Quater:{quarter}",fitbounds="locations",height=600,width=600)
        f_i2.update_geos(visible=False)
        st.plotly_chart(f_i2)

    return tacy



def a_t_n(df,state):
    tacy=df[df["States"]==state].copy()
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("Transaction_Name")[["Transaction_Count","Transaction_Amount"]].sum().reset_index()
    tacyg.reset_index(drop=True, inplace=True)

    col1,col2=st.columns(2)
    with col1:
        fig_pie=px.pie(data_frame=tacyg,names="Transaction_Name",values="Transaction_Amount",width=600,title=f"Transaction Amount {state}",hole=0.5)
        st.plotly_chart(fig_pie)

    with col2:
        fig_pie1=px.pie(data_frame=tacyg,names="Transaction_Name",values="Transaction_Count",width=600,title=f"Transaction Count {state}",hole=0.5)
        st.plotly_chart(fig_pie1)



#aggregated user analysis
def ag_u_p(df,year):
    ag_u_y=df[df["Years"]==year]
    ag_u_y.reset_index(drop=True,inplace=True)

    ag_u_y_g=pd.DataFrame(ag_u_y.groupby("Brands")["Transaction_Count"].sum())
    ag_u_y_g.reset_index(inplace=True)

    fig_bar=px.bar(ag_u_y_g,x="Brands",y="Transaction_Count",title=f"Brands and Transaction Count {year}",width=1000,color_discrete_sequence=px.colors.sequential.Cividis,hover_name="Brands")
    st.plotly_chart(fig_bar)

    return ag_u_y



#aggregated user analysis quater wise
def a_u__p_q(df, quarter):
    # Filter data for the selected quarter
    ag_u_y_q = df[df["Quarter"] == quarter].copy()
    ag_u_y_q.reset_index(drop=True, inplace=True)

    # Group by "Brands" and sum "Transaction_Count"
    ag_u_y_q_g = ag_u_y_q.groupby("Brands")["Transaction_Count"].sum().reset_index()

    # Plotting bar chart using Plotly Express
    fig_bar = px.bar(ag_u_y_q_g, x="Brands", y="Transaction_Count",
                     title=f"Brands and Transaction Count Quarter: {quarter}",
                     width=1000, color_discrete_sequence=px.colors.sequential.Cividis,
                     hover_name="Brands")
    
    st.plotly_chart(fig_bar)

    return ag_u_y_q



#aggregate user statewise analysis
def ag_u_p_s(df,state):
    ag_u_y_q_s=df[df["States"]==state]
    ag_u_y_q_s.reset_index(drop=True,inplace=True)


    fig_l=px.line(ag_u_y_q_s,x="Brands", y="Transaction_Count",hover_data="Percentage",title="Brands, Transaction Count, Percentage",
                color_discrete_sequence=px.colors.sequential.Cividis,width=1000,markers=True)

    st.plotly_chart(fig_l)



#map insurance district analysis
def m_i_t_a_d(df,state):
    tacy=df[df["States"]==state].copy()
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("Districts")[["Transaction_Count","Transaction_Amount"]].sum().reset_index()
    tacyg.reset_index(drop=True, inplace=True)

    col1,col2=st.columns(2)

    with col1:
        fig_b_1=px.bar(tacyg,x="Transaction_Amount",y="Districts",orientation="h",height=600,title=f" District And Transaction Amount for {state}",color_discrete_sequence=px.colors.sequential.Viridis)
        st.plotly_chart(fig_b_1)

    with col2:
        fig_b_2=px.bar(tacyg,x="Districts",y="Transaction_Count",height=600,width=600,title=f" District And Transaction Count for {state}",color_discrete_sequence=px.colors.sequential.Viridis)
        
        st.plotly_chart(fig_b_2)



#map user analysis
def m_u_p(df,year):
    m_u_y=df[df["Years"]==year]
    m_u_y.reset_index(drop=True,inplace=True)

    m_u_y_g=m_u_y.groupby("States")[["Registered_Users","App_Opens"]].sum()
    m_u_y_g.reset_index(inplace=True)

    fig_bar = px.bar(m_u_y_g,x='States',y=['Registered_Users', 'App_Opens'],title=f'Registered Users and App Opens by State {year}',barmode='group',
                    labels={'value': 'Count', 'variable': 'Metric', 'States': 'States'},width=900, height=800,color_discrete_sequence=px.colors.qualitative.Set2)
    st.plotly_chart(fig_bar)

    return m_u_y



#map user quarter analysis
def m_u_p_1(df,quarter):
    m_u_y_q=df[df["Quarter"]==quarter]
    m_u_y_q.reset_index(drop=True,inplace=True)

    m_u_y_q_g=m_u_y.groupby("States")[["Registered_Users","App_Opens"]].sum()
    m_u_y_q_g.reset_index(inplace=True)

    fig_bar = px.bar(m_u_y_q_g,x='States',y=['Registered_Users', 'App_Opens'],title=f'Registered Users and App Opens by State Quarter:{quarter}',barmode='group',
                    labels={'value': 'Count', 'variable': 'Metric', 'States': 'States'},width=900, height=800)
    st.plotly_chart(fig_bar)

    return m_u_y_q



def m_u_p_2(df, state):
    m_u_y_q_s=df[df["States"]==state]
    m_u_y_q_s.reset_index(drop=True, inplace=True)

    m_u_y_q_s_g = pd.DataFrame(m_u_y_q_s.groupby("Districts")[["Registered_Users", "App_Opens"]].sum())
    m_u_y_q_s_g.reset_index(inplace=True)

    col1, col2=st.columns(2)
    with col1:
        fig_m_u_b_1 = px.bar(m_u_y_q_s_g, x="Registered_Users", y="Districts", orientation="h", title=f"Registered Users {state}", color_discrete_sequence=px.colors.sequential.Viridis)
        st.plotly_chart(fig_m_u_b_1)

    with col2:
        m_u_y_q_s_g = m_u_y_q_s_g[m_u_y_q_s_g["App_Opens"] > 0]

        if not m_u_y_q_s_g.empty:

            fig_m_u_b_2 = px.bar(m_u_y_q_s_g, x="App_Opens", y="Districts", orientation="h", title=f"App Opens State: {state}", color_discrete_sequence=px.colors.sequential.Viridis)
            st.plotly_chart(fig_m_u_b_2)



#top insurance plot1 
def t_i_p_1(df,state):
    t_i_t_a_y_s=df[df["States"]==state]
    t_i_t_a_y_s.reset_index(drop=True,inplace=True)

    col1,col2=st.columns(2)
    with col1:
        fig_bar_1 = px.bar(t_i_t_a_y_s,x='Quarter',y='Transaction_Amount',title='Transaction Amount',hover_data="Pincodes",color_discrete_sequence=px.colors.sequential.Viridis,
                            width=900, height=800)
        st.plotly_chart(fig_bar_1)

    with col2:
        fig_bar_2 = px.bar(t_i_t_a_y_s,x='Quarter',y='Transaction_Count',title='Transaction Count',hover_data="Pincodes",color_discrete_sequence=px.colors.sequential.Viridis,
                            width=900, height=800)
        st.plotly_chart(fig_bar_2)



#top user analysis quater wise
def t_u_p_1(df,year):
    t_u_y_q=df[df["Years"]==year]
    t_u_y_q.reset_index(drop=True,inplace=True)

    t_u_y_q_g=pd.DataFrame(t_u_y_q.groupby(["States","Quarter"])["Registered_Users"].sum())
    t_u_y_q_g.reset_index(inplace=True)

    fig_bar_pl_1=px.bar(t_u_y_q_g,x="States",y="Registered_Users",color="Quarter",title=f"Registered Users by States and Quarter in {year}",width=1000,height=800,color_discrete_sequence=px.colors.sequential.Cividis,hover_name="States")
    st.plotly_chart(fig_bar_pl_1)

    return t_u_y_q



#top user plot pincodes
def t_u_pl_2(df,state):
    t_u_y_s1=df[df["States"]==state]
    t_u_y_s1.reset_index(drop=True,inplace=True)

    fig_t_plot=px.bar(t_u_y_s1,x="Quarter",y="Registered_Users",width=900,height=800,color="Registered_Users",hover_data="Pincodes",color_continuous_scale=px.colors.sequential.Teal,
                      title=f"Users by Pincodes in {state}")
    st.plotly_chart(fig_t_plot)



#streamlit
logo=Image.open("image 3.jpg")
st.set_page_config(page_title="Phonepe Pulse",page_icon=logo,layout="wide")



selected=option_menu(menu_title="PHONEPE PULSE DATA VISUALIZATION AND EXPLORATION",
            options=["Home","Aggregated Analysis","Map Analysis","Top Analysis","Data Analysis"],
            icons=["house","cash-coin","map","star","database"],
            orientation="horizontal")



if selected=="Home":
    custom_css = """
        <style>

        .custom-text {
            color: white;
        }
        </style>
        """



    # Apply the CSS styles
    st.markdown(custom_css, unsafe_allow_html=True)
    phonepe_url="https://www.phonepe.com/"
    col1,col2=st.columns(2)

    with col1:
            phonepe_url = "https://www.phonepe.com/"

            st.markdown(f'''
                <h2 style="color: violet;">
                    <a href="{phonepe_url}" style="text-decoration: none; color: violet;">PhonePe</a>
                </h2>
                <div class="custom-text">
                     <h5 class="custom-text">PhonePe is a popular digital payments platform in India. It enables users to perform a variety of financial transactions including:</h5>
                     <ul>
                         <li><strong>Peer-to-Peer Transfers:</strong> Sending and receiving money from friends and family.</li>
                         <li><strong>Bill Payments:</strong> Paying utility bills, such as electricity, water, and gas.</li>
                         <li><strong>Recharges:</strong> Prepaid mobile recharges and DTH (Direct-to-Home) TV recharges.</li>
                         <li><strong>Merchant Payments:</strong> Paying for goods and services at participating merchants.</li>
                         <li><strong>Investments:</strong> Investing in mutual funds and other financial products.</li>
                         <li><strong>Insurance:</strong> Buying insurance policies.</li>
                     </ul>
                     <p>PhonePe operates through a mobile app that leverages the Unified Payments Interface (UPI) system, allowing users to make instant payments using their bank accounts.</p>
                 </div>   
                 ''', unsafe_allow_html=True)

    with col2:       
        st.markdown(f'''
                <h2 style="color: violet;">
                    <a href="{phonepe_url}" style="text-decoration: none; color: violet;">Phonepe Pulse</a>
                </h2>
                <div class="custom-text">
                    <h5 class="custom-text">PhonePe Pulse provides real-time insights and trends on digital payments across India. It offers comprehensive analytics including:
                    </h5>
                    <ul>
                        <li><strong>Transaction Volumes:</strong> Detailed data on the total number of transactions processed.</li>
                        <li><strong>Consumer Demographics:</strong> Insights into the demographics of users making digital payments.</li>
                        <li><strong>Popular Merchant Categories:</strong> Analysis of which merchant categories are most frequently used.</li>
                        <li><strong>Geographic Trends:</strong> Trends and patterns in digital payments across different regions and states.</li>
                        <li><strong>Transaction Values:</strong> Information on the value of transactions, including average transaction sizes.</li>
                        <li><strong>Payment Methods:</strong> Breakdown of the different payment methods used, such as UPI, debit cards, and credit cards.</li>
                        <li><strong>Seasonal Fluctuations:</strong> Analysis of how digital payment trends vary with seasons and holidays.</li>
                    </ul>
                    <p>PhonePe Pulse leverages this data to provide valuable insights into the digital payments landscape, helping businesses, policymakers, and researchers make informed decisions.</p>
                </div>   
                ''', unsafe_allow_html=True)
        
    

    def get_base64_of_bin_file(bin_file):
        with open(bin_file, 'rb') as f:
            data = f.read()
        return base64.b64encode(data).decode()
    background_image_path = r'C:\My Setups\Phonepe Project\i12.jpg'
    base64_image = get_base64_of_bin_file(background_image_path)
    page_bg_img = f"""
    <style>
    [data-testid="stAppViewContainer"] {{
        background-image: url("data:image/jpg;base64,{base64_image}");
        background-size: cover;
        background-repeat: no-repeat;
        background-attachment: fixed;
        ;
    }}
    </style>
    """
    st.markdown(page_bg_img, unsafe_allow_html=True)



elif selected=="Aggregated Analysis":
    m1=st.selectbox("Select the Method", ("Aggregated Insurance Analysis","Aggregated Transaction Analysis","Aggregated User Analysis"))
    
    if m1=="Aggregated Insurance Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Agg_Insurance_Df["Years"].min(),Agg_Insurance_Df["Years"].max(),Agg_Insurance_Df["Years"].min())
        tacy1=yearwise_insurance_amount_count(Agg_Insurance_Df,years)

        quarters_available = sorted(tacy1["Quarter"].unique())
        if quarters_available:
            if len(quarters_available) == 1:
                quarter = quarters_available[0]
                st.write(f"Selected Quarter: {quarter}")
            else:
                quarters_min = quarters_available[0]
                quarters_max = quarters_available[-1]
                quarters_default = quarters_min
                col1,col2=st.columns(2)
                with col1:
                    quarter = st.slider("Select the Quarter", quarters_min, quarters_max, quarters_default)
            year_quarter_wise_insurance_amount_count(tacy1, quarter)



    elif m1=="Aggregated Transaction Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Agg_Transaction_Df["Years"].min(),Agg_Transaction_Df["Years"].max(),Agg_Transaction_Df["Years"].min())
        y_a_t_a_c=yearwise_insurance_amount_count(Agg_Transaction_Df,years)

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Select the State", y_a_t_a_c["States"].unique())        
        a_t_n(y_a_t_a_c,states) 

        quarters_available = sorted(y_a_t_a_c["Quarter"].unique())
        if quarters_available:
            if len(quarters_available) == 1:
                quarter = quarters_available[0]
                st.write(f"Selected Quarter: {quarter}")
            else:
                quarters_min = quarters_available[0]
                quarters_max = quarters_available[-1]
                quarters_default = quarters_min

                col1,col2=st.columns(2)
                with col1:
                    quarter = st.slider("Select the Quarter", quarters_min, quarters_max, quarters_default)
            year_q_wise_agg_transancation_amount_count=year_quarter_wise_insurance_amount_count(y_a_t_a_c,quarter)

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Choose the State", year_q_wise_agg_transancation_amount_count["States"].unique())    
        a_t_n(year_q_wise_agg_transancation_amount_count,states) 



    elif m1=="Aggregated User Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Agg_User_Df["Years"].min(),Agg_User_Df["Years"].max(),Agg_User_Df["Years"].min())
        y_a_u_a_c=ag_u_p(Agg_User_Df,years)

        quarters_available = sorted(y_a_u_a_c["Quarter"].unique())
        if quarters_available:
            if len(quarters_available) == 1:
                quarter = quarters_available[0]
                st.write(f"Selected Quarter: {quarter}")
            else:
                quarters_min = quarters_available[0]
                quarters_max = quarters_available[-1]
                quarters_default = quarters_min

                col1,col2=st.columns(2)
                with col1:
                    quarter = st.slider("Select the Quarter", quarters_min, quarters_max, quarters_default)        
        y_q_a_u_a_c=a_u__p_q(y_a_u_a_c,quarter)

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Choose the State", y_q_a_u_a_c["States"].unique())
        
        ag_u_p_s(y_q_a_u_a_c,states) 



elif selected=="Map Analysis":
    m2=st.selectbox("Select the Method", ("Map Insurance Analysis","Map Transaction Analysis","Map User Analysis"))
    
    if m2=="Map Insurance Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Map_Insuracnce_Df["Years"].min(),Map_Insuracnce_Df["Years"].max(),Map_Insuracnce_Df["Years"].min())
        m_i_t_a_y=yearwise_insurance_amount_count(Map_Insuracnce_Df,years)

        col1, col2=st.columns(2)       
        with col1:
            states=st.selectbox("Select the State", m_i_t_a_y["States"].unique())
        m_i_t_a_d(m_i_t_a_y,states) 

        quarters_available = sorted(m_i_t_a_y["Quarter"].unique())
        if quarters_available:
            if len(quarters_available) == 1:
                quarter = quarters_available[0]
                st.write(f"Selected Quarter: {quarter}")
            else:
                quarters_min = quarters_available[0]
                quarters_max = quarters_available[-1]
                quarters_default = quarters_min
                col1,col2=st.columns(2)
                with col1:
                    quarter = st.slider("Select the Quarter", quarters_min, quarters_max, quarters_default)
            m_i_t_a_y_q=year_quarter_wise_insurance_amount_count(m_i_t_a_y, quarter)        

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Choose the State", m_i_t_a_y_q["States"].unique())    
        m_i_t_a_d(m_i_t_a_y_q,states) 



    elif m2=="Map Transaction Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Map_Transaction_Df["Years"].min(),Map_Transaction_Df["Years"].max(),Map_Transaction_Df["Years"].min())
        m_t_t_a_y=yearwise_insurance_amount_count(Map_Transaction_Df,years)

        col1, col2=st.columns(2)       
        with col1:
            states=st.selectbox("Select the State", m_t_t_a_y["States"].unique())
        m_i_t_a_d(m_t_t_a_y,states) 

        quarters_available = sorted(m_t_t_a_y["Quarter"].unique())
        if quarters_available:
            if len(quarters_available) == 1:
                quarter = quarters_available[0]
                st.write(f"Selected Quarter: {quarter}")
            else:
                quarters_min = quarters_available[0]
                quarters_max = quarters_available[-1]
                quarters_default = quarters_min
                col1,col2=st.columns(2)
                with col1:
                    quarter = st.slider("Select the Quarter", quarters_min, quarters_max, quarters_default)
            m_t_t_a_y_q=year_quarter_wise_insurance_amount_count(m_t_t_a_y, quarter)        

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Choose the State", m_t_t_a_y_q["States"].unique())    
        m_i_t_a_d(m_t_t_a_y_q,states) 



    elif m2=="Map User Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Map_User_Df["Years"].min(),Map_User_Df["Years"].max(),Map_User_Df["Years"].min())
        m_u_y=m_u_p(Map_User_Df,years)

        quarters_available = sorted(m_u_y["Quarter"].unique())
        if quarters_available:
            if len(quarters_available) == 1:
                quarter = quarters_available[0]
                st.write(f"Selected Quarter: {quarter}")
            else:
                quarters_min = quarters_available[0]
                quarters_max = quarters_available[-1]
                quarters_default = quarters_min
                col1,col2=st.columns(2)
                with col1:
                    quarter = st.slider("Select the Quarter", quarters_min, quarters_max, quarters_default)
            m_u_q=m_u_p_1(m_u_y, quarter)

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Choose the State", m_u_q["States"].unique())    
        m_u_p_2(m_u_q,states)
        


elif selected=="Top Analysis":
    
    m3=st.selectbox("Select the Method", ("Top Insurance Analysis","Top Transaction Analysis","Top User Analysis"))

    if m3=="Top Insurance Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Top_Insuracnce_Df["Years"].min(),Top_Insuracnce_Df["Years"].max(),Top_Insuracnce_Df["Years"].min())
        t_i_t_a_y=yearwise_insurance_amount_count(Top_Insuracnce_Df,years)

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Choose the State", t_i_t_a_y["States"].unique())    
        t_i_p_1(t_i_t_a_y,states)

        quarters_available = sorted(t_i_t_a_y["Quarter"].unique())
        if quarters_available:
            if len(quarters_available) == 1:
                quarter = quarters_available[0]
                st.write(f"Selected Quarter: {quarter}")
            else:
                quarters_min = quarters_available[0]
                quarters_max = quarters_available[-1]
                quarters_default = quarters_min
                col1,col2=st.columns(2)
                with col1:
                    quarter = st.slider("Select the Quarter", quarters_min, quarters_max, quarters_default)
            t_i_t_a_y_q=year_quarter_wise_insurance_amount_count(t_i_t_a_y, quarter)    



    elif m3=="Top Transaction Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Top_Transaction_Df["Years"].min(),Top_Transaction_Df["Years"].max(),Top_Transaction_Df["Years"].min())
        t_t_t_a_y=yearwise_insurance_amount_count(Top_Transaction_Df,years)

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Choose the State", t_t_t_a_y["States"].unique())    
        t_i_p_1(t_t_t_a_y,states)

        quarters_available = sorted(t_t_t_a_y["Quarter"].unique())
        if quarters_available:
            if len(quarters_available) == 1:
                quarter = quarters_available[0]
                st.write(f"Selected Quarter: {quarter}")
            else:
                quarters_min = quarters_available[0]
                quarters_max = quarters_available[-1]
                quarters_default = quarters_min
                col1,col2=st.columns(2)
                with col1:
                    quarter = st.slider("Select the Quarter", quarters_min, quarters_max, quarters_default)
            t_t_t_a_y_q=year_quarter_wise_insurance_amount_count(t_t_t_a_y, quarter)    



    elif m3=="Top User Analysis":
        col1, col2=st.columns(2)
        with col1:
            years=st.slider("Select the Year", Top_User_Df["Years"].min(),Top_User_Df["Years"].max(),Top_User_Df["Years"].min())
        t_u_y=t_u_p_1(Top_User_Df,years)

        col1, col2=st.columns(2)
        with col1:
            states=st.selectbox("Choose the State", t_u_y["States"].unique())    
        t_u_pl_2(t_u_y,states)



elif selected=="Data Analysis":
    question=st.selectbox("Select the Question",["1.Total Transactions Count and Total Transaction Amount across all states for each year?",
                                                 "2.Highest and Least Transaction Type?" ,
                                                 "3.Query to find monthly Transaction Amount For a specific state for a given range of years?",
                                                 "4.Top 5 Brands by Transaction Count?",
                                                 "5.Districts with less Transaction Amount?",
                                                 "6.Top States with High Phonepe Registered Users and App Opens?",
                                                 "7.Least 50 States with Average Transaction Amount for each pincode?",
                                                 "8.Top 10 Pincodes By Total Transaction Count?",
                                                 "9.Number of Registered Users for each Quarter in a specific Pincode?",
                                                 "10.Which year has the miniumum Registered Users?"

    ])



    #question 1
    if question=="1.Total Transactions Count and Total Transaction Amount across all states for each year?":
        query1='''SELECT years,
        SUM(Transaction_Count) AS Total_Transaction_Count,
        SUM(Transaction_Amount) AS Total_Transaction_Amount
        FROM aggregate_insurance
        GROUP BY years
        ORDER BY Total_Transaction_Count DESC, Total_Transaction_Amount DESC
        LIMIT 10;'''
        mycursor.execute(query1)
        t1=mycursor.fetchall()
        mydb.commit()

        q_1=pd.DataFrame(t1, columns=("Years","Total_Transaction_Count","Total_Transaction_Amount"))
        st.dataframe(q_1)

    
    #question 2
    if question=="2.Highest and Least Transaction Type?":
        query2='''WITH ranked_transactions AS (
            SELECT Transaction_Name,
                Transaction_Amount,
                ROW_NUMBER() OVER (ORDER BY Transaction_Amount DESC) AS rank_desc,
                ROW_NUMBER() OVER (ORDER BY Transaction_Amount ASC) AS rank_asc
            FROM aggregate_transaction)
            SELECT Transaction_Name, Transaction_Amount,
                CASE 
                    WHEN rank_desc = 1 THEN 'Highest'
                    WHEN rank_asc = 1 THEN 'Least'
                    ELSE ''
                END AS Ranking
            FROM ranked_transactions
            WHERE rank_desc = 1 OR rank_asc = 1;'''
        mycursor.execute(query2)
        t2=mycursor.fetchall()
        mydb.commit()

        q_2=pd.DataFrame(t2, columns=("Transaction_Type","Total_Transaction_Amount","Ranking"))
        st.dataframe(q_2)


    #question 3
    if question=="3.Query to find monthly Transaction Amount For a specific state for a given range of years?":
        query3='''SELECT States,years,SUM(Transaction_Amount) 
                FROM aggregate_transaction 
                WHERE States="West Bengal" 
                AND Years BETWEEN 2020 AND 2022
                GROUP BY States,Years 
                ORDER BY States;'''
        mycursor.execute(query3)
        t3=mycursor.fetchall()
        mydb.commit()

        q_3=pd.DataFrame(t3, columns=("States","Years","Transaction_Amount"))
        st.dataframe(q_3)


    #question 4
    if question=="4.Top 5 Brands by Transaction Count?":
        query4='''SELECT Brands,SUM(Transaction_Count) AS Total_Transaction_Count FROM aggregate_user
        GROUP BY Brands ORDER BY Total_Transaction_Count DESC
        LIMIT 5;'''
        mycursor.execute(query4)
        t4=mycursor.fetchall()
        mydb.commit()

        q_4=pd.DataFrame(t4, columns=("Brands","Total_Transaction_Count"))
        st.dataframe(q_4)

    
    #question 5
    if question=="5.Districts with less Transaction Amount?":
        query5='''SELECT Districts, SUM(Transaction_Amount) AS Total_Transaction_Amount 
        FROM map_insurance_details 
        GROUP BY Districts 
        ORDER BY Total_Transaction_Amount ASC 
        LIMIT 10;'''
        mycursor.execute(query5)
        t5=mycursor.fetchall()
        mydb.commit()

        q_5=pd.DataFrame(t5, columns=("Districts","Total_Transaction_Amount"))
        st.dataframe(q_5)


    #question 6
    if question=="6.Top States with High Phonepe Registered Users and App Opens?":
        query6='''SELECT States,Years,SUM(Registered_Users) AS Total_Registered_Users,SUM(App_Opens) AS Total_APP_Opens 
        FROM map_user_details 
        GROUP BY States,Years 
        ORDER BY Total_Registered_Users DESC,Total_APP_Opens DESC 
        LIMIT 10;'''
        mycursor.execute(query6)
        t6=mycursor.fetchall()
        mydb.commit()

        q_6=pd.DataFrame(t6, columns=("States","Years","Total_Registered_Users","Total_APP_Opens"))
        st.dataframe(q_6)


    #question 7
    if question=="7.Least 50 States with Average Transaction Amount for each pincode?":
        query7='''SELECT States,Pincodes,AVG(Transaction_Amount) AS Average_Transaction_Amount 
          FROM top_insurance_details 
          GROUP BY States,Pincodes 
          ORDER BY Average_Transaction_Amount ASC 
          LIMIT 50;'''
        mycursor.execute(query7)
        t7=mycursor.fetchall()
        mydb.commit()

        q_7=pd.DataFrame(t7, columns=("States","Pincodes","Average_Transaction_Amount"))
        st.dataframe(q_7)


    #question 8
    if question=="8.Top 10 Pincodes By Total Transaction Count?":
        query8='''SELECT Pincodes, SUM(Transaction_Count) AS Total_Transaction_Count 
        FROM top_transaction_details 
        GROUP BY Pincodes 
        ORDER BY Total_Transaction_Count DESC 
        LIMIT 10;'''
        mycursor.execute(query8)
        t8=mycursor.fetchall()
        mydb.commit()

        q_8=pd.DataFrame(t8, columns=("Pincodes","Total_Transaction_Count"))
        st.dataframe(q_8)


    #question 9
    if question=="9.Number of Registered Users for each Quarter in a specific Pincode?":
        query9='''SELECT Quarter,SUM(Registered_Users) AS Total_Registered_Users 
        FROM top_user_details 
        WHERE Pincodes ='700015' 
        GROUP BY Quarter 
        ORDER BY Quarter ASC;'''
        mycursor.execute(query9)
        t9=mycursor.fetchall()
        mydb.commit()

        q_9=pd.DataFrame(t9, columns=("Pincodes","Total_Transaction_Count"))
        st.dataframe(q_9)


    #question 10
    if question=="10.Which year has the miniumum Registered Users?":
        query10='''SELECT Years, MIN(Registered_Users) AS Minimum_Registered_Users 
            FROM top_user_details 
            GROUP BY Years
            LIMIT 1;'''
        mycursor.execute(query10)
        t10=mycursor.fetchall()
        mydb.commit()

        q_10=pd.DataFrame(t10, columns=("Years","Minimum_Registered_Users"))
        st.dataframe(q_10)


            
