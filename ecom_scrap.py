import requests
from bs4 import BeautifulSoup
import pandas as pd

import mysql.connector
from mysql.connector import errorcode
import configparser

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}

def scrap_listing():

    baseurl = "https://webscraper.io"

    data = requests.get('https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops').text
    soup=BeautifulSoup(data,'html.parser')
    productlist = soup.find_all("div",{"class":"thumbnail"})

    db, cursor =  create_db_connection_and_tables()

    product_links = []
    product_ids = []
    product_names = []
    for product in productlist:
        link = product.find("a",{"class":"title"}).get('href')           
        names = product.find("a",{"class":"title"}).get('title')   
        url = baseurl + link     
        product_links.append(url)
        id = link.rsplit('/', 1)[-1]
        product_ids.append(id)
        product_names.append(names)
            
        if not check_record_listings(id, cursor):
            sql = "INSERT INTO scr_data.scrap_listings (product_ids, product_names,product_links) VALUES (%s, %s, %s )"
            val = (id, names,url)
            cursor.execute(sql, val)

    db.commit()

    cursor.close()
    db.close()

    return True

    # listings  = {'product_ids': product_ids,'product_names':product_names,'product_links':product_links}
    # df = pd.DataFrame(listings) 
    # df.to_csv('/home/huzefa/Desktop/listings.csv', index=False) 


def scrap_data():
    # file = pd.read_csv("/home/huzefa/Desktop/listings.csv")
    # productlinks = file['product_links'].tolist()
    #data_lst = []

    db, cursor =  create_db_connection_and_tables()

    sql_select_Query = "select product_links from scr_data.scrap_listings "
    cursor.execute(sql_select_Query)
    # get all records
    records = cursor.fetchall()
    
    for link in records:
        data = requests.get(link[0],headers=headers).text
        soup=BeautifulSoup(data,'html.parser')

        id = link[0].rsplit('/', 1)[-1]
        try:
            price=soup.find("h4",{"class":"price"}).text 
        except:
            price = None

        try:
            name=soup.find_all("h4")[1].text 
        except:
            name = None

        try:
            description=soup.find("p",{"class":"description"}).text
        except:
            description = None
        
        try:
            rating=soup.find("div",{"class":"ratings"}).text.strip()
        except:
            rating=None


        if not check_record_data(id, cursor):
            sql = "INSERT INTO scr_data.scrap_data (id,name, price,rating,description) VALUES (%s, %s, %s, %s, %s)"
            val = (id,name, price,rating,description)
            cursor.execute(sql, val)
        db.commit()
        
        # Save records in CSV files 

        # mydict = {"name":name,"price":price,"rating":rating,"description":description}
        # data_lst.append(mydict)

    # df = pd.DataFrame(data_lst)
    # df.to_csv('/home/huzefa/Desktop/data.csv', index=False) 

    cursor.close()
    db.close()
    return True

# Creates Database Connection & Tables
def create_db_connection_and_tables():

    try:
        
    # Creating Databse Connection
        db = mysql.connector.connect(host = "localhost", user = "root", password = "root", database = "scr_data")
        cursor = db.cursor(buffered=True)
        cursor.execute('CREATE DATABASE IF NOT EXISTS scr_data;')
        #  'scrap_data'  Table
        cursor.execute('create TABLE IF NOT EXISTS  scr_data.scrap_data (id int(11) NOT NULL ,  name varchar(300), price varchar(300) ,rating varchar(300) , description varchar(300)  ,PRIMARY KEY (`id`))')
        #  'scrap_listings'  Table
        cursor.execute('create TABLE  IF NOT EXISTS  scr_data.scrap_listings(product_ids  int(11) NOT NULL, product_names varchar(300), product_links varchar(300)  ,PRIMARY KEY (`product_ids`))')
        return db, cursor
    
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

def check_record_listings(product_ids, cursor):
    # Checking if the Record doesn't already exists
    cursor.execute("SELECT product_ids FROM scr_data.scrap_listings WHERE product_ids = %s", (product_ids,))
    return (cursor.rowcount > 0)

def check_record_data(id, cursor):
    # Checking if the Record doesn't already exists
    cursor.execute("SELECT id FROM scr_data.scrap_data WHERE id = %s", (id,))
    return (cursor.rowcount > 0)

def analysis():
    db, cursor =  create_db_connection_and_tables()

    sql_select_Query = """Select sl.product_ids,sd.name,sd.price,sd.rating,sd.description
                        From scr_data.scrap_listings sl
                        Inner Join scr_data.scrap_data sd
                        On sl.product_names = sd.name limit 10;"""
    cursor.execute(sql_select_Query)
    # get all records
    for product_ids, name ,price , rating , description in cursor.fetchall():
        print( product_ids,'   ', name ,'   ',price ,'   ', rating ,'   ', description )

def main():
    analysis()

if __name__ == '__main__':
    main()
