import sqlite3
import csv
import pandas as pd

class my_converter:
    def __init__(self):
        #creating an instance of a method and printing instructions
        print("TO CONVERT FROM CSV TO DB USE csv_to_db('name_of_csv_file.csv','name_of_database.db','table_name')")
        print("TO CONVERT FROM DB TO CSV USE db_to_csv('name_of_database.db','name_of_csv_file.csv')")

    def csv_to_db(self,name_1_csv,name_1_db,table_name):
        df = pd.read_csv(name_1_csv)
        df = df.fillna("No_Data") #fill empty spaces with No_Data string
        df.to_csv("fill_nan_with_nodata.csv",index=False) #convert to new csv file with no index

        with open("fill_nan_with_nodata.csv",'r') as csv_file_2: #open new csv file
            reader = csv.reader(csv_file_2)
            column_name_list = next(reader) #get the column name
            for i in range(len(column_name_list)): #replace blanks,(,) in column names
                column_name_list[i] = column_name_list[i].replace(" ","")
                column_name_list[i] = column_name_list[i].replace("(","_")
                column_name_list[i] = column_name_list[i].replace(")","")

            conn2 = sqlite3.connect(name_1_db) #connection to database
            cur2 = conn2.cursor() #cursor in database
            query_1 = "CREATE TABLE " + table_name + "({0});" #creating table
            query_1 = query_1.format(','.join(column_name_list)) #formatting query in the style like (col1,col2,col3)
            cur2.execute(query_1) #executing query
            query_2 = "INSERT INTO "+table_name+"({0}) VALUES ({1})" #importing data in the table
            query_2 = query_2.format(','.join(column_name_list),','.join('?'*len(column_name_list))) #formatting query in the style like VALUES (?,?,?,?,?,?)
            for row in reader:
                cur2.execute(query_2,row) #execure every lines of data
            conn2.commit()
            conn2.close()

    def db_to_csv(self,db_file_1,csv_file_1):
        conn = sqlite3.connect(db_file_1)
        cur = conn.cursor()
        res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';") #getting the name of the table
        for name in res:
            table_name = name[0]
        cur.execute('SELECT * FROM '+ table_name)
        header = [i[0] for i in cur.description] #getting the column names

        with open(csv_file_1,'w') as csv_file: #writing into csv file
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(header)
            csv_writer.writerows(cur)

        return csv_file_1

trial = my_converter()
trial.db_to_csv('all_fault_line.db','all_fault_lines.csv')
trial.csv_to_db('list_volcano.csv','list_volcanos.db','volcanos')
