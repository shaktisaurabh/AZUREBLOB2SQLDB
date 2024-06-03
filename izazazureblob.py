from azure.storage.blob import BlobServiceClient
import pandas as pd
from flask import request,jsonify,session
from flask import Flask, session
import os.path
# import fsspec
from io import StringIO
import pyodbc
import sqlalchemy as sql
from flask_cors import CORS
from azure.storage.blob import ContainerClient

app = Flask(__name__)
CORS(app)

@app.route('/blob', methods=['POST'])
def login():
    try:
       if request.method == 'POST':
        #  STORAGEACCOUNTNAME= request.form.get('STORAGEACCOUNTNAME')
        #  STORAGEACCOUNTKEY= request.form.get('STORAGEACCOUNTKEY')       
         CONTAINERNAME= request.form.get('CONTAINERNAME')
         BLOBNAME= request.form.get('BLOBNAME')
         ACCOUNT_URL=request.form.get('ACCOUNT_URL')
         conn=request.form.get('connection_string')
        # 'abfss://files@dbdtcon.blob.dfs.core.windows.net/files/azurepoc.csv'
        # connn=sql.create_engine('mssql+pyodbc://LAPTOP-B7AD4K08\SQLEXPRESS/kerla?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server')
         blob_service = BlobServiceClient(account_url=ACCOUNT_URL)
         print('success')
         blobstring = blob_service.get_blob_client(CONTAINERNAME,BLOBNAME,snapshot=None)
         blob_data = blobstring.download_blob()
         data = blob_data.readall()
        # print(blob_data)
        # # print(data)
         str1 = data.decode('UTF-8')  
# print(str1)  
# for line in str1.splitlines():
#     print(line)
# print(type(data))
# l1=[]
# print(str1.splitlines()[0])
# l1.append(str1.splitlines()[0])
# print(l1)
# for k in l1:
#     s=k.split(',')
# print(s)#columns
# m=1
# k=0
# for line in str1.splitlines():
#     k+=1
# print(k)
# new_rn=k-1#actual number of rows
         rown=[]
         container = ContainerClient.from_connection_string(conn_str=conn, container_name="container")
         blob_list = container.list_blobs()
         file_list=[]
         for blob in blob_list:
            file_list.append(blob.name.split('/')[1])

         for line in str1.splitlines():
           rown.append(line.split(','))
# print(rown)
         h=rown.pop(0)
# print(rown)#rows
# print(h)#columns
         df = pd.DataFrame(rown,columns=h) 
    # print(df)
         dict_data = df.to_dict(orient='records')
    # final_data= {
    #     'response': 'success',
    #     'data': df
    # }
         out_data={
            "response":"sucessfull",
            "data":dict_data,
            "file_list":file_list
        }
         return jsonify(out_data)
# df.to_sql('azureblob1', con=connn, index=False)
    except Exception as e:
        return jsonify({'status':'failed','message':str(e.__cause__)})

if __name__ == '__main__':
    app.run(debug=True)